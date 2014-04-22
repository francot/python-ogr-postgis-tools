import sys
import os
import psycopg2
import psycopg2.extras
from osgeo import ogr
from osgeo import osr

dbname = '__'
user = '__'
password = '__'
host = '__'

connString = "PG: host=%s dbname=%s user=%s password=%s" %(host, dbname, user, password)
connString_pg = "dbname=%s user=%s password=%s  host=%s" %(dbname, user, password,  host)


###################### export from pg to shp ################################
#############################################################################

def GetPgLayerFieldTypes (table_pg, geometry_field):
# return a Dictionary with Pg table field name and Ogr feature
    conn = ogr.Open(connString)
    lyr = conn.GetLayer(table_pg)
    if lyr is None:
        print >> sys.stderr, '[ ERROR ]: layer name = "%s" could not be found in database "%s"' % ( table_pg, databaseName )
        sys.exit(1)
    lyrDefn = lyr.GetLayerDefn()
    pgtable_fields={}
    for i in range(lyrDefn.GetFieldCount()):
        fieldName =  lyrDefn.GetFieldDefn(i).GetName()
        fieldTypeCode = lyrDefn.GetFieldDefn(i).GetType()
        fieldWidth = lyrDefn.GetFieldDefn(i).GetWidth()
        GetPrecision = lyrDefn.GetFieldDefn(i).GetPrecision()    
        pgtable_fields[fieldName]= fieldTypeCode, fieldWidth, GetPrecision
    conn.Destroy()
    return pgtable_fields      


def GetShapefilesLayerFieldTypes (shapefile):
# return a Dictionary with shapefile field name and Ogr feature
    shp = osgeo.ogr.Open(shapefile)
    layer = shp.GetLayer(0)
    lyrDefn = layer.GetLayerDefn()
    shp_fields={}
    for i in range(lyrDefn.GetFieldCount()):  
        fieldName =  lyrDefn.GetFieldDefn(i).GetName() 
        fieldTypeCode = layer_defn.GetFieldDefn(i).GetType()
        fieldType = lyrDefn.GetFieldDefn(i).GetFieldTypeName(fieldTypeCode)            
        if fieldType=='String':
            fieldType='Varchar'
        fieldType
        shp_fields[field_name]=fieldType

   
def GetGeometryTypeFromPg (table_pg, geometry_field ):
# return the GeometryType from a Pg Table
    con = psycopg2.connect(connString_pg) 
    cur = con.cursor()
    query ='SELECT ST_AsEWKB(%s)  FROM %s' %(geometry_field, table_pg)
    cur.execute (query)
    res = cur.fetchone()
    geometrytype = ogr.CreateGeometryFromWkb(bytes(res[0])).GetGeometryType()
    return geometrytype
    #TODO: manage MultiGeometry Type

    
def GetPgTableAsDict (table_pg, geometry_field ):
# return a Pg table as a Dictionary and transform Geometry to WKB
    con = psycopg2.connect(connString_pg)      
    cur = con.cursor(cursor_factory = psycopg2.extras.RealDictCursor)  ### http://wiki.postgresql.org/wiki/Using_psycopg2_with_PostgreSQL  ### http://stackoverflow.com/questions/6739355/dictcursor-doesnt-seem-to-work-under-psycopg2
    query = "SELECT *, ST_AsText (%s) as geom_wkb   FROM %s " %(geometry_field, table_pg)
    cur.execute(query)
    res = cur.fetchall()
    return res
    #TODO: solve bug for decimal or data field. In this case output is not accepted from createShpFromPg function


def CreateShpFromPg (output_shp,table_pg,geometry_field,srid):
# using the above function, return a shpfile from Pg table  
    # set up the shapefile driver
    driver = ogr.GetDriverByName("ESRI Shapefile")
    # create the data source
    if os.path.exists(output_shp+'.shp'):	
        os.remove(output_shp+'.shp')
    data_source = driver.CreateDataSource (output_shp +'.shp')
    # create the spatial reference
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(srid)   
    # create the layer
    layer = data_source.CreateLayer(output_shp, srs, GetGeometryTypeFromPg(table_pg,geometry_field))
    # Add the fields taking dictionary output from GetPGLayerFieldTypes function
    for k,v in GetPgLayerFieldTypes(table_pg,geometry_field).items():
        # normalize field_name to 10 character
        k_n=k[:10]
        print 'field   '+ k + '       normalized to  ' + k_n
        field_name = ogr.FieldDefn(k_n, v[0])
        field_name.SetWidth (v[1])   
        layer.CreateField(field_name)        
    # Process the output dictionary from PgTableAsDict function and add features to the shapefile
    for pg_table_row in GetPgTableAsDict(table_pg,geometry_field):
        # create the feature
        feature = ogr.Feature(layer.GetLayerDefn())
        for f in pg_table_row.keys():
            if f == geometry_field:
                continue
            elif f == 'geom_wkb':
                geometrytype = ogr.CreateGeometryFromWkt(pg_table_row[f])
                # Set the geometry attributes
                feature.SetGeometry(geometrytype)
            else:
                f_n=f[:10]                
                # Set the attributes
                feature.SetField(f_n, pg_table_row[f])
        layer.CreateFeature(feature)
        feature.Destroy()
    # Destroy the data source to free resources
    data_source.Destroy()


###################### import from shp to pg ################################
#############################################################################

def GetFieldValueasDictFromShp(shapefile):
# return a Dictionary with shp field and values
    shp = ogr.Open(shapefile)
    if shapefile is None:
        print 'Could not open %s' % (filename)
    else:
        layer = shp.GetLayer(0)
        lyrDefn = layer.GetLayerDefn()
        #this Dictionary store field name as keys and field type
        shp_field = {}
        for i in range(lyrDefn.GetFieldCount()):
            fieldName = lyrDefn.GetFieldDefn(i).GetName()
            fieldTypeCode = lyrDefn.GetFieldDefn(i).GetType()
            fieldType = lyrDefn.GetFieldDefn(i).GetFieldTypeName(fieldTypeCode)
            if fieldType=='String':
                fieldType='Varchar'
            fieldType
            shp_field[fieldName]=fieldType
        shp_records=[]
        for i in range(layer.GetFeatureCount()):
            shp_values = {}
            for j in range(lyrDefn.GetFieldCount()):
                fieldName = lyrDefn.GetFieldDefn(j).GetName()
                feature = layer.GetFeature(i)
                value = feature.GetField(fieldName)
                wkt = feature.GetGeometryRef().ExportToWkt()
                shp_values[fieldName]=value
                shp_values['geom']=wkt
            #append in the vector all the shp records stored as Dictionary
            shp_records.append(shp_values)
        #insert in the first position of the vector the Dictionary with field type
        shp_records.insert(0,shp_field)
        return shp_records


def import_shp_to_postgres(filename,schema,table, srid):
# import shp to pg
    con = None
    try:
        con = psycopg2.connect(connString_pg)
        cur = con.cursor()
        #Create Table using field type from the Dictionary
        FieldType = GetFieldValueasDictFromShp(filename)[0]
        # TODO append or insert choice
        sql_parameter = 'CREATE TABLE IF NOT EXISTS __s__.__t__ \n(' + ',\n '.join([str(i)+' '+str(v)  for i,v in FieldType.iteritems()]) + ',\n geom Geometry' + ')'
        sql = sql_parameter.replace('__s__',schema).replace('__t__',table)
        cur.execute("DROP TABLE test")
        cur.execute (sql)
        #Insert in the Table all the record from the Dictionary
        for i in range(1,len(GetFieldValueasDictFromShp(filename))):
            d=GetFieldValueasDictFromShp(filename)[i]
            field_name = ', '.join([str(i)  for i in d.keys()])
            value= ', '.join([str(repr(i)) for i in d.values()])  #### repr solve the string cast -- http://initd.org/psycopg/docs/usage.html#adapt-string
            sql= "INSERT INTO %s.%s (%s)" %(schema,table,field_name) + " VALUES (%s)" %(value)
            cur.execute(sql)
            # TODO pass srid as function output and Srid Transform
            sql = 'UPDATE %s.%s SET geom=ST_SetSrid(geom,%s)'%(schema,table,srid)
            cur.execute(sql)
        con.commit()
        print "Import Ok\n for shapefile: %s \n in table: %s.%s" % (os.path.basename(filename),schema,table)
    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if con:
            con.close()




