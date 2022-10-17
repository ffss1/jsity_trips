import sys
import csv
import sqlite3
# how to install: pip install spatialite
# requires mod_spatialite. For Win, download binaries zip and extract at C:/Windows/system32 https://www.gaia-gis.it/gaia-sins/index.html
import spatialite

SPATIALITE_PATH = 'C:/windows/system32/mod_spatialite.dll'
POINT_SRID = 4326  # this is a datatype code used in scope of spatial databases

conn = None


def process(args):
    '''
    1. Initializes db: w/ -init
    2. Loads CSV into db: w/ -f <path>
    3. Calculates average weekly trips: w/ -aw [-r <region>|-bbox <x1,y1,x2,y2>]
    '''
    def get_argval(name):
        val = args[args.index(name) + 1]
        if name == '-bbox':
            val = tuple(float(v.strip()) for v in val.split(','))
        return val
    
    init_db(first_time='-init' in args)  # init SQLite DB

    if '-f' in args: # load CSV from path into DB
        path = get_argval('-f')
        load_rows(path)
        conn.commit()
    
    if '-aw' in args: # gets average weekly number of trips, per region name and/or bounding box
        region = get_argval('-r') if '-r' in args else None
        bbox = get_argval('-bbox') if '-bbox' in args else None
        
        # do we need to check which one? it's easy to do for both
        cols = ['point_origin', 'point_dest'] if bbox else [None]
        
        for point_col in cols:
            results = get_avg_trips_per_week(region=region, bbox=bbox, point_col=point_col)
            print(list(results))
        

def get_avg_trips_per_week(region=None, bbox=None, point_col='point_origin', ):
    '''
    Makes SQLite Spatialite query to get mean value of trips per week.
    Params:
    (optional) region (default gets all regions) - case-sensitive region name
    (optional) bbox - tuple of bounding box coords for spatial dbms BuildMBR( X1 , Y1 , X2 , Y2 ) 
    (optional) point_col (default 'point_origin') - name of column containing coord to be tested with bounding box
    '''
    cur = conn.cursor()
    where_clause = '' # get all
    conditions = [
        f'region="{region}"' if region else '',
        f'MBRContains(BuildMBR{str(tuple(bbox))}, {point_col})' if bbox else ''
    ]
    if any(c for c in conditions):
        where_clause = 'where ({})'.format(' AND '.join(c for c in conditions if c))

    '''In the event of slow queries, to take better advantage of Spatialite DBMS, it's possible to explore in more detail a solution using SpatialIndex,
    which could turn into something like below.

    Maybe it would not be needed for 100 mi records?

    SELECT region,count()/count(DISTINCT strftime("%Y-%W", datetime)) as avg_trips_per_week
    FROM trips as tr JOIN (
        SELECT ROWID FROM SpatialIndex WHERE
            (f_table_name="trips") AND 
            (f_geometry_column="{point_col}") AND
            (search_frame = BuildMBR{str(tuple(bbox))})
        ) as spi ON tr.ROWID=spi.ROWID
    GROUP BY region
    ORDER BY region;

    https://www.gaia-gis.it/gaia-sins/spatialite-cookbook-5/cookbook_topics.04.html#topic_SpatialIndex_as_BoundingBox
    '''    

    stmt = f'''SELECT region,count()/count(DISTINCT strftime("%Y-%W", datetime)) as avg_trips_per_week
    FROM trips {where_clause}
    GROUP BY region
    ORDER BY region;'''

    print(stmt)
    res = cur.execute(stmt)
    cols = [col[0] for col in res.description]
    for row in res:
        yield dict(zip(cols, row))  
        

def load_rows(path):
    '''Reads CSV from path and load into previously initialized DB'''
                        
    def get_row_count():
        # pretty hacky way to assess load progress without loading to memory https://stackoverflow.com/questions/2890549/number-of-lines-in-csv-dictreader
        # -1 is header row
        return -1 + sum(1 for _ in open(path, 'r'))
    
    def get_rows():
        with open(path, 'r') as f:
            for row in csv.DictReader(f):
                yield row

    def store_row(row):
        stmt = '''INSERT INTO trips (id,region,point_origin,point_dest,datetime,datasource) 
        values (null,"{region}",GeomFromText("{origin_coord}",4326),GeomFromText("{destination_coord}",4326),"{datetime}","{datasource}") ; '''.format(**row)
        #print(stmt)
        conn.execute(stmt)
    
    '''Develop a way to inform the user about the status of the data ingestion without using a
    polling solution.'''
    total_rows = get_row_count()
    STATUS_EVERY = total_rows/100 # 1 pct

    for i,r in enumerate(get_rows(), start=1):
        if not i % STATUS_EVERY:
            print(f'Loading {i}/{total_rows} ({i/total_rows*100:.1f}%)')
        store_row(r)
    
    
def init_db(first_time=True, fname='jobsity.db'):
    global conn
    conn = sqlite3.connect(fname)
    # load spatialite
    conn.enable_load_extension(True)
    conn.load_extension(SPATIALITE_PATH)
    
    if first_time:
        conn.execute('select InitSpatialMetadata(1)')
        
        '''this exercise does some simplification, to be better scalable perhaps it would be a good idea to add the following
        tables: regions(id,name) datasource(id,name) then change table trips cols to
        (region_id integer not null foreign key references regions(id) on delete set null, 
        datasource_id integer not null foreign key references datasources(id) on delete set null,)
        
        depending on architecture, maybe I would also write a trigger that allows simply writing region name to 'trips' for
        example, then the trigger consists/adds that to 'regions' and replaces new.region_id with the proper id in 'regions'.
        '''
        conn.executescript(f'''
            create table if not exists trips(
                id integer not null primary key,
                region varchar(20) not null,
                datetime text not null,
                datasource varchar(20)
            );
            select AddGeometryColumn('trips', 'point_origin', {POINT_SRID}, 'POINT', 2);
            select AddGeometryColumn('trips', 'point_dest', {POINT_SRID}, 'POINT', 2);
            
            select CreateSpatialIndex('trips', 'point_origin');
            select CreateSpatialIndex('trips', 'point_dest');
        ''')
        '''
        # this appears to delete some unnecessary metadata (?) and shrink db a little - not sure of impact will leave it out
        conn.execute("delete from spatial_ref_sys")
        conn.execute("delete from spatial_ref_sys_aux")
        conn.commit()
        conn.execute("vacuum")
        ''' 
        
if __name__ == "__main__":
    print(f"Arguments count: {len(sys.argv)}")
    print(f"Arguments list: {list(sys.argv)}")
    process(sys.argv)        