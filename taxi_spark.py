
def processTrips(pid, records):
    if pid==0:
        next(records)
    counts = {}
    import rtree
    import fiona
    import fiona.crs
    import shapely
    import rtree
    import pyproj
    import shapely.geometry as geom
    import csv
    reader = csv.reader(records)
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    neighborhoods = gpd.read_file(neighbor_shape).to_crs(fiona.crs.from_epsg(2263))
    boroughs = gpd.read_file(borough_shape).to_crs(fiona.crs.from_epsg(2263))
    index_pick = rtree.Rtree()
    index_drop = rtree.Rtree()
    
    #gets the pick location neighborhood and sets up an index for it
    for idx,geometry in enumerate(neighborhoods.geometry):
        index_pick.insert(idx, geometry.bounds)
    
    #set up r-tree index for drop off location
    for idx,geometry in enumerate(boroughs.geometry):
        index_drop.insert(idx, geometry.bounds)
    
    for row in reader:
        try:
            p_pick = geom.Point(proj(float(row[3]), float(row[2]))) #pick-up location lat / lon
            p_drop = geom.Point(proj(float(row[5]), float(row[4]))) #drop off
            match_pick = None
            for idx in index_pick.intersection((p_pick.x, p_pick.y, p_pick.x, p_pick.y)):
                
                shape = neighborhoods.geometry[idx]
                
                if shape.contains(p_pick):
                    match_pick = idx
                    break

            match_drop = None
            for idx in index_drop.intersection((p_drop.x, p_drop.y, p_drop.x, p_drop.y)):
                
                shape = boroughs.geometry[idx]
                
                if shape.contains(p_drop):
                    match_drop = idx
                    break

            if match_pick is None or match_drop is None:
                pass
            else:
                match = (match_pick,match_drop)
                counts[match] = counts.get(match, 0) + 1
        
        except ValueError:
            pass 

    return counts.items()
            

if __name__=='__main__':
    import pyspark
    import geopandas as gpd
    import sys
    import pandas as pd
    import rtree
    import fiona
    import fiona.crs
    import shapely
    import rtree
    import pyproj
    import shapely.geometry as geom
    import csv
    sc = pyspark.SparkContext()
    neighbor_shape = 'hdfs:///tmp/bdm/neighborhoods.geojson'
    borough_shape = 'hdfs:///tmp/bdm/boroughs.geojson'
    yellow_data = 'hdfs:///tmp/bdm/yellow_tripdata_2011-05.csv'
    neighborhoods = gpd.read_file(neighbor_shape).to_crs(fiona.crs.from_epsg(2263))
    boroughs = gpd.read_file(borough_shape).to_crs(fiona.crs.from_epsg(2263))

    rdd = sc.textFile(yellow_data)
    counts = rdd.mapPartitionsWithIndex(processTrips).reduceByKey(lambda x,y: x+y).collect()
    
    countsPerNeighborhood = list(map(lambda x: (x[0][0], x[0][1], x[1]), counts))
    mapped_counts = list(map(lambda x: (neighborhoods['neighborhood'][x[0]], boroughs['boroname'][x[1]], x[2]), countsPerNeighborhood))
    df = pd.DataFrame(mapped_counts).sort_values(2,ascending=False)

    borough_list = ['Staten Island','Manhattan','Brooklyn','Queens','Bronx']

    for i in borough_list:
        df_filt = df[df[1]==i]
        df_filt.sort_values(2,ascending=False,inplace=True)
        top_neighbs = list(df_filt[0].head(3))
        print("Top Neighborhoods for "+i+ ": {}".format(top_neighbs))
