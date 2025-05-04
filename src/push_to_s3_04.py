# FAST-realtime update
# Script 04 - push_to_s3_04
#
#
# Created by: Andy Carter, PE
# Created - 2025.05.03
# ************************************************************

# ************************************************************
import geopandas as gpd
import boto3
from io import BytesIO
import psycopg2
import argparse
import configparser
from shapely.geometry import MultiLineString, Point, Polygon
import os

import argparse
import configparser
import time
import datetime
import warnings
# ************************************************************


# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
def is_valid_file(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file %s does not exist" % arg)
    else:
        # File exists so return the directory
        return arg
        return open(arg, 'r')  # return an open file handle
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


# ----------------
def fn_str_to_bool(value):
    if isinstance(value, bool):
        return value
    if value.lower() in {'true', 't', '1'}:
        return True
    elif value.lower() in {'false', 'f', '0'}:
        return False
    else:
        raise argparse.ArgumentTypeError(f"Boolean value expected. Got '{value}'.")
# ----------------


# ------------------
def fn_get_geodataframe_from_postgresql(table_name: str,
                                        db_params: dict,
                                        geom_col: str = 'geometry') -> gpd.GeoDataFrame:
    """
    Fetch a GeoDataFrame from a PostGIS table using psycopg2.

    Parameters:
        table_name (str): Name of the table in 'schema.table' or 'table' format.
        db_params (dict): Dictionary with keys: host, dbname, user, password, port.
        geom_col (str): Name of the geometry column.

    Returns:
        GeoDataFrame: The queried spatial data.
    """
    connection = psycopg2.connect(
        host=db_params.get("host"),
        dbname=db_params.get("dbname"),
        user=db_params.get("user"),
        password=db_params.get("password"),
        port=db_params.get("port", "5432")
    )

    try:
        sql = f"SELECT * FROM {table_name}"
        gdf = gpd.read_postgis(sql, con=connection, geom_col=geom_col)
    finally:
        connection.close()

    return gdf
# ------------------


# ----------------------
def fn_write_gdf_to_s3(gdf, str_bucket_name, str_s3_key):

    # Convert datetime columns to string format
    gdf = gdf.apply(lambda x: x.dt.strftime('%Y-%m-%dT%H:%M:%S') if x.dtype == 'datetime64[ns]' else x)

    # Convert to GeoJSON in memory ---
    geojson_buffer = BytesIO()
    geojson_str = gdf.to_json()
    geojson_buffer.write(geojson_str.encode('utf-8'))
    geojson_buffer.seek(0)

    # Upload to S3 ---
    s3 = boto3.client('s3')
    s3.upload_fileobj(geojson_buffer, str_bucket_name, str_s3_key)
    print(f"  -- Uploaded to s3://{str_bucket_name}/{str_s3_key}")
# ----------------------


# .........................................................
def fn_push_to_s3(str_config_file_path, b_print_output):
    # suppress all warnings
    warnings.filterwarnings("ignore", category=UserWarning)
    warnings.filterwarnings("ignore", category=RuntimeWarning)

    print(" ")
    if b_print_output:
        print("+=================================================================+")
        print("|                   PUSH FAST LAYERS TO S3                        |")
        print("|                Created by Andy Carter, PE of                    |")
        print("|             Center for Water and the Environment                |")
        print("|                 University of Texas at Austin                   |")
        print("+-----------------------------------------------------------------+")
        print("  ---(c) INPUT GLOBAL CONFIGURATION FILE: " + str_config_file_path)
        print("  ---[r] PRINT OUTPUT: " + str(b_print_output))
        print("===================================================================")
    else:
        print('Step 4: Uploading FAST Layers to S3')

    # --- Read variables from config.ini ---
    config = configparser.ConfigParser()
    config.read(str_config_file_path)
    
    if 'database' in config:
        section = config['database']
        
        # Database connection configuration
        db_params = {
            'host': section.get('host', ''),
            'dbname': section.get('dbname', ''),
            'user': section.get('username', ''),
            'password': section.get('password', ''),
            'port': section.get('port', '5432')
        }
    else:
        raise KeyError("Missing [database] section in config file")
        
    if 'write_to_s3' in config:
        section = config['write_to_s3']
        
        str_bucket_name = section.get('publish_bucket', '')
    else:
        raise KeyError("Missing [write_to_s3] section in config file")
        
    # bridge point table
    str_bridge_table_name = 's_bridge_warning_pnt'
    str_road_table_name = 's_flood_road_trim_ln'
    str_inundation_table_name = 's_flood_merge_ar'
    
    gdf_s_flood_road_trim_ln = fn_get_geodataframe_from_postgresql(str_road_table_name, db_params,'geometry')
    gdf_s_bridge_warning_pnt = fn_get_geodataframe_from_postgresql(str_bridge_table_name, db_params,'geometry')
    gdf_s_flood_merge_ar = fn_get_geodataframe_from_postgresql(str_inundation_table_name, db_params,'geometry')
    
    # Even if there is no polygons, this shoold have one row with model_run_time
    str_model_run_time = gdf_s_flood_merge_ar.iloc[0]['model_run_time']
    
    geometry_fake_area = Polygon([
            (-97.793186, 30.547194),
            (-97.7892304, 30.5487087),
            (-97.7892304, 30.5497087),
            (-97.793186, 30.547194)
        ])
    
    if gdf_s_flood_merge_ar.iloc[0]['geometry'] is None:
        gdf_s_flood_merge_ar.at[gdf_s_flood_merge_ar.index[0], 'geometry'] = geometry_fake_area
        
    # -- If empty, create a AGOL placeholder for road lines
    if gdf_s_flood_road_trim_ln.empty:
        
        geometry_fake_line = MultiLineString([[
            (-97.793186, 30.547194),
            (-97.7892304, 30.5487087)]])
    
        # Define placeholder attributes
        dict_empty_road_data = {
            'osm_id': [-1],
            'fclass': ['motorway'],
            'name': ["This placeholder when there is no flooding that allows AGOL to still load the layer"],
            'ref': [''],
            'road_id': [-1],
            'nextgen_id': [-1],
            'min_flood_flow': [-1],
            'max_flow': [-1],
            'model_run_time': [str_model_run_time],
            'geometry': [geometry_fake_line]
        }
        
        gdf_s_flood_road_trim_ln = gpd.GeoDataFrame(dict_empty_road_data, crs="EPSG:4326")
        
    # -- If empty, create a AGOL placeholder for bridge warning points
    if gdf_s_bridge_warning_pnt.empty:
        
        geometry_fake_point = Point(-97.7431, 30.2672)
    
        # Define placeholder attributes
        dict_empty_bridge_data = {
            'BRDG_ID': ['-1'],
            'uuid_bridge': ['-1'],
            'min_low_ch': [None],
            'min_ground': [None],
            'min_overtop': [None],
            'name': ["This placeholder when there is no flooding that allows AGOL to still load the layer"],
            'ref': [''],
            'nhd_name': [''],
            'model_run_time': [str_model_run_time],
            'max_wse': [None],
            'min_dist_to_low_ch': [None],
            'is_overtop': [False],
            'depth_array': [[ ]],
            'url': [''],
            'geometry': [geometry_fake_point]
        }
        
        gdf_s_bridge_warning_pnt = gpd.GeoDataFrame(dict_empty_bridge_data, crs="EPSG:4326")
        
    # --- Write the flood polygons ---
    str_s3_flood_ar_key = 'flood_ar.geojson'
    fn_write_gdf_to_s3(gdf_s_flood_merge_ar, str_bucket_name, str_s3_flood_ar_key)
    
    # --- Write the road lines ---
    str_s3_road_ln_key = 'flood_road_ln.geojson'
    fn_write_gdf_to_s3(gdf_s_flood_road_trim_ln, str_bucket_name, str_s3_road_ln_key)
    
    # --- Write the bridge points ---
    str_s3_bridge_pnt_key = 'bridge_warning_pnts.geojson'
    fn_write_gdf_to_s3(gdf_s_bridge_warning_pnt, str_bucket_name, str_s3_bridge_pnt_key)
# .........................................................


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='========= PUSH FAST LAYERS TO S3 =========')
    
    parser.add_argument('-c',
                        dest = "str_config_file_path",
                        help=r'REQUIRED: Global configuration filepath Example:C:\Users\civil\dev\fast_realtime\src\config_realtime.ini',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))
    
    parser.add_argument('-r',
                    dest = "b_print_output",
                    help=r'OPTIONAL: Print output messages Default: True',
                    required=False,
                    default=True,
                    metavar='T/F',
                    type=fn_str_to_bool)
    
    args = vars(parser.parse_args())
    
    str_config_file_path = args['str_config_file_path']
    b_print_output = args['b_print_output']

    fn_push_to_s3(str_config_file_path, b_print_output)

    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
 #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~