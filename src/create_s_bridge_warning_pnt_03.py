# FAST-realtime update
# Script 03 - create_s_bridge_warning_pnt_03
#
#
# Created by: Andy Carter, PE
# Created - 2025.05.02
# ************************************************************

# ************************************************************
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import geopandas as gpd
import numpy as np
import ast
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


# ------------
def fn_get_dataframe_from_postgresql(str_table_name, dict_db_params):
    conn = psycopg2.connect(
        host=dict_db_params.get("host"),
        user=dict_db_params.get("user"),
        password=dict_db_params.get("password"),
        dbname=dict_db_params.get("dbname")
    )

    cur = conn.cursor()
    query = f"SELECT * FROM public.{str_table_name}"
    cur.execute(query)
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=colnames)
    cur.close()
    conn.close()

    return df
# ------------


# ~~~~~~~~~~~~~~~~~~
def fn_get_geodataframe_from_postgresql(str_table_name,
                                        dict_db_params,
                                        str_geom_col='geometry'):
    # Create psycopg2 connection
    conn = psycopg2.connect(
        host=dict_db_params.get("db_host"),
        user=dict_db_params.get("db_user"),
        password=dict_db_params.get("db_password"),
        dbname=dict_db_params.get("db_name")
    )

    # Read GeoDataFrame using raw psycopg2 connection
    gdf = gpd.read_postgis(f"SELECT * FROM public.{str_table_name}", con=conn, geom_col=str_geom_col)
    
    conn.close()
    return gdf
# ~~~~~~~~~~~~~~~~~~


# -------------
def fn_interpolate_wse_from_flow(flow_array, list_rating_curve_str_or_list):
    """
    Interpolate WSE values from a list of flow values using the rating curve.

    Parameters:
        flow_array (list of float): List of flow values (e.g., from df['flow_array']).
        list_rating_curve_str_or_list (str or list): Rating curve as a string or list of (flow, wse) tuples.

    Returns:
        list of float: Interpolated WSE values (rounded to 1 decimal place) for each flow in flow_array.
    """
    # Parse string to list if necessary
    if isinstance(list_rating_curve_str_or_list, str):
        rating_curve = ast.literal_eval(list_rating_curve_str_or_list)
    else:
        rating_curve = list_rating_curve_str_or_list

    # Unzip into separate arrays
    curve_flows, curve_wses = zip(*rating_curve)

    # Interpolate
    interpolated_wses = np.interp(flow_array, curve_flows, curve_wses, left=np.nan, right=np.nan)

    # Round to one decimal place
    return [round(wse, 1) if not np.isnan(wse) else np.nan for wse in interpolated_wses]
# -------------


# ---------
def fn_max_wse_arrays(series_of_lists):
    """Element-wise max from a Series of equal-length lists"""
    return list(np.nanmax(np.array(series_of_lists.tolist()), axis=0))
# ---------


# -------------
def fn_replace_nan_with_min_ground(row):
    return [val if not np.isnan(val) else row['min_ground'] for val in row['wse_array']]
# -------------


# -------
def fn_calculate_depth_array(row):
    return [round(max(wse - row['min_ground'], 0), 1) for wse in row['wse_array']]
# -------


# .........................................................
def fn_create_s_bridge_warning_pnt(str_config_file_path, b_print_output):
    # suppress all warnings
    warnings.filterwarnings("ignore", category=UserWarning)
    warnings.filterwarnings("ignore", category=RuntimeWarning)

    print(" ")
    if b_print_output:
        print("+=================================================================+")
        print("|                CREATE BRIDGE WARNING POINTS                     |")
        print("|                Created by Andy Carter, PE of                    |")
        print("|             Center for Water and the Environment                |")
        print("|                 University of Texas at Austin                   |")
        print("+-----------------------------------------------------------------+")
        print("  ---(c) INPUT GLOBAL CONFIGURATION FILE: " + str_config_file_path)
        print("  ---[r] PRINT OUTPUT: " + str(b_print_output))
        print("===================================================================")
    else:
        print('Step 3: Creating bridge warning points')

    # --- Read variables from config.ini ---
    config = configparser.ConfigParser()
    config.read(str_config_file_path)
    
    if 'database' in config:
        section = config['database']
        
        # Database connection configuration
        dict_db_params = {
            'host': section.get('host', ''),
            'dbname': section.get('dbname', ''),
            'user': section.get('username', ''),
            'password': section.get('password', '')
        }
        
        # Overwrite with environment variable if password is 'xxx'
        if dict_db_params['password'] == 'xxx':
            dict_db_params['password'] = os.environ.get('DB_PASSWORD', '')
    else:
        raise KeyError("Missing [database] section in config file")
        
    print('  -- Computing bridge points')
    
    df_rating_curves = fn_get_dataframe_from_postgresql('t_bridge_rating_curve', dict_db_params)
    df_max_flow = fn_get_dataframe_from_postgresql('t_flow_per_nextgen', dict_db_params)
    
    # Extract unique uuid_bridge values from df_rating_curves
    uuid_list = df_rating_curves['uuid_bridge'].dropna().unique().tolist()
    uuid_tuple = tuple(uuid_list)
    
    # Ensure that the tuple is handled correctly when only one element is present
    if len(uuid_tuple) == 1:
        uuid_tuple = (uuid_tuple[0], uuid_tuple[0])
    
    # Use psycopg2 connection to execute the query
    with psycopg2.connect(**dict_db_params) as conn:
        sql_query = f"""
            SELECT * FROM public.s_bridge_pnt
            WHERE uuid_bridge IN %s
        """
        gdf_flow_points = gpd.read_postgis(sql_query, conn, params=(uuid_tuple,), geom_col='geometry')
        
    df_rating_max_flow = df_rating_curves.merge(
        df_max_flow,
        on='nextgen_id',
        how='left')

    # drop any row where max_flow < min_flow
    df_rating_max_flow = df_rating_max_flow[df_rating_max_flow['max_flow'] >= df_rating_max_flow['min_flow']]
    
    if len(df_rating_max_flow) > 0:
        df_rating_max_flow['wse_array'] = df_rating_max_flow.apply(
            lambda row: fn_interpolate_wse_from_flow(row['flow_array'], row['list_rating_curve']),
            axis=1)
        
        # Within df_rating_max_flow, there are possibly multiple rows with the same uuid_bridge.  If that is
        # true, then I will need a wse_array that represents the higest value... for example ...
        # [0,0,100,0,200] and [300,0,100,0,150] ... would be [300,0,100,0,200]
        df_max_by_uuid = df_rating_max_flow.groupby('uuid_bridge', as_index=False).agg({
            'wse_array': fn_max_wse_arrays,
            'model_run_time': 'first'
        })
        
        df_max_by_uuid['max_wse'] = df_max_by_uuid['wse_array'].apply(lambda wse_list: np.nanmax(wse_list))
        
        # Merge with gdf_flow_points on 'uuid'
        gdf_flow_points = gdf_flow_points.merge(df_max_by_uuid, on='uuid_bridge', how='left')
        
        # drop all rows where wse_array is NaN
        gdf_flow_points = gdf_flow_points[~gdf_flow_points['max_wse'].isna()]
        
        gdf_flow_points['min_dist_to_low_ch'] = (
            gdf_flow_points['min_low_ch'] - gdf_flow_points['max_wse']
        ).round(1)
        
        # create a coloumn named 'is_overtop'... from gdf_flow_points, if max_wse >= min_overtop ...True ... else False
        # for AGOL geoJSON, this has to be an integer
        gdf_flow_points['is_overtop'] = (gdf_flow_points['max_wse'] >= gdf_flow_points['min_overtop']).astype(int).astype(object)

        # For each row in gdf_flow_points, within wse_array, if value is nan, set it to 'min_ground'
        gdf_flow_points['wse_array'] = gdf_flow_points.apply(fn_replace_nan_with_min_ground, axis=1)
        
        # create a depth_array col that for each row in gdf_flow_points, subtracts 'min_ground' from every value in wse_array
        gdf_flow_points['depth_array'] = gdf_flow_points.apply(fn_calculate_depth_array, axis=1)
        
        # drop the wse_array from gdf_flow_points
        gdf_flow_points = gdf_flow_points.drop(columns=['wse_array'])
        
        # convert the time to the bridge xs format
        gdf_flow_points['model_run_time'] = pd.to_datetime(gdf_flow_points['model_run_time']).dt.strftime('%Y-%m-%dT%H:%M:%S')
        
        # convert depth_array to string for url
        gdf_flow_points['depth_array_str'] = gdf_flow_points['depth_array'].apply(
            lambda arr: ','.join(f"{x:.1f}" for x in arr))
        
        # create the url
        gdf_flow_points['url'] = (
            
            # *** HARDCODED PATH TO CROSS SECTION SERVICE ***
            "https://bridges.txdot.kisters.cloud/xs/?uuid=" +
            # *** HARDCODED PATH TO CROSS SECTION SERVICE ***
            
            gdf_flow_points['uuid_bridge'] +
            "&list_wse=" +
            gdf_flow_points['depth_array_str'] +
            "&first_utc_time=" +
            gdf_flow_points['model_run_time']
        )
        
        # drop the depth_array_str from gdf_flow_points
        gdf_flow_points = gdf_flow_points.drop(columns=['depth_array_str'])
    else:
        print('  -- No bridge warnings to report')
        # create an empty geodataframe of bridge points that matches a populated dataset of gdf_flow_points
        gdf_flow_points = gpd.GeoDataFrame(columns=[
        'geometry', 'BRDG_ID', 'uuid_bridge', 'min_low_ch',
        'min_ground', 'min_overtop', 'min_overtop', 'name',
        'ref', 'nhd_name', 'model_run_time', 'max_wse',
        'min_dist_to_low_ch', 'is_overtop', 'depth_array',
        'url'], geometry='geometry', crs='EPSG:4326')
    
    print('  -- Uploading bridge points to PostgreSQL')
    
    table_name = "s_bridge_warning_pnt"

    # Construct connection string
    connection_string = (
        f"postgresql+psycopg2://{dict_db_params['user']}:{dict_db_params['password']}@"
        f"{dict_db_params['host']}/{dict_db_params['dbname']}"
    )
    
    # Create engine
    engine = create_engine(connection_string)
    
    # Upload to PostGIS
    with engine.connect() as conn:
        gdf_flow_points.to_postgis(table_name, conn, if_exists='replace', index=False)
        
    print('  -- Bridge points successfully uploaded')

# .........................................................


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='========= CREATE BRIDGE WARNING POINTS =========')
    
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

    fn_create_s_bridge_warning_pnt(str_config_file_path, b_print_output)

    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
 #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~