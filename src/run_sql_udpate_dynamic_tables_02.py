# FAST-realtime update
# Script 02 - run_sql_udpate_dynamic_tables_02
#
#
# Created by: Andy Carter, PE
# Created - 2025.05.01
# ************************************************************


# ************************************************************
import psycopg2
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


# ---------------
def fn_run_sql_script(db_config, sql_file_path):
    # Connect to the PostgreSQL database
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password']
        )
        print("  -- Connected to the database")
        
        # Open the SQL file and read its contents
        with open(sql_file_path, 'r') as sql_file:
            sql_script = sql_file.read()

        # Create a cursor and execute the SQL script
        cursor = conn.cursor()
        cursor.execute(sql_script)
        conn.commit()  # Commit the transaction

        print(f"  -- SQL script executed successfully")

    except Exception as e:
        print(f"Error executing SQL script: {e}")
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()
# ---------------


# .........................................................
def fn_run_sql_udpate_dynamic_tables(str_config_file_path, b_print_output):
    # suppress all warnings
    warnings.filterwarnings("ignore", category=UserWarning)

    print(" ")
    if b_print_output:
        print("+=================================================================+")
        print("|              UPDATE FAST DYNAMIC POSTGRES TABLES                |")
        print("|                Created by Andy Carter, PE of                    |")
        print("|             Center for Water and the Environment                |")
        print("|                 University of Texas at Austin                   |")
        print("+-----------------------------------------------------------------+")
        print("  ---(c) INPUT GLOBAL CONFIGURATION FILE: " + str_config_file_path)
        print("  ---[r] PRINT OUTPUT: " + str(b_print_output))
        print("===================================================================")
    else:
        print('Step 2: Update realtime flood tables')

    # --- Read variables from config.ini ---
    config = configparser.ConfigParser()
    config.read(str_config_file_path)
    
    if 'database' in config:
        section = config['database']
        
        # Database connection configuration
        db_config = {
            'host': section.get('host', ''),
            'dbname': section.get('dbname', ''),
            'user': section.get('username', ''),
            'password': section.get('password', '')
        }
        
        # Overwrite with environment variable if password is 'xxx'
        if db_config['password'] == 'xxx':
            db_config['password'] = os.environ.get('DB_PASSWORD', '')
    else:
        raise KeyError("Missing [database] section in config file")
        
    if 'sql' in config:
        section = config['sql']
        
        # SQL file to run
        sql_file_path = section.get('sql_file_path', '')
        print(f"  -- SQL file: {sql_file_path}")
    else:
        raise KeyError("Missing [sql] section in config file")
        
    try:
        print("  -- Connecting to the database")
        # Run the script
        fn_run_sql_script(db_config, sql_file_path)
    except Exception as e:
        print(f"Processing failed: {e}")
        raise
# .........................................................


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='========= UPDATE FAST DYNAMIC POSTGRES TABLES =========')
    
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

    fn_run_sql_udpate_dynamic_tables(str_config_file_path, b_print_output)

    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
 #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~