# import os
import snowflake.connector as sf
import logging
# import ConfigParser#replace configparsert 2024June 5
import configparser

config = configparser.ConfigParser()
config.read('E:\\WebClient\\config.ini')

DB_SERVER = config.get('DATABASE', 'DB_SERVER')
DB_USER = config.get('DATABASE', 'DB_USER')
DB_PASSWORD = config.get('DATABASE', 'DB_PASSWORD')
DB_DATABASE = config.get('DATABASE', 'DB_DATABASE')

snowflake_user = config.get('DATABASE', "snowflake_user")
snowflake_password = config.get('DATABASE', "snowflake_password")
snowflake_account = config.get('DATABASE', "snowflake_account")
snowflake_warehouse = config.get('DATABASE', "snowflake_warehouse")
snowflake_database = config.get('DATABASE', "snowflake_database")
snowflake_schema = config.get('DATABASE', "snowflake_schema")


def create_connection():
    conn = sf.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        database=snowflake_database,
        schema=snowflake_schema,
        warehouse=snowflake_warehouse
    )
    return conn


def create_polestar_position(cur, database_name="DEV_WSM_DB", schema_name="API"):
    """
    Create (if not exists) the table polestar_position
    """
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.LOAD_POLESTAR_POSITION (
        IMONUMBER VARCHAR(20),
        SPEED VARCHAR(15),
        HEADING VARCHAR(15),
        LATITUDE VARCHAR(15),
        LONGITUDE VARCHAR(15),
        ALERT_STATUS VARCHAR(20),
        CYCLONE_NAME VARCHAR(100),
        CYCLONE_TYPE VARCHAR(20),
        CYCLONE_DISTANCE VARCHAR(20),
        CYCLONE_LATITUDE VARCHAR(15),
        CYCLONE_LONGITUDE VARCHAR(15),
        CYCLONE_SPEED VARCHAR(20),
        REPORT_TIME DATETIME,
        CREATE_TIME DATETIME
    );
    """
    cur.execute(create_sql)


def create_polestar_voyage(cur, database_name="DEV_WSM_DB", schema_name="API"):
    """
    Create (if not exists) the table polestar_voyage
    """
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.LOAD_POLESTAR_VOYAGE (
        IMONUMBER VARCHAR(20),
        DRAUGHT VARCHAR(20),
        ETA VARCHAR(30),
        DESTINATION VARCHAR(50),
        SHIPTYPE VARCHAR(100),
        REPORT_TIME DATETIME,
        CREATE_TIME DATETIME
    );
    """
    cur.execute(create_sql)


def create_polestar_vesselInfo(cur, database_name="DEV_WSM_DB", schema_name="API"):
    """
    Create (if not exists) the table polestar_vesselInfo
    """
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.LOAD_POLESTAR_VESSELINFO (
        IMONUMBER VARCHAR(20),
        MMSI VARCHAR(20),
        SUBSCRIPTIONID VARCHAR(20),
        COMPANYNAME VARCHAR(150),
        SHIPNAME VARCHAR(150),
        TECHNICALMANAGER VARCHAR(150),
        SHIPTYPE VARCHAR(50),
        CALLSIGN VARCHAR(20),
        FLAGNAME VARCHAR(50),
        PORTOFREGISTRY VARCHAR(50),
        CLASSIFICATIONSOCIETY VARCHAR(150),
        REGISTEREDOWNER VARCHAR(150),
        OPERATOR VARCHAR(150),
        DEADWEIGHT VARCHAR(50),
        DISPLACEMENT VARCHAR(50),
        GROSSTONNAGE VARCHAR(50),
        LENTHOVERALL VARCHAR(50),
        BREADTH VARCHAR(50),
        DEPTH VARCHAR(50),
        DRAUGHT VARCHAR(50),
        SHIPBUILDER VARCHAR(150),
        COUNTRYOFBUILD VARCHAR(50),
        YEAROFBUILD NUMBER(38,0),
        CREATE_TIME DATETIME
        );
    """
    cur.execute(create_sql)


def insert_polestar_position(cursor, vessel):
    # print(vessel)
    tup = (vessel['imo_number'], vessel['speed'], vessel['heading'], vessel['latitude'], vessel['longitude'],
           vessel['alert_status'], vessel['cyclone_name'],
           vessel['cyclone_type'], vessel['cyclone_distance'], vessel['cyclone_latitude'], vessel['cyclone_longitude'],
           vessel['cyclone_speed'], vessel['timestamp_gnss'],
           vessel['create_time'])
    logging.info(tup)
    cursor.execute(
        "Insert INTO polestar_position (imonumber,speed,heading,latitude,longitude,alert_status,cyclone_name,cyclone_type,cyclone_distance,cyclone_latitude,cyclone_longitude,cyclone_speed,report_time,create_time) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,convert(datetimeoffset,%s,121),%s)",
        tup)
    return cursor


def insert_polestar_voyage(cursor, voyageInfo):
    tup = (voyageInfo['IMONumber'], voyageInfo['draught'], voyageInfo['ETA'], voyageInfo['destination'],
           voyageInfo['shipType'], voyageInfo['lastUpdatedTime'], voyageInfo['create_time'])
    logging.info(tup)
    cursor.execute(
        "Insert INTO polestar_voyage (imonumber,draught,eta,destination,shipType,report_time,create_time) VALUES(%s,%s,%s,%s,%s,%s,%s)",
        tup)
    return cursor


def insert_polestar_vesselInfo(cursor, vesselInfo):
    # print(vesselInfo)
    tup = (vesselInfo['IMONumber'], vesselInfo['MMSI'], vesselInfo['subscriptionId'], vesselInfo['companyName'],
           vesselInfo['shipName'],
           vesselInfo['technicalManager'], vesselInfo['shipType'], vesselInfo['callSign'], vesselInfo['flagName'],
           vesselInfo['portOfRegistry'],
           vesselInfo['classificationSociety'], vesselInfo['registeredOwner'], vesselInfo['operator'],
           vesselInfo['deadweight'], vesselInfo['displacement'], vesselInfo['grossTonnage'],
           vesselInfo['lengthOverall'], vesselInfo['breadth'], vesselInfo['depth'], vesselInfo['draught'],
           vesselInfo['shipBuilder'],
           vesselInfo['countryOfBuild'], vesselInfo['yearOfBuild'], vesselInfo['create_time'])
    logging.info(tup)

    vessel_imo_number = vesselInfo['IMONumber']
    if vessel_imo_number and vessel_imo_number != 'None':
        # Check if the vessel already exists in the table
        cursor.execute('''
                SELECT COUNT(*) 
                FROM polestar_vesselInfo
                WHERE IMONumber = %s
            ''', (vessel_imo_number,))

        row_count = cursor.fetchone()[0]
        # Only insert if the vessel is not already in the table
        if row_count == 0:
            cursor.execute(
                f'''
               Insert INTO polestar_vesselInfo (imonumber,mmsi,subscriptionID,companyname,shipname,technicalManager,shipType,callSign,flagName,portOfRegistry,classificationSociety,
               registeredOwner,operator,deadweight,displacement,grossTonnage,lenthOverall,breadth,depth,draught,shipBuilder,countryOfBuild,yearOfBuild,create_time)
               VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)end''',
                tup)

    # cursor.execute(
    #     f'''
    #     DECLARE @InputIMO varchar(10) = '{vesselInfo['IMONumber']}'
    #     DECLARE @RowCount INT;
    #     SELECT @RowCount = COUNT(*)
    #     FROM polestar_vesselInfo
    #     WHERE IMONumber = @InputIMO;
    #     IF @RowCount = 0 and @InputIMO <> 'None'
    #     Begin
    #    Insert INTO polestar_vesselInfo (imonumber,mmsi,subscriptionID,companyname,shipname,technicalManager,shipType,callSign,flagName,portOfRegistry,classificationSociety,
    #    registeredOwner,operator,deadweight,displacement,grossTonnage,lenthOverall,breadth,depth,draught,shipBuilder,countryOfBuild,yearOfBuild,create_time)
    #    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)end''',
    #     tup)
    return cursor


def delete_polestar_vesselInfo(cursor):
    cursor.execute("TRUNCATE TABLE IF EXISTS polestar_vesselInfo")
    return cursor


def select_polestar_vesselInfo(cursor):
    cursor.execute("SELECT IMONumber,mmsi,subscriptionID from polestar_vesselInfo")
    rows = cursor.fetchall()
    return rows
