import os
import pymssql
import logging
#import ConfigParser#replace configparsert 2024June 5
import configparser

config = configparser.ConfigParser()
config.read('E:\\WebClient\\config.ini')


DB_SERVER = config.get('DATABASE','DB_SERVER')
DB_USER = config.get('DATABASE','DB_USER')
DB_PASSWORD = config.get('DATABASE','DB_PASSWORD')
DB_DATABASE = config.get('DATABASE','DB_DATABASE')

def create_connection():
    conn = pymssql.connect(server=DB_SERVER, user=DB_USER,password=DB_PASSWORD,database=DB_DATABASE,as_dict=True)
    return conn

def insert_polestar_position(cursor,vessel):
    #print(vessel)
    tup = (vessel['imo_number'],vessel['speed'],vessel['heading'],vessel['latitude'],vessel['longitude'],vessel['alert_status'],vessel['cyclone_name'],
		vessel['cyclone_type'],vessel['cyclone_distance'],vessel['cyclone_latitude'],vessel['cyclone_longitude'],vessel['cyclone_speed'],vessel['timestamp_gnss'],
		vessel['create_time'])
    logging.info(tup)
    cursor.execute(
        "Insert INTO polestar_position (imonumber,speed,heading,latitude,longitude,alert_status,cyclone_name,cyclone_type,cyclone_distance,cyclone_latitude,cyclone_longitude,cyclone_speed,report_time,create_time) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,convert(datetimeoffset,%s,121),%s)",
        tup)
    return cursor

def insert_polestar_voyage(cursor,voyageInfo):
    tup = (voyageInfo['IMONumber'],voyageInfo['draught'],voyageInfo['ETA'],voyageInfo['destination'],
                    voyageInfo['shipType'],voyageInfo['lastUpdatedTime'],voyageInfo['create_time'])
    logging.info(tup)
    cursor.execute(
        "Insert INTO polestar_voyage (imonumber,draught,eta,destination,shipType,report_time,create_time) VALUES(%s,%s,%s,%s,%s,%s,%s)",
        tup)
    return cursor

def insert_polestar_vesselInfo(cursor,vesselInfo):
    #print(vesselInfo)
    tup = (vesselInfo['IMONumber'],vesselInfo['MMSI'],vesselInfo['subscriptionId'],vesselInfo['companyName'],vesselInfo['shipName'],
            vesselInfo['technicalManager'],vesselInfo['shipType'],vesselInfo['callSign'],vesselInfo['flagName'],vesselInfo['portOfRegistry'],
            vesselInfo['classificationSociety'],vesselInfo['registeredOwner'],vesselInfo['operator'],vesselInfo['deadweight'],vesselInfo['displacement'],vesselInfo['grossTonnage'],
            vesselInfo['lengthOverall'],vesselInfo['breadth'],vesselInfo['depth'],vesselInfo['draught'],vesselInfo['shipBuilder'],
            vesselInfo['countryOfBuild'],vesselInfo['yearOfBuild'],vesselInfo['create_time'])
    logging.info(tup)
    cursor.execute(
       f'''
        DECLARE @InputIMO varchar(10) = '{vesselInfo['IMONumber']}'
        DECLARE @RowCount INT;
        SELECT @RowCount = COUNT(*)
        FROM polestar_vesselInfo
        WHERE IMONumber = @InputIMO;
        IF @RowCount = 0 and @InputIMO <> 'None'
        Begin
       Insert INTO polestar_vesselInfo (imonumber,mmsi,subscriptionID,companyname,shipname,technicalManager,shipType,callSign,flagName,portOfRegistry,classificationSociety,
       registeredOwner,operator,deadweight,displacement,grossTonnage,lenthOverall,breadth,depth,draught,shipBuilder,countryOfBuild,yearOfBuild,create_time)
       VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)end''',
       tup)
    return cursor

def delete_polestar_vesselInfo(cursor):
	cursor.execute("delete from polestar_vesselInfo")
	return cursor

def select_polestar_vesselInfo(cursor):
    cursor.execute("SELECT IMONumber,mmsi,subscriptionID from polestar_vesselInfo")
    rows = cursor.fetchall()
    return rows
