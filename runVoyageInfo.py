from webClient import *
from database import *
import logging
# import json
import datetime

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    filename='runVoyageInfo.log',  # changed to relative path
    datefmt='%Y-%m-%d %H:%M:%S')


def main():
    conn = create_connection()
    cursor = conn.cursor()
    timeNow = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Create table if not exists
    create_polestar_vesselInfo(cursor)
    create_polestar_voyage(cursor)
    # retrive Voyage data
    results = select_polestar_vesselInfo(cursor)

    for item in results:
        mmsi = str(item.get('mmsi'))
        voyageData = getVoyage(mmsi)

        voyageInfo = {}
        logging.debug('getting voyage info for : ' + mmsi)
        # if no voyage is found
        if voyageData.get('IMONumber') == "MMSI not found":
            pass
        else:
            voyageInfo['IMONumber'] = str(voyageData.get('imo_num'))
            voyageInfo['draught'] = str(voyageData.get('draught'))
            voyageInfo['ETA'] = str(voyageData.get('eta'))
            voyageInfo['destination'] = str(voyageData.get('destination'))
            voyageInfo['shipType'] = str(voyageData.get('ship_type'))
            voyageInfo['lastUpdatedTime'] = str(voyageData.get('timestamp'))
            voyageInfo['create_time'] = timeNow

            # insert into database
            insert_polestar_voyage(cursor, voyageInfo)
            # conn.commit()
    conn.close()


main()
