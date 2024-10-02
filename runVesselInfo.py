from webClient import *
from database import *
import logging
import json
import datetime

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    filename='E:\\WebClient\\runVesselInfo.log',
    datefmt='%Y-%m-%d %H:%M:%S')


def main():
    conn = create_connection()
    cursor = conn.cursor()
    timeNow = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # retrive Voyage data

    delete_polestar_vesselInfo(cursor)

    positionData = getSubscription()  # Subscription gives last known position of all vessels

    vesselInfo = {}
    logging.info('Constructing vessel object')

    for item in positionData['objects']:
        vesselInfo['IMONumber'] = str(item.get('ship').get('imo_number'))
        vesselInfo['MMSI'] = str(item.get('ship').get('mmsi'))
        vesselInfo['subscriptionId'] = item.get('id')
        vesselInfo['companyName'] = str(item.get('account').get('company_name'))
        vesselInfo['shipName'] = str(item.get('ship').get('ship_name'))
        vesselInfo['technicalManager'] = str(item.get('ship').get('technical_manager'))
        vesselInfo['shipType'] = str(item.get('ship').get('ship_type'))
        vesselInfo['callSign'] = str(item.get('ship').get('call_sign'))
        if item.get('ship').get('flag') is not None:
            vesselInfo['flagName'] = str(item.get('ship').get('flag').get('name'))
        else:
            vesselInfo['flagName'] = ''
        vesselInfo['portOfRegistry'] = str(item.get('ship').get('port_of_registry'))
        vesselInfo['classificationSociety'] = str(item.get('ship').get('classification_society'))
        vesselInfo['registeredOwner'] = str(item.get('ship').get('registered_owner'))
        vesselInfo['operator'] = str(item.get('ship').get('operator'))
        if item.get('ship').get('deadweight') is not None:
            vesselInfo['deadweight'] = str(item.get('ship').get('deadweight'))
        else:
            vesselInfo['deadweight'] = ''
        if item.get('ship').get('displacement') is not None:
            vesselInfo['displacement'] = str(item.get('ship').get('displacement'))
        else:
            vesselInfo['displacement'] = ''
        if item.get('ship').get('grossTonnage') is not None:
            vesselInfo['grossTonnage'] = str(item.get('ship').get('gross_tonnage'))
        else:
            vesselInfo['grossTonnage'] = ''
        if item.get('ship').get('length_overall_loa') is not None:
            vesselInfo['lengthOverall'] = str(item.get('ship').get('length_overall_loa'))
        else:
            vesselInfo['lengthOverall'] = ''

        if item.get('ship').get('breadth') is not None:
            vesselInfo['breadth'] = str(item.get('ship').get('breadth'))
        else:
            vesselInfo['breadth'] = ''
        if item.get('ship').get('depth') is not None:
            vesselInfo['depth'] = str(item.get('ship').get('depth'))
        else:
            vesselInfo['depth'] = ''
        if item.get('ship').get('draught') is not None:
            vesselInfo['draught'] = str(item.get('ship').get('draught'))
        else:
            vesselInfo['draught'] = ''
        if item.get('ship').get('shipbuilder') is not None:
            vesselInfo['shipBuilder'] = str(item.get('ship').get('shipbuilder'))
        else:
            vesselInfo['shipBuilder'] = ''

        vesselInfo['countryOfBuild'] = str(item.get('ship').get('country_of_build'))
        if item.get('ship').get('year_of_build') is not None:
            vesselInfo['yearOfBuild'] = str(item.get('ship').get('year_of_build'))
        else:
            vesselInfo['yearOfBuild'] = ''
        vesselInfo['create_time'] = timeNow
        insert_polestar_vesselInfo(cursor, vesselInfo)
        logging.info('Inserting vessel: ' + vesselInfo['IMONumber'] + ' : ' + vesselInfo['shipName'])
    logging.info('Finished writing vessel Info')

    conn.commit()


# insert into database

main()
