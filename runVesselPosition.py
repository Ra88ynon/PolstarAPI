from webClient import *
from database import *
import logging
# import json
import datetime

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    filename='runVesselPosition.log',  # changed to relative path
    datefmt='%Y-%m-%d %H:%M:%S')


def main():
    conn = create_connection()
    cursor = conn.cursor()
    timeNow = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Create table if not exists
    create_polestar_position(cursor)

    # retrive Position data
    positionData = getSubscription()  # Subscription gives last known position of all vessels

    vesselPosition = {}
    logging.info('Constructing vessel position object')
    for item in positionData['objects']:
        vesselPosition['imo_number'] = str(item.get('ship').get('imo_number'))
        vesselPosition['create_time'] = timeNow
        if item.get('last_position') is not None:
            vesselPosition['latitude'] = str(item.get('last_position').get('latitude'))
            vesselPosition['latitude'] = vesselPosition['latitude'][:14]
            vesselPosition['longitude'] = str(item.get('last_position').get('longitude'))
            vesselPosition['longitude'] = vesselPosition['longitude'][:14]
            vesselPosition['speed'] = str(item.get('last_position').get('sog_reported'))
            vesselPosition['speed'] = vesselPosition['speed'][:14]
            vesselPosition['heading'] = str(item.get('last_position').get('cog_reported'))
            vesselPosition['heading'] = vesselPosition['heading'][:14]
            vesselPosition['timestamp_gnss'] = str(item.get('last_position').get('timestamp_gnss'))
            vesselPosition['alert_status'] = str(item.get('last_position').get('position_info').get('alert_status'))
            # Looks like cyclone reported actual observation and forcast, first item is always actual observation
            if len(item.get('last_position').get('position_info').get('cyclones')) > 0:
                vesselPosition['cyclone_name'] = \
                    item.get('last_position').get('position_info').get('cyclones')[0].get('track')[0].get('name')
                vesselPosition['cyclone_type'] = \
                    item.get('last_position').get('position_info').get('cyclones')[0].get('track')[0].get('storm_type')
                vesselPosition['cyclone_distance'] = \
                    item.get('last_position').get('position_info').get('cyclones')[0].get('track')[0].get('distance')
                vesselPosition['cyclone_latitude'] = \
                    item.get('last_position').get('position_info').get('cyclones')[0].get('track')[0].get('latitude')
                vesselPosition['cyclone_longitude'] = \
                    item.get('last_position').get('position_info').get('cyclones')[0].get('track')[0].get('longitude')
                vesselPosition['cyclone_speed'] = \
                    item.get('last_position').get('position_info').get('cyclones')[0].get('track')[0].get('speed')
            else:
                vesselPosition['cyclone_name'] = ''
                vesselPosition['cyclone_type'] = ''
                vesselPosition['cyclone_distance'] = ''
                vesselPosition['cyclone_latitude'] = ''
                vesselPosition['cyclone_longitude'] = ''
                vesselPosition['cyclone_speed'] = ''
        else:
            vesselPosition['latitude'] = ''
            vesselPosition['longitude'] = ''
            vesselPosition['speed'] = ''
            vesselPosition['heading'] = ''
            vesselPosition['timestamp_gnss'] = ''
            vesselPosition['alert_status'] = ''
            vesselPosition['cyclone_name'] = ''
            vesselPosition['cyclone_type'] = ''
            vesselPosition['cyclone_distance'] = ''
            vesselPosition['cyclone_latitude'] = ''
            vesselPosition['cyclone_longitude'] = ''
            vesselPosition['cyclone_speed'] = ''
        logging.debug('Inserting vessel position for : ' + vesselPosition['imo_number'] + ' latitude : ' +
                      vesselPosition['latitude'] + ' longitude :' + vesselPosition['longitude'] + ' cyclone_name :' +
                      vesselPosition['longitude'])
        insert_polestar_position(cursor, vesselPosition)

    logging.info('Finished inserting position Info')

    conn.commit()


main()
