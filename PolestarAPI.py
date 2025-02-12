import urllib.request as urllib2
import json
import logging
import os


BASE_URL = os.getenv("BASE_URL_Polestar")
NEWAPI_URL = os.getenv("NEWAPI_URL_Polestar")
API_KEY = os.getenv("API_KEY_Polestar")
USERNAME = os.getenv("USERNAME_Polestar")

logger = logging.getLogger('azure')
logger.setLevel(logging.INFO)

def getAccount():
    requestUrl = BASE_URL + 'account?' + 'api_key=' + API_KEY + '&username=' + USERNAME
    logger.info('requestUrl is : ' + requestUrl)

    response = urllib2.urlopen(requestUrl)
    data = json.load(response)
    logger.info('Successfully retrived Account data from web service')

    return data


def getSubscription():
    requestUrl = BASE_URL + 'subscription?' + 'api_key=' + API_KEY + '&username=' + USERNAME + '&limit=400'
    logger.info('requestUrl is : ' + requestUrl)

    response = urllib2.urlopen(requestUrl)
    data = json.load(response)
    logger.info('Successfully retrived Subscription data from web service')

    return data


def getVoyage(mmsi):
    try:
        requestUrl = NEWAPI_URL + mmsi + '?api_key=' + API_KEY + '&username=' + USERNAME
        logger.info('requestUrl is : ' + requestUrl)

        response = urllib2.urlopen(requestUrl)
        data = json.load(response)
        logger.info('Successfully retrived Voyage data for ' + mmsi + ' from web service')

        return data
    except urllib2.HTTPError as err:
        errorData = {'IMONumber': 'MMSI not found'}
        return errorData


# below function might not be needed, as in email if we only need last known location, getSubscription works better
# Hardcoded timeStamp, subscription_id  could change to parameter

def getPosition(timestamp_gnss__gt, subscription_id):
    requestUrl = BASE_URL + 'position?' + 'api_key=' + API_KEY + '&username=' + USERNAME + '&timestamp_gnss__gt=' + timestamp_gnss__gt + '&subscription_id=' + subscription_id
    logger.info('requestUrl is : ' + requestUrl)

    response = urllib2.urlopen(requestUrl)
    data = json.load(response)
    logger.info('Successfully retrived Subscription data from web service')

    return data
