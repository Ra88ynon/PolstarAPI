#import urllib2#replace with urllib.request 2024June 5
import urllib.request as urllib2
import json
import logging
import time
#import ConfigParser#replace configparsert 2024June 5
import configparser

config = configparser.ConfigParser()
config.read('E:\\WebClient\\config.ini')

BASE_URL = config.get('API','BASE_URL')
NEWAPI_URL = config.get('API','NEWAPI_URL')
API_KEY = config.get('API','API_KEY')
USERNAME = config.get('API','USERNAME')

def getAccount():
	requestUrl = BASE_URL + 'account?' +'api_key=' +API_KEY + '&username=' + USERNAME
	logging.info('requestUrl is : ' + requestUrl)

	response = urllib2.urlopen(requestUrl)
	data =json.load(response)
	logging.info('Successfully retrived Account data from web service')

	return data

def getSubscription():
	requestUrl = BASE_URL + 'subscription?' +'api_key=' +API_KEY + '&username=' + USERNAME + '&limit=400'
	logging.info('requestUrl is : ' + requestUrl)

	response = urllib2.urlopen(requestUrl)
	data =json.load(response)
	logging.info('Successfully retrived Subscription data from web service')

	return data


def getVoyage(mmsi):
	try:
		requestUrl = NEWAPI_URL + mmsi +'?api_key=' +API_KEY + '&username=' + USERNAME
		logging.info('requestUrl is : ' + requestUrl)

		response = urllib2.urlopen(requestUrl)
		data =json.load(response)
		logging.info('Successfully retrived Voyage data for ' +  mmsi +' from web service')

		return data
	except urllib2.HTTPError as err:
		errorData = {'IMONumber':'MMSI not found'}
		return errorData

#below function might not be needed, as in email if we only need last known location, getSubscription works better
#Hardcoded timeStamp, subscription_id  could change to parameter

def getPosition(timestamp_gnss__gt,subscription_id):
	requestUrl = BASE_URL + 'position?' +'api_key=' +API_KEY + '&username=' + USERNAME + '&timestamp_gnss__gt='+ timestamp_gnss__gt +'&subscription_id='+ subscription_id
	logging.info('requestUrl is : ' + requestUrl)

	response = urllib2.urlopen(requestUrl)
	data =json.load(response)
	logging.info('Successfully retrived Subscription data from web service')

	return data
