#!/usr/bin/python

from datetime import date, datetime, timedelta
import httplib2
import sys
import MySQLdb as mdb
from apiclient.discovery import build
from optparse import OptionParser
from refresh import oauth_credentials
from time import sleep

mysql_params = dict(
	host='host-name',
	user='user-name',
	passwd='password',
	db='database-name'
)

YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
YOUTUBE_ANALYTICS_API_SERVICE_NAME = "youtubeAnalytics"
YOUTUBE_ANALYTICS_API_VERSION = "v1"

credentials = oauth_credentials()

http = credentials.authorize(httplib2.Http())
youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, http=http)
youtube_analytics = build(YOUTUBE_ANALYTICS_API_SERVICE_NAME,
						  YOUTUBE_ANALYTICS_API_VERSION, http=http)


# Defining the temporal variables
# to use in a query, use the function .strftime("%Y-%m-%d")

now = datetime.now()
one_day_ago = (now - timedelta(days=1))
two_days_ago = (now - timedelta(days=2))
one_week_ago = (now - timedelta(days=7))
first_day_this_month = date(day=1, month=now.month, year=now.year)
last_day_last_month = (first_day_this_month - timedelta(days=1))
first_day_last_month = date(day=1, month=last_day_last_month.month, year=last_day_last_month.year)

CHANNEL_ID = 'Channel_id'

table_options = { 
		'VidGeneralMetrics': {
			'dimensions': "day",
			'metrics': "views,comments,favoritesAdded,favoritesRemoved,likes,dislikes,shares,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,annotationClickThroughRate,annotationCloseRate,subscribersGained,subscribersLost,uniques",
			'insert': """INSERT INTO VidGeneralMetrics VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s );""",
			'filters': "video_id"
		},
		'VidInsightPlaybackLocationType': {
			'dimensions': "day,insightPlaybackLocationType",
			'metrics': "views,estimatedMinutesWatched",
			'insert': """INSERT INTO VidInsightPlaybackLocationType VALUES (%s, %s, %s, %s, %s, %s, %s );""",
			'filters': 'video_id'
		},
		'VidInsightTrafficSourceType': {
			'dimensions': "day,insightTrafficSourceType",
			'metrics': "views,estimatedMinutesWatched",
			'insert': """INSERT INTO VidInsightTrafficSourceType  VALUES (%s, %s, %s, %s, %s, %s, %s );""",
			'filters' : 'video_id'
		},
		'VidAgeGroupGender' : {
			'dimensions' : "ageGroup,gender",
			'metrics' : "viewerPercentage",
			'insert' : """INSERT INTO VidAgeGroupGender VALUES (%s, %s, %s, %s, %s, %s, %s, %s );""",
			'filters' : 'video_id'
		},
		'VidSharingService' : {
			'dimensions' : "sharingService",
			'metrics' : "shares",
			'insert' : """INSERT INTO VidSharingService VALUES (%s, %s, %s, %s, %s, %s, %s );""",
			'filters' : 'video_id'
		},
		'VidCountry' : {
			'dimensions' : "country",
			'metrics' : "views,comments,favoritesAdded,favoritesRemoved,likes,dislikes,shares,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,annotationClickThroughRate,annotationCloseRate,subscribersGained,subscribersLost",
			'insert' : """INSERT INTO VidCountry VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s );""",
			'filters' : 'video_id'
		},
		'VidAgeGroup' : {
			'dimensions' : "ageGroup,gender",
			'metrics' : "viewerPercentage",
			'insert' : """INSERT INTO VidAgeGroup VALUES (%s, %s, %s, %s, %s, %s, %s);""",
			'filters' : 'None'
		},
		'MonthlyViews' : {
			'dimensions' : "month",
			'metrics' : "views",
			'insert' : """INSERT INTO MonthlyViews VALUES (%s, %s, %s, %s, %s);""",
			'filters' : 'None'
		}
	}

def query(table, video_id, start_date, end_date):

	
	sleep(1)
	curr_options = table_options[table]
	filters = ''
	if curr_options['filters'] == 'video_id':
		filters = "video=="+video_id

	analytics_response =  youtube_analytics.reports().query(
			  ids="channel==%s" % CHANNEL_ID,
			  dimensions=curr_options['dimensions'],
			  metrics=curr_options['metrics'], 
			  start_date=start_date.strftime("%Y-%m-%d"), 
			  end_date=end_date.strftime("%Y-%m-%d"),
			  filters=filters,
				).execute()
	columns = []
	for column_header in analytics_response.get("columnHeaders", []):
		columns.append(column_header["name"])
	response = analytics_response.get("rows", [])
	return (response, columns)

def getVideoList():

	#get the first page of results for that channel
	video_list = youtube.search().list(part="id", maxResults=50, type="video", channelId=CHANNEL_ID).execute()

	# The number of queries needed to get all the results, with 50 results per page
	numQueries = video_list['pageInfo']['totalResults']/50
	videos = []

	#add the first page of results to the list of videos
	for video in video_list['items']:
		videos.append(video['id']['videoId'])
	
	#get the rest of the results and add them to the list of videos	
	while (numQueries > 0):
		video_list = youtube.search().list(part="id", maxResults=50, type="video", pageToken=video_list["nextPageToken"], channelId=CHANNEL_ID).execute()
		for video in video_list['items']:
			videos.append(video['id']['videoId'])
		numQueries = numQueries - 1
	return videos

def getValues(table, video_id, start_date, columns, row):
	row_dict = {}
	for idx, item in enumerate(row):
		row_dict[columns[idx]] = item

	values = None

	#daily ones, have the correct day from youtube
	if table in ['VidGeneralMetrics', 'VidInsightPlaybackLocationType', 'VidInsightTrafficSourceType']:
		values = [row_dict['day'], video_id, CHANNEL_ID, "Learn Liberty"]
	 	values = values + row[1:]

	#monthly ones that are determined by start and end dates and by video	
	elif table in ['VidAgeGroupGender', 'VidSharingService', 'VidCountry']:
		values = [start_date.month, start_date.year, video_id, CHANNEL_ID, "Learn Liberty"]
		values = values + row

	#monthly ones that are determined by start and end dates	
	elif table == 'VidAgeGroup':
		values = [start_date.month, start_date.year, CHANNEL_ID, "Learn Liberty", row[0], row[1], "%.4f" % float(row[2])]

	#MonthlyViews has month from youtube
	elif table == 'MonthlyViews':
		values = [int(row[0][5:]), int(row[0][:4]), CHANNEL_ID, "Learn Liberty", row[1]]

	print values
	return values
	#print values