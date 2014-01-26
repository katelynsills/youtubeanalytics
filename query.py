#!/usr/bin/python

import httplib
import os
import random
import time
from apiclient.errors import HttpError
from datetime import date, datetime, timedelta
import httplib2
import sys
import MySQLdb as mdb
from apiclient.discovery import build
from refresh import oauth_credentials
from time import sleep
import config

credentials = oauth_credentials()

http = credentials.authorize(httplib2.Http())
youtube = build(config.YOUTUBE_API_SERVICE_NAME, config.YOUTUBE_API_VERSION, http=http)
youtube_analytics = build(config.YOUTUBE_ANALYTICS_API_SERVICE_NAME,
						  config.YOUTUBE_ANALYTICS_API_VERSION, http=http)


# Defining the temporal variables
# to use in a query, use the function .strftime("%Y-%m-%d")

now = datetime.now()
one_day_ago = (now - timedelta(days=1))
two_days_ago = (now - timedelta(days=2))
one_week_ago = (now - timedelta(days=7))
eight_days_ago = (now - timedelta(days=8))
first_day_this_month = date(day=1, month=now.month, year=now.year)
last_day_last_month = (first_day_this_month - timedelta(days=1))
first_day_last_month = date(day=1, month=last_day_last_month.month, year=last_day_last_month.year)


table_options = { 
		'VidGeneralMetrics': {
			'dimensions': "day",
			'metrics': "views,comments,favoritesAdded,favoritesRemoved,likes,dislikes,shares,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,annotationClickThroughRate,annotationCloseRate,subscribersGained,subscribersLost,uniques",
			'insert': """INSERT INTO VidGeneralMetrics (Day, VideoID, Views, Comments, FavoritesAdded, FavoritesRemoved, Likes, Dislikes, Shares, EstimatedMinutesWatched, AverageViewDuration, AverageViewPercentage, AnnotationClickThroughRate, AnnotationCloseRate, SubscribersGained, SubscribersLost, Uniques) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
			'filters': "video_id"
		},
		'VidInsightPlaybackLocationType': {
			'dimensions': "day,insightPlaybackLocationType",
			'metrics': "views,estimatedMinutesWatched",
			'insert': """INSERT INTO VidInsightPlaybackLocationType (Day, VideoID, InsightPlaybackLocationType, Views, EstimatedMinutesWatched) VALUES (%s, %s, %s, %s, %s);""",
			'filters': 'video_id'
		},
		'VidInsightTrafficSourceType': {
			'dimensions': "day,insightTrafficSourceType",
			'metrics': "views,estimatedMinutesWatched",
			'insert': """INSERT INTO VidInsightTrafficSourceType  (Day, VideoID, InsightTrafficSourceType, Views, EstimatedMinutesWatched) VALUES (%s, %s, %s, %s, %s);""",
			'filters' : 'video_id'
		},
		'VidLifetimeAgeGroupGender' : {
			'dimensions' : "ageGroup,gender",
			'metrics' : "viewerPercentage",
			'insert' : """INSERT INTO VidLifetimeAgeGroupGender (VideoID, AgeGroup, Gender, ViewerPercentage) VALUES (%s, %s, %s, %s);""",
			'filters' : 'video_id'
		},
		'VidSharingService' : {
			'dimensions' : "sharingService",
			'metrics' : "shares",
			'insert' : """INSERT INTO VidSharingService (Month, VideoID, SharingService, Shares) VALUES (%s, %s, %s, %s);""",
			'filters' : 'video_id'
		},
		'AgeGroupGender' : {
			'dimensions' : "ageGroup,gender",
			'metrics' : "viewerPercentage",
			'insert' : """INSERT INTO AgeGroupGender (Month, AgeGroup, Gender, ViewerPercentage) VALUES (%s, %s, %s, %s);""",
			'filters' : 'None'
		},
		'MonthlyViews' : {
			'dimensions' : "month",
			'metrics' : "views",
			'insert' : """INSERT INTO MonthlyViews (Month, Views) VALUES (%s, %s);""",
			'filters' : 'None'
		},
		'DailyMetrics' : {
			'dimensions': "day",
			'metrics': "views,comments,favoritesAdded,favoritesRemoved,likes,dislikes,shares,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,annotationClickThroughRate,annotationCloseRate,subscribersGained,subscribersLost,uniques",
			'insert': """INSERT INTO DailyMetrics (Day, Views, Comments, FavoritesAdded, FavoritesRemoved, Likes, Dislikes, Shares, EstimatedMinutesWatched, AverageViewDuration, AverageViewPercentage, AnnotationClickThroughRate, AnnotationCloseRate, SubscribersGained, SubscribersLost, Uniques) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
			'filters': 'None'
		}
	}

def query(table, video_id, start_date, end_date):

	analytics_response = None
	error = None
	retry = 0

	sleep(1)
	curr_options = table_options[table]
	filters = ''
	if curr_options['filters'] == 'video_id':
		filters = "video=="+video_id

	while analytics_response is None:
		try:
			analytics_response =  youtube_analytics.reports().query(
			  ids="channel==%s" % config.CHANNEL_ID,
			  dimensions=curr_options['dimensions'],
			  metrics=curr_options['metrics'], 
			  start_date=start_date.strftime("%Y-%m-%d"), 
			  end_date=end_date.strftime("%Y-%m-%d"),
			  filters=filters,
				).execute()
			if 'columnHeaders' not in analytics_response:
				exit("The upload failed with an unexpected response: %s" % analytics_response)
		except HttpError, e:
		  if e.resp.status in config.RETRIABLE_STATUS_CODES:
			error = "A retriable HTTP error %d occurred:\n%s" % (e.resp.status,
																 e.content)
		  else:
			raise
		except config.RETRIABLE_EXCEPTIONS, e:
		  error = "A retriable error occurred: %s" % e
		if error is not None:
		  print error
		  retry += 1
		  if retry > config.MAX_RETRIES:
			exit("No longer attempting to retry.")

		  max_sleep = 2 ** retry
		  sleep_seconds = random.random() * max_sleep
		  print "Sleeping %f seconds and then retrying..." % sleep_seconds
		  time.sleep(sleep_seconds)
	
	columns = []
	for column_header in analytics_response.get("columnHeaders", []):
		columns.append(column_header["name"])
	response = analytics_response.get("rows", [])
	return (response, columns)


def getOrderedVideoList(orderBy, results_per_page):
	video_list = None
	error = None
	retry = 0
	while video_list is None:
		try:
			video_list = youtube.search().list(part="snippet", maxResults=results_per_page, order=orderBy, type="video", channelId=config.CHANNEL_ID).execute()
			if 'items' not in video_list:
				#print "Get video_list was a success"
				exit("Getting video_list failed with an unexpected response: %s" % video_list)
		except HttpError, e:
		  if e.resp.status in RETRIABLE_STATUS_CODES:
			error = "A retriable HTTP error %d occurred:\n%s" % (e.resp.status,
																 e.content)
		  else:
			raise
		except RETRIABLE_EXCEPTIONS, e:
		  error = "A retriable error occurred: %s" % e
		if error is not None:
		  print error
		  retry += 1
		  if retry > MAX_RETRIES:
			exit("No longer attempting to retry.")

		  max_sleep = 2 ** retry
		  sleep_seconds = random.random() * max_sleep
		  print "Sleeping %f seconds and then retrying..." % sleep_seconds
		  time.sleep(sleep_seconds)

	# a dictionary with videoID as the key, and the tuple (title, publishedAt) as the value
	videoInfo = {}

	# the first page of results to the list of videos
	for video in video_list['items']:
		videoInfo[video['id']['videoId']] = (video['snippet']['title'], video['snippet']['publishedAt'])

	#get the rest of the results and add them to the list of videos	
	while ("nextPageToken" in video_list):
		video_list = youtube.search().list(part="snippet", maxResults=results_per_page, order=orderBy, type="video", pageToken=video_list["nextPageToken"], channelId=config.CHANNEL_ID).execute()
		for video in video_list['items']:
			videoInfo[video['id']['videoId']] = (video['snippet']['title'], video['snippet']['publishedAt'])
	return videoInfo

def getVideoList():
	results_per_page = 25

	dict = getOrderedVideoList("date", results_per_page)
	dict2 = getOrderedVideoList("title", results_per_page)
	dict3 = getOrderedVideoList("viewCount", results_per_page)
	for key in dict2:
		if key not in dict:
			dict[key] = dict2[key]

	for key in dict3:
		if key not in dict:
			dict[key] = dict3[key]

	return dict


def getValues(table, video_id, start_date, columns, row):

	values = None

	#daily ones, have the correct day from youtube
	if table in ['VidGeneralMetrics', 'VidInsightPlaybackLocationType', 'VidInsightTrafficSourceType']:
		values = [row[0], video_id] + row[1:]

	#monthly ones that are determined by start and end dates and by video	
	elif table in ['VidAgeGroupGender', 'VidSharingService']:
		values = [start_date, video_id] + row

	#monthly ones that are determined by start and end dates	
	elif table == 'AgeGroupGender':
		values = [start_date, row[0], row[1], "%.4f" % float(row[2])]

	#MonthlyViews has month from youtube
	elif table == 'MonthlyViews':
		values = [datetime(int(row[0].split("-")[0]), int(row[0].split("-")[1]), 1).strftime("%Y-%m-%d"), row[1]]

	elif table == 'VidLifetimeAgeGroupGender':
		values = [video_id] + row

	elif table == 'DailyMetrics':
		values = row

	#print values
	return values
