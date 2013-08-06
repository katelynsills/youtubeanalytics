from query import *
from datetime import date, datetime, timedelta
import httplib2
import sys
import MySQLdb as mdb
from apiclient.discovery import build
from optparse import OptionParser
from refresh import oauth_credentials
from bs4 import BeautifulSoup
import urllib2
import json


def daily(start_date, end_date):

	tables = ['VidGeneralMetrics', 'VidInsightPlaybackLocationType', 'VidInsightTrafficSourceType']

	con = None
	try:
		con = mdb.connect(**mysql_params)
		cur = con.cursor()

		for video in getVideoList():
			for table in tables: 
				
				response, columns = query(table, video, start_date, end_date)
				insert_statement = table_options[table]['insert']


				for row in response:
					cur.execute(insert_statement, getValues(table, video, start_date, columns, row))
					con.commit()

		#Twitter
		response = urllib2.urlopen('https://twitter.com/LearnLiberty')
		html = response.read()
		soup = BeautifulSoup(html)
		# put Day, NumFollowers in Database
		values = [datetime.today().strftime('%Y-%m-%d'), soup.findAll(href="/LearnLiberty/followers")[1].find('strong').text.replace(",", "")]
		cur.execute("""INSERT INTO TwitterFollowers VALUES (%s, %s);""", values)
		con.commit()

		#Youtube Cumulative
		response = urllib2.urlopen('http://gdata.youtube.com/feeds/api/users/LearnLiberty?alt=json')
		youtube_data = json.load(response)
		stats = youtube_data['entry']['yt$statistics']
		values = [datetime.today().strftime('%Y-%m-%d'), stats['subscriberCount'], stats['totalUploadViews']]
		cur.execute("""INSERT INTO YoutubeOverall VALUES (%s, %s, %s);""", values)
		con.commit()	

		video_list = youtube.search().list(part="snippet", maxResults=50, type="video", channelId=CHANNEL_ID).execute()

		numQueries = video_list['pageInfo']['totalResults']/50 
		videos = []
		names = []
		published = []

		trunc_statement = """TRUNCATE VideoInfo"""
		cur.execute(trunc_statement)
		con.commit()

		for video in video_list['items']:
			videos.append(video['id']['videoId'])
			names.append(video['snippet']['title'])
			published.append(video['snippet']['publishedAt'])
		while ('nextPageToken' in video_list.keys()):
			video_list = youtube.search().list(part="snippet", maxResults=50, type="video", pageToken=video_list['nextPageToken'], channelId=CHANNEL_ID).execute()
			for video in video_list['items']:
				videos.append(video['id']['videoId'])
				names.append(video['snippet']['title'])
				published.append(video['snippet']['publishedAt'])	

		for idx, value in enumerate(videos):
			insert_statement = """INSERT INTO VideoInfo VALUE (%s, %s, %s)"""
			print [value, names[idx], published[idx]]
			cur.execute(insert_statement, [value, names[idx], published[idx]])
			con.commit()
					
	except mdb.Error, e:
	  
		print "Error %d: %s" % (e.args[0],e.args[1])
		sys.exit(1)
		
	finally:    
			
		if con:    
			con.close()

daily(two_days_ago, now)
