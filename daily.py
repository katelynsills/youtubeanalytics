#!/usr/bin/python

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
import config


def daily(start_date, end_date):

	tables = ['VidGeneralMetrics', 'VidInsightPlaybackLocationType', 'VidInsightTrafficSourceType', 'VidLifetimeAgeGroupGender']

	con = None
	print "Ran on: ", now
	print "Start Date: ", str(start_date)
	print "End Date: ", str(end_date)
	try:
		con = mdb.connect(**config.mysql_params)
		cur = con.cursor()
		
		trunc_statement = """TRUNCATE VideoInfo"""
		cur.execute(trunc_statement)
		con.commit()

		trunc_VidLifetimeAgeGroupGender = """TRUNCATE VidLifetimeAgeGroupGender"""
		cur.execute(trunc_VidLifetimeAgeGroupGender)
		con.commit()

		videos = getVideoList()

		for video in videos:
			insert_statement = """INSERT INTO VideoInfo (VideoID, Name, PublishedDate) VALUE (%s, %s, %s)"""
			cur.execute(insert_statement, [video, videos[video][0], videos[video][1]])
			con.commit()

		for video in videos:

			for table in tables: 
				if table == 'VidLifetimeAgeGroupGender':
					start_date = lifetime_start
				else:
					start_date = daily_start
				response, columns = query(table, video, start_date, end_date)
				insert_statement = table_options[table]['insert']


				for row in response:
					#print row
					cur.execute(insert_statement, getValues(table, video, start_date, columns, row))
					con.commit()

		#get daily metrics
		start_date = daily_start
		response, columns = query("DailyMetrics", "", start_date, end_date)
		insert_statement = table_options["DailyMetrics"]['insert']
		for row in response:
			cur.execute(insert_statement, getValues("DailyMetrics", "", start_date, columns, row))
			con.commit()

		
		#Twitter
		response = urllib2.urlopen('TWITTERFEED')
		html = response.read()
		soup = BeautifulSoup(html)
		# put Day, NumFollowers in Database
		values = [datetime.today().strftime('%Y-%m-%d'), soup.findAll(href="/USER/followers")[1].find('strong').text.replace(",", "")]
		cur.execute("""INSERT INTO TwitterFollowers VALUES (%s, %s);""", values)
		con.commit()

		#Youtube Cumulative
		response = urllib2.urlopen('http://gdata.youtube.com/feeds/api/users/USER?alt=json')
		youtube_data = json.load(response)
		stats = youtube_data['entry']['yt$statistics']
		values = [datetime.today().strftime('%Y-%m-%d'), stats['subscriberCount'], stats['totalUploadViews']]
		cur.execute("""INSERT INTO YoutubeOverall VALUES (%s, %s, %s);""", values)
		con.commit()	
			
		#Facebook 
		response = urllib2.urlopen('http://graph.facebook.com/USER/')
		facebook_data = json.load(response)

		values = [datetime.today().strftime('%Y-%m-%d'), facebook_data["talking_about_count"], facebook_data["likes"]]
		cur.execute("""INSERT INTO FacebookOverall VALUES (%s, %s, %s);""", values)
		con.commit()
	
	
	except mdb.Error, e:
	  
		print "Error %d: %s" % (e.args[0],e.args[1])
		sys.exit(1)
		
	finally:    
			
		if con:    
			con.close()

daily_start = one_week_ago
daily(daily_start, daily_start)


