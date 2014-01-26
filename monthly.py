#!/usr/bin/python

from query import *
from bs4 import BeautifulSoup
import urllib2
from datetime import date, datetime, timedelta
import json
import MySQLdb as mdb
import sys
from dateutil.relativedelta import relativedelta
import config

def monthly(start_date, end_date):

	tables = ['AgeGroupGender', 'MonthlyViews', 'VidSharingService']
	
	print "Ran on: ", now  
	con = None
	try:
		con = mdb.connect(**config.mysql_params)
		cur = con.cursor()

		videos = getVideoList()

		while (start_date <= end_date):

			
			for table in tables: 

				if table == 'AgeGroupGender' or table == 'MonthlyViews':
					if table == 'MonthlyViews':
						end = start_date
					else:
						end = start_date+relativedelta(months = +1)+relativedelta(days = -1)
					response, columns = query(table, "", start_date, end)
					insert_statement = table_options[table]['insert']

					for row in response:
						cur.execute(insert_statement, getValues(table, "", start_date, columns, row))
						con.commit()
				else:
					for video in videos:
					
						end = start_date+relativedelta(months = +1)+relativedelta(days = -1)
						response, columns = query(table, video, start_date, end)
						insert_statement = table_options[table]['insert']

						for row in response:
							cur.execute(insert_statement, getValues(table, video, start_date, columns, row))
							con.commit()

			start_date = start_date+relativedelta(months = +1)			
					
	except mdb.Error, e:
	  
		print "Error %d: %s" % (e.args[0],e.args[1])
		sys.exit(1)
		
	finally:    
			
		if con:    
			con.close()

#lifetime_start = datetime(2011, 02, 1)

monthly(first_day_last_month, last_day_last_month)
