from daily import *
from monthly import *
from query import *
from datetime import date, datetime, timedelta
import httplib2
import sys
import MySQLdb as mdb
from apiclient.discovery import build
from optparse import OptionParser
from refresh import oauth_credentials
from time import sleep
from dateutil.relativedelta import relativedelta

#insert the date from which you'd like to start counting
lifetime_start = datetime(2011, 02, 1)

daily(lifetime_start, now)
monthly(lifetime_start, now)