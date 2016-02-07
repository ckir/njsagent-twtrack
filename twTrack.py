#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'user'
import os
import json
import HTMLParser
import logging
import re
from threading import Timer
import time

# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')

from sys import exit
from datetime import datetime

# Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import HTMLParser
import urlparse
import psycopg2
from psycopg2.extras import Json

if "NJSAGENT_APPROOT" in os.environ:
    approot = os.getenv('NJSAGENT_APPROOT', "") + "/logs"
else:
    approot = os.path.dirname(os.path.realpath(__file__))

logfile = approot + "/" + os.path.basename(__file__) + ".log"
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=logfile)
# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.ERROR)
# set a format which is simpler for console use
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
# tell the handler to use this format
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger('').addHandler(console)
log = logging.getLogger('')

if not "NJSAGENT_APPROOT" in os.environ:
    log.warning("Missing NJSAGENT_APPROOT environment variable")
    exit(1)

conn = ''
cur = ''
sources = []
terms = []
recieved = 0
found = 0
lastdata = datetime.now()
inserted = []
pgtable = ''

html = HTMLParser.HTMLParser()

class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer = None
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False


def report():
    log.info("Recieved " + str(recieved) + " found " + str(found))


def check_inactive():
    diff = datetime.now() - lastdata
    total_seconds = diff.seconds
    if total_seconds > (60 * 15):
        log.warn("Stream idle: Lastdata recieved " + lastdata.strftime("%Y-%m-%d %H:%M:%S.%f") + " now " + datetime.now() + " diff" + str(total_seconds))
        exit(1)


# This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):
    def on_data(self, data):
        global recieved, found, lastdata, inserted
        recieved += 1
        lastdata = datetime.now()
        try:
            tweet = json.loads(data)
            try:
                if self.is_valid_content(tweet) and self.is_valid_source(tweet):
                    if not tweet['id_str'] in inserted:
                        found += 1
                        self.save_tweet(tweet)
            except KeyError, AssertionError:
                pass
        except Exception, e:
            log.exception(str(e))
            log.warning('Bad tweet json' + data)
        return True

    def on_error(self, status):
        log.error(status)
        return False

    def is_valid_content(self, tweet):
        global terms, html
        text = html.unescape(tweet['text']).lower()
        if any(term in text for term in terms):
            return True
        else:
            return False

    def is_valid_source(self, tweet):
        global sources
        if len(sources) == 0:
            return True
        id = tweet['user']['id_str']
        if any(source in id for source in sources):
            return True
        else:
            return False

    def save_tweet(self, tweet):
        global inserted, pgtable
        try:
            data = json.dumps(tweet)
            idata = '{"data": [' + data + ']}'
            sqlt = "INSERT INTO public.%TABLE% (%TABLE%_date, %TABLE%_jdata) VALUES (DEFAULT, %s::jsonb) ON CONFLICT (%TABLE%_date) DO UPDATE set %TABLE%_jdata=jsonb_set(%TABLE%.%TABLE%_jdata::jsonb, '{data,2147483647}', %s::jsonb, true)"
            sql = sqlt.replace("%TABLE%", pgtable)
            cur.execute(sql, [idata, data])
            inserted.append(tweet['id_str'])
        except psycopg2.Error as e:
            log.warning("Cannot insert tweet because " + e.pgerror)


def main():
    global conn, cur, pgtable, sources, terms

    if not "TWTRACK_PGSQL" in os.environ:
        log.warning("Missing TWTRACK_PGSQL environment variable")
        exit(1)
    if not "TWTRACK_PGSQL_TABLE" in os.environ:
        log.warning("Missing TWTRACK_PGSQL_TABLE environment variable")
        exit(1)
    if not "TWTRACK_TWITTER_ACCESS_TOKEN_KEY" in os.environ:
        log.warning("Missing TWTRACK_TWITTER_ACCESS_TOKEN_KEY environment variable")
        exit(1)
    if not "TWTRACK_TWITTER_ACCESS_TOKEN_SECRET" in os.environ:
        log.warning("Missing TWTRACK_TWITTER_ACCESS_TOKEN_SECRET environment variable")
        exit(1)
    if not "TWTRACK_TWITTER_CONSUMER_KEY" in os.environ:
        log.warning("Missing TWTRACK_TWITTER_CONSUMER_KEY environment variable")
        exit(1)
    if not "TWTRACK_TWITTER_CONSUMER_SECRET" in os.environ:
        log.warning("Missing TWTRACK_TWITTER_CONSUMER_SECRET environment variable")
        exit(1)
    if not "TWTRACK_TERMS" in os.environ:
        log.warning("Missing TWTRACK_TERMS environment variable")
        exit(1)

    pgsql = os.getenv('TWTRACK_PGSQL', '')
    pgtable = os.getenv('TWTRACK_PGSQL_TABLE', '')
    # Variables that contains the user credentials to access Twitter API
    access_token = os.getenv('TWTRACK_TWITTER_ACCESS_TOKEN_KEY', '')
    access_token_secret = os.getenv('TWTRACK_TWITTER_ACCESS_TOKEN_SECRET', '')
    consumer_key = os.getenv('TWTRACK_TWITTER_CONSUMER_KEY', '')
    consumer_secret = os.getenv('TWTRACK_TWITTER_CONSUMER_SECRET', '')
    trms = os.getenv('TWTRACK_TERMS', '').lower()
    terms=[x.strip() for x in trms.split(',')]
    srces = os.getenv('TWTRACK_SOURCES', '')
    if len(srces) > 0:
        sources = [x.strip() for x in srces.split(',')]

    log.info("Started tracking " + trms)
    
    try:
        urlparse.uses_netloc.append('postgres')
        url = urlparse.urlparse(pgsql)
        conn = psycopg2.connect(
            "dbname=%s user=%s password=%s host=%s " % (url.path[1:], url.username, url.password, url.hostname))
        conn.autocommit = True
        cur = conn.cursor()
    except  psycopg2.Error as e:
        log.error("I am unable to connect to the database because " + e.pgerror)
        exit(1)

    # This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    # This line filter Twitter Streams to capture data by the keywords
    stream.filter(track=terms)


if __name__ == '__main__':
    try:
        rt = RepeatedTimer(60 * 10, report)  # it auto-starts, no need of rt.start()
        ia = RepeatedTimer(60, check_inactive)
        main()
    except KeyboardInterrupt:
        logging.shutdown()
        print '\nGoodbye!'
        exit()
    except Exception, e:
        log.exception(str(e))
        logging.shutdown()
    finally:
        rt.stop()  # better in a try/finally block to make sure the program ends!
        ia.stop()
        time.sleep(60)
        log.warn("Exiting")
        exit(1)
