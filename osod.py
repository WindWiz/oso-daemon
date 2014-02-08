#! /usr/bin/env python
"""osod - Onsala space observatory daemon

Collects weather samples from the Onsala space observatory and store
them in a database.

usage: osod [options]

options:
-u <url>   URL to fetch data from
-v         Increase verbosity
-f <db>    Database file (defaults to osod.db)
-s <str>   Simulate input <str>, process and exit
-i <file>  PID file (defaults to /tmp/osod.pid)
-n <inst>  Tag samples gathered with instance <inst>
-p <rate>  Pollrate (in seconds, defaults to 60)
-c <path>  Callback (must be executable)

"""
import getopt
import sys
import os.path
import subprocess
import urllib2
import datetime
import time
import sqlite3

global verbose
global callback

def log(str):
    if (verbose > 0):
        print str

def dbg(str):
    if (verbose > 1):
        print str

def create_database_table(db):
    query = """CREATE TABLE IF NOT EXISTS osod (
instance VARCHAR(255),
pollrate int,
sample_tstamp datetime,
create_tstamp datetime,
airtemp_avg float,
airpressure int,
humidity int,
windspeed_max float,
windspeed_avg float,
windspeed_min float,
wind_dir int)"""

    cursor = db.cursor()
    success = cursor.execute(query)
    if not success:
        return False

    success = cursor.execute('PRAGMA journal_mode=WAL')
    cursor.close()
    db.commit()

    return success

def process(str, db, instance, callback, pollrate):
    dbg("Parsing: '%s'" % str)

    p = str.split(' ')
    if (len(p) != 8):
        log("Invalid packet '%s', unexpected number of tokens '%d'" %
            (str, len(p)))
        return False

    try:
        create_date = time.time()
        sample_date = int(p[0])
        temp = float(p[1])
        airpressure = int(p[2])
        humidity = int(p[3])
        windavg = float(p[4])
        winddir = int(p[5])
        windmax = float(p[6])
        windmin = float(p[7])
    except ValueError:
        log("Invalid packet '%s', illegal field formatting." % str)
        return False

    cursor = db.cursor()

    query = """INSERT INTO osod (instance, pollrate, sample_tstamp,
create_tstamp, airtemp_avg, airpressure, humidity, windspeed_max,
windspeed_min, windspeed_avg, wind_dir)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    dbg("Executing query %s" % query)
    if not cursor.execute(query, (instance, pollrate, sample_date,
        create_date, temp, airpressure, humidity, windmax, windmin,
        windavg, winddir)):
        print "Failed to insert: %s" % str
        cursor.close()
        return False

    db.commit()
    cursor.close()

    log("Sampled at %s UTC " % datetime.datetime.utcfromtimestamp(sample_date))
    log("Retrieved at %s UTC " % datetime.datetime.utcfromtimestamp(create_date))
    log("- Air temperature: %.1f degrees celcius" % temp)
    log("- Air pressure: %d kPa" % airpressure)
    log("- Humidity: %d%%" % humidity)
    log("- Wind: avg=%.1f max=%.1f min=%.1f dir=%d" % (windavg, windmax, windmin, winddir))

    if callback is not None:
        args = [callback]
        if instance is not None:
            args.append(instance)
        try:
            retcode = subprocess.call(args)
            if retcode != 0:
                print "Callback '%s' failed (%d)" % (callback, retcode)
        except Exception, x:
            print "Callback '%s' failed: %s" % (callback, x)

    return True

if __name__ == "__main__":
    pidfile = "/tmp/osod.pid"
    url = "http://www.oso.chalmers.se/~weather/onsala.txt"
    verbose = 0
    callback = None
    dbfile = "osod.db"
    simstr = False
    pollrate = 60
    instance = None

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'u:vs:c:f:i:n:')
    except getopt.error, msg:
        print msg
        sys.exit(1)

    for o, a in opts:
        if o == '-v': verbose = verbose + 1
        if o == '-u': url = a
        if o == '-f': dbfile = a
        if o == '-s': simstr = a
        if o == '-i': pidfile = a
        if o == '-n': instance = a
        if o == '-p': pollrate = a
        if o == '-c':
            if (not os.path.isfile(a)):
                print "No such callback file '%s', aborting." % a
                sys.exit(1)
            if (not os.access(a, os.X_OK)):
                print "Specified callback file '%s' is not an executable." % a
                sys.exit(1)
            callback = a

    log("Using database '%s'" % dbfile)
    db = sqlite3.connect(dbfile)

    # Create database table unless it already exists
    create_database_table(db)

    if (simstr):
        log("Simulating input: %s" % simstr)
        if not process(simstr, db, instance, callback, pollrate):
            sys.exit(1)
    else:
        if (url is False):
            print "No URL specified."
            sys.exit(1)

        pid = str(os.getpid())

        if os.path.isfile(pidfile):
            print "%s already exists, exiting." % pidfile
            sys.exit(1)

        # Initial fetch, to ensure it's valid
        try:
            urllib2.urlopen(url).read()
        except urllib2.URLError, x:
            print "Unable to fetch URL: %s" % x.reason
            sys.exit(1)

        file(pidfile, 'w').write(pid)

        try:
            while True:
                try:
                    inputstr = urllib2.urlopen(url).read().rstrip()
                    process(inputstr, db, instance, callback, pollrate)
                except urllib2.URLError, x:
                    print "URL error: %s (ignoring)" % x.reason
                time.sleep(pollrate)
        except KeyboardInterrupt:
            os.unlink(pidfile)

    db.close()
