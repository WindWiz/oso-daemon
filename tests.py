#!/usr/bin/env python 
import unittest
import subprocess
import sqlite3
import uuid
import os
import tempfile
import stat

class osodTests(unittest.TestCase):
	def setUp(self):
		self.cbfile = os.path.join(os.getcwd(), str(uuid.uuid4()))
		self.dbfile = os.path.join(os.getcwd(), str(uuid.uuid4()))
				
	def tearDown(self):
		if os.path.exists(self.dbfile):
			os.unlink(self.dbfile)
		if os.path.exists(self.cbfile):
			os.unlink(self.cbfile)

	def run_osod(self, input, dbfile, instance = None, callback = None):
		args = ["./osod.py", "-s", input, "-f", dbfile]

		if instance is not None:
			args.extend(["-n", instance])
		
		if callback is not None:
			args.extend(["-c", callback])
		
		return subprocess.call(args)
		
	def get_test_db(self, dbfile):
		db = sqlite3.connect(dbfile)
		cursor = db.cursor()
		query = """SELECT
instance,
sample_tstamp,
create_tstamp,
airtemp_avg,
airpressure,
humidity,
windspeed_max,
windspeed_avg,
windspeed_min,
wind_dir
FROM osod"""
		if not cursor.execute(query):
			return None
		
		rows = cursor.fetchall()
		cursor.close()
		return rows
		
	""" Process valid entry """
	def testValidEntry(self):		
		input = "1348384242 9.4 1011 86 3.5 349 5.1"
		self.assertEqual(self.run_osod(input, self.dbfile), 0)
		
		rows = self.get_test_db(self.dbfile)
		self.assertEqual(len(rows), 1)
		
	""" Process entry with invalid token count """
	def testInvalidTokenCount(self):
		input = "1348384242 9.4 1011 86 3.5 349 5.1 0"
		self.assertNotEqual(self.run_osod(input, self.dbfile), 0)		

		rows = self.get_test_db(self.dbfile)
		self.assertEqual(len(rows), 0)		
		
	""" Try inserting random garbage """
	def testGarbageInput(self):
		input = "this is not valid input"
		self.assertNotEqual(self.run_osod(input, self.dbfile), 0)		

		rows = self.get_test_db(self.dbfile)
		self.assertEqual(len(rows), 0)	
	
	""" Try inserting something almost valid, but has incorrect field datatypes """
	def testIncorrectDatatype(self):
		input = "A B C D E F G"
		self.assertNotEqual(self.run_osod(input, self.dbfile), 0)		

		rows = self.get_test_db(self.dbfile)
		self.assertEqual(len(rows), 0)

	""" Fetch an invalid URL """
	def testInvalidURL(self):
		args = ["./osod.py", "-u", "http://invalidurl/"]
		self.assertNotEqual(subprocess.call(args), 0)
		
	def testCallback(self):				
		input = "1 2 3 4 5 6 7"
				
		# Basic callback script which writes "hello world" to a random file
		randomfile = os.path.join(os.getcwd(), str(uuid.uuid4()))
		callback = "#!/bin/sh\necho -n 'hello world' > %s" % randomfile
			
		script = open(self.cbfile, 'w+')
		script.write(callback)
		script.close()
		
		os.chmod(self.cbfile, 0)

		# Make sure it's doesn't run if not an executable		
		retcode = self.run_osod(input, self.dbfile, instance="MyInstance",
		  						callback=self.cbfile)

		self.assertNotEqual(retcode, 0)

		os.chmod(self.cbfile, stat.S_IRUSR | stat.S_IXUSR | stat.S_IWUSR)

		retcode = self.run_osod(input, self.dbfile, instance="MyInstance",
		  						callback=self.cbfile)
		
		# Osod should have completed without errors	
		self.assertEqual(retcode, 0)

		# Callback should have created the tempfile
		self.assertTrue(os.path.exists(randomfile))

		p = open(randomfile, 'r')
		self.assertEqual(p.read(), "hello world")
		p.close()
		
		os.unlink(randomfile)
		
	def testNonexistingCallback(self):
		input = "1 2 3 4 5 6 7"
		retcode = self.run_osod(input, self.dbfile, 
								callback="/This/callback/does/not/exist")

		self.assertNotEqual(retcode, 0)
				
if __name__ == '__main__':
	unittest.main()

