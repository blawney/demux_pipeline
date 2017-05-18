import logging
import sys
import os
import subprocess
import unittest
import mock
import time
this_dir = os.path.dirname( os.path.abspath(__file__) )
sys.path.append( os.path.dirname(this_dir ) )

import datetime

import cloud_retention_scanner
import utils

class LiveTestNotifications(unittest.TestCase):

	def setUp(self):

		# a basic dictionary to hold the config parameters that don't change
		# recall that we're mocking out the configuration file parsing process
		self.static_params = utils.parse_config_file('TRACKING')

		self.test_input_files = []

		# make a dummy bucket
		self.test_bucket = 'cccb-seq-tmp-testing'
		cmd = 'gsutil mb gs://%s' % self.test_bucket 
		subprocess.Popen(cmd, shell=True)
	
		# need to sleep so the copy can work.
		time.sleep(5)
	
		# a dummy file to delete -- just copy this file
		cmd = 'gsutil cp %s gs://%s/' % (os.path.abspath(__file__), self.test_bucket)
		subprocess.Popen(cmd, shell=True)

		# a 'good' existing db where the expiration date is in the future, but falls on one of our reminder days
		# a second entry does NOT fall on any of the reminder days
		f1 = os.path.join(this_dir, 'test_db_file.db')
		self.test_input_files.append(f1)
		today = datetime.datetime.now()

		# set a target date that will trigger a notification
		target_expiration = today + datetime.timedelta(days=int(self.static_params['reminder_intervals'][1]))

		# set another target date that will NOT trigger a notification		
		target_expiration_2 = today + datetime.timedelta(days=int(self.static_params['reminder_intervals'][1]) + 1)

		target_expiration_string = target_expiration.strftime(self.static_params['date_format'])
		target_expiration_string_2 = target_expiration_2.strftime(self.static_params['date_format'])

		with open(f1, 'w') as fout:
			fout.write('\t'.join(['abc-12345-678', self.test_bucket, 'blawney@jimmy.harvard.edu', target_expiration_string]))
			fout.write('\n')
			fout.write('\t'.join(['def', 'bucket-def', 'ghi@gmail.com', target_expiration_string_2]))

		f2 = os.path.join(this_dir, 'test_db_file2.db')
		self.test_input_files.append(f2)

		# set a target date that will trigger a notification to CCCB
		target_expiration = today

		# set another target date that will NOT trigger a notification		
		target_expiration_2 = today + datetime.timedelta(days=int(self.static_params['reminder_intervals'][1]) + 1)

		target_expiration_string = target_expiration.strftime(self.static_params['date_format'])
		target_expiration_string_2 = target_expiration_2.strftime(self.static_params['date_format'])

		with open(f2, 'w') as fout:
			fout.write('\t'.join(['abc-12345-678', self.test_bucket, 'brianlawney@gmail.com', target_expiration_string]))
			fout.write('\n')
			fout.write('\t'.join(['def', 'bucket-def', 'foobar@dfci.harvard.edu', target_expiration_string_2]))


	def tearDown(self):
		cmd = 'gsutil rm -f gs://%s/*' % self.test_bucket
		subprocess.Popen(cmd, shell=True)
		time.sleep(5)
		cmd = 'gsutil rb gs://%s' % self.test_bucket
		time.sleep(5)
		subprocess.Popen(cmd, shell=True)
		for f in self.test_input_files:
			os.remove(f)

	@mock.patch('cloud_retention_scanner.utils.parse_config_file')
	def test_send_client_notification(self, mock_parser):
		params = self.static_params.copy()
		params['data_retention_db'] = os.path.join(this_dir, 'test_db_file.db')
		params['backup_db_list'] = [os.path.join(this_dir, 'test_db_file.db'),] # same
		mock_parser.return_value = params
		cloud_retention_scanner.main()

	@mock.patch('cloud_retention_scanner.utils.parse_config_file')
	def test_send_internal_notification(self, mock_parser):
		params = self.static_params.copy()
		params['data_retention_db'] = os.path.join(this_dir, 'test_db_file2.db')
		params['backup_db_list'] = [os.path.join(this_dir, 'test_db_file2.db'),] # same
		mock_parser.return_value = params
		cloud_retention_scanner.main()
