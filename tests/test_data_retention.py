import logging
import sys
import os
import unittest
import mock
this_dir = os.path.dirname( os.path.abspath(__file__) )
sys.path.append( os.path.dirname(this_dir ) )

import datetime

import cloud_delivery_tracking
import utils

class TestSuite(unittest.TestCase):

	def setUp(self):

		# a basic dictionary to hold the config parameters that don't change
		# recall that we're mocking out the configuration file parsing process
		"""
		self.static_params = {}
		self.static_params['retention_period'] = '30'
		self.static_params['reminder_intervals'] = ['14','7','3']
		self.static_params['date_format'] = '%m/%d/%Y'
		"""
		self.static_params = utils.parse_config_file('TRACKING')

		self.test_input_files = []

		# an empty file
		f2 = os.path.join(this_dir, 'test_db_file2.db')
		self.test_input_files.append(f2)
		with open(f2, 'w') as fout:
			fout.write('\n')

		# malformatted date
		f3 = os.path.join(this_dir, 'test_db_file3.db')
		self.test_input_files.append(f3)
		with open(f3, 'w') as fout:
			fout.write('\t'.join(['abc', 'bucket-abc', 'abc@gmail.com,def@gmail.com', '3/13/17']))
		
		# mismatching mbucket names for same project
		f4 = os.path.join(this_dir, 'test_db_file4.db')
		self.test_input_files.append(f4)
		with open(f4, 'w') as fout:
			fout.write('\t'.join(['abc', 'bucket-abc', 'abc@gmail.com,def@gmail.com', '03/13/2017']))

		# a 'good' existing db
		f5 = os.path.join(this_dir, 'test_db_file5.db')
		self.test_input_files.append(f5)
		with open(f5, 'w') as fout:
			fout.write('\t'.join(['abc', 'bucket-abc', 'abc@gmail.com,def@gmail.com', '03/13/2017']))

		# a 'good' existing db
		f6 = os.path.join(this_dir, 'test_db_file6.db')
		self.test_input_files.append(f6)
		with open(f6, 'w') as fout:
			fout.write('\t'.join(['abc', 'bucket-abc', 'abc@gmail.com,def@gmail.com', '03/13/2017']))

	def tearDown(self):
		for f in self.test_input_files:
			os.remove(f)


	@mock.patch('cloud_delivery_tracking.utils.parse_config_file')
	def test_missing_database_file_raises_exception(self, mock_parser):
		"""
		If file doesn't exist, warn about it, but then write a new database
		"""
		params = self.static_params.copy()
		missing_filename = 'test_missing_db_file.db'
		params['data_retention_db'] = os.path.join(this_dir, missing_filename)
		mock_parser.return_value = params

		project_mapping = {}
		info_dict = {'bucket': 'cccb-seq-dummy-ilab-id', 'client_emails': ['abc@gmail.com','def@dfci.harvard.edu']} 
		project_mapping['dummy-ilab-id'] = info_dict

		with self.assertRaises(cloud_delivery_tracking.MissingPrimaryDatabaseException):
			cloud_delivery_tracking.main(project_mapping)

		
	@mock.patch('cloud_delivery_tracking.utils.parse_config_file')
	def test_empty_db_file(self, mock_parser):
		params = self.static_params.copy()
		params['data_retention_db'] = os.path.join(this_dir, 'test_db_file2.db')
		mock_parser.return_value = params

		project_mapping = {}
		info_dict = {'bucket': 'cccb-seq-dummy-ilab-id', 'client_emails': ['abc@gmail.com','def@dfci.harvard.edu']} 
		project_mapping['dummy-ilab-id'] = info_dict
		cloud_delivery_tracking.main(project_mapping)

		current_date = datetime.datetime.now()
		expected_target_date = current_date + datetime.timedelta(params['retention_period'])
		new_content = '\t'.join(['dummy-ilab-id', 'cccb-seq-dummy-ilab-id', 'abc@gmail.com,def@dfci.harvard.edu', expected_target_date.strftime(params['date_format'])])
		actual_content = open(os.path.join(this_dir, 'test_db_file2.db')).read()
		self.assertEqual(new_content + '\n', actual_content)


	@mock.patch('cloud_delivery_tracking.utils.parse_config_file')
	def test_file_w_malformatted_date(self, mock_parser):
		params = self.static_params.copy()
		params['data_retention_db'] = os.path.join(this_dir, 'test_db_file3.db')
		mock_parser.return_value = params
		project_mapping = {}
		info_dict = {'bucket': 'cccb-seq-dummy-ilab-id', 'client_emails': ['abc@gmail.com','def@dfci.harvard.edu']} 
		project_mapping['dummy-ilab-id'] = info_dict
		with self.assertRaises(cloud_delivery_tracking.utils.MalformattedDateException):
			cloud_delivery_tracking.main(project_mapping)


	@mock.patch('cloud_delivery_tracking.utils.parse_config_file')
	def test_mismatching_bucket_for_same_project_raises_exception(self, mock_parser):
		params = self.static_params.copy()
		params['data_retention_db'] = os.path.join(this_dir, 'test_db_file4.db')
		mock_parser.return_value = params
		project_mapping = {}
		# the bucket name below intentionally mismatches the one in the db
		info_dict = {'bucket': 'cccb-seq-dummy-ilab-id', 'client_emails': ['abc@gmail.com','def@dfci.harvard.edu']} 
		project_mapping['abc'] = info_dict
		with self.assertRaises(cloud_delivery_tracking.MultipleBucketsForSameProjectException):
			cloud_delivery_tracking.main(project_mapping)


	@mock.patch('cloud_delivery_tracking.utils.parse_config_file')
	def test_adds_new_entry_correctly(self, mock_parser):
		original_file_contents =  open(os.path.join(this_dir, 'test_db_file5.db')).read()
		params = self.static_params.copy()
		params['data_retention_db'] = os.path.join(this_dir, 'test_db_file5.db')
		mock_parser.return_value = params
		project_mapping = {}
		# the bucket name below intentionally mismatches the one in the db
		info_dict = {'bucket': 'cccb-seq-dummy-ilab-id', 'client_emails': ['abc@gmail.com','def@dfci.harvard.edu']} 
		project_mapping['new_project_id'] = info_dict
		cloud_delivery_tracking.main(project_mapping)

		current_date = datetime.datetime.now()
		expected_target_date = current_date + datetime.timedelta(params['retention_period'])
		new_content = '\t'.join(['new_project_id', 'cccb-seq-dummy-ilab-id', 'abc@gmail.com,def@dfci.harvard.edu', expected_target_date.strftime(params['date_format'])])
		actual_content = open(os.path.join(this_dir, 'test_db_file5.db')).read()
		self.assertEqual('\n'.join([original_file_contents, new_content]) + '\n', actual_content)


	@mock.patch('cloud_delivery_tracking.utils.parse_config_file')
	def test_updates_date_for_recurring_project(self, mock_parser):
		params = self.static_params.copy()
		params['data_retention_db'] = os.path.join(this_dir, 'test_db_file6.db')
		mock_parser.return_value = params
		project_mapping = {}
		# the bucket name below intentionally mismatches the one in the db
		info_dict = {'bucket': 'bucket-abc', 'client_emails': ['abc@gmail.com','def@gmail.com']} 
		project_mapping['abc'] = info_dict
		cloud_delivery_tracking.main(project_mapping)

		# get the current date:
		current_date = datetime.datetime.now()
		expected_target_date = current_date + datetime.timedelta(params['retention_period'])
		for line in open(os.path.join(this_dir, 'test_db_file6.db')):
			if line.startswith('abc'):
				contents = line.strip().split('\t')
				self.assertEqual(contents[-1], expected_target_date.strftime('%m/%d/%Y'))
				break


	@mock.patch('cloud_delivery_tracking.utils.parse_config_file')
	def test_add_new_email_existing_project(self, mock_parser):
		params = self.static_params.copy()
		params['data_retention_db'] = os.path.join(this_dir, 'test_db_file6.db')
		mock_parser.return_value = params
		project_mapping = {}
		# the bucket name below intentionally mismatches the one in the db
		info_dict = {'bucket': 'bucket-abc', 'client_emails': ['ghi@gmail.com','jkl@dfci.harvard.edu']} 
		project_mapping['abc'] = info_dict
		cloud_delivery_tracking.main(project_mapping)

		# get the current date:
		current_date = datetime.datetime.now()
		expected_target_date = current_date + datetime.timedelta(params['retention_period'])
		for line in open(os.path.join(this_dir, 'test_db_file6.db')):
			if line.startswith('abc'):
				contents = line.strip().split('\t')
				self.assertEqual(contents[-1], expected_target_date.strftime('%m/%d/%Y'))
				email_set = set(contents[2].split(','))
				expected_email_set = set(['abc@gmail.com', 'def@gmail.com','ghi@gmail.com','jkl@dfci.harvard.edu'])
				self.assertEqual(email_set, expected_email_set)
				break
