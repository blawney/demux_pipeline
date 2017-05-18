import logging
import sys
import os
import unittest
import mock
this_dir = os.path.dirname( os.path.abspath(__file__) )
sys.path.append( os.path.dirname(this_dir ) )

import datetime

import cloud_retention_scanner
import utils

class TestDBScanProcess(unittest.TestCase):

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

		# a 'good' existing db
		f1 = os.path.join(this_dir, 'test_db_file.db')
		self.test_input_files.append(f1)
		with open(f1, 'w') as fout:
			fout.write('\t'.join(['abc', 'bucket-abc', 'abc@gmail.com,def@gmail.com', '03/13/2017']))

		# a db with a bad date
		f2 = os.path.join(this_dir, 'test_db_file2.db')
		self.test_input_files.append(f2)
		with open(f2, 'w') as fout:
			fout.write('\t'.join(['abc', 'bucket-abc', 'abc@gmail.com,def@gmail.com', '13/13/2017']))

		# another 'good' existing db
		f3 = os.path.join(this_dir, 'test_db_file3.db')
		self.test_input_files.append(f3)
		with open(f3, 'w') as fout:
			fout.write('\t'.join(['abc', 'bucket-abc', 'abc@gmail.com,def@gmail.com', '03/13/2017']))
			fout.write('\n')
			fout.write('\t'.join(['def', 'bucket-def', 'ghi@gmail.com', '03/15/2017']))

		# a 'good' existing db where the expiration date is in the future, but falls on one of our reminder days
		# a second entry does NOT fall on any of the reminder days
		f4 = os.path.join(this_dir, 'test_db_file4.db')
		self.test_input_files.append(f4)
		today = datetime.datetime.now()
		target_expiration = today + datetime.timedelta(days=int(self.static_params['reminder_intervals'][1]))
		target_expiration_2 = today + datetime.timedelta(days=int(self.static_params['reminder_intervals'][1]) + 1)
		target_expiration_string = target_expiration.strftime(self.static_params['date_format'])
		target_expiration_string_2 = target_expiration_2.strftime(self.static_params['date_format'])
		with open(f4, 'w') as fout:
			fout.write('\t'.join(['abc', 'bucket-abc', 'abc@gmail.com,def@gmail.com', target_expiration_string]))
			fout.write('\n')
			fout.write('\t'.join(['def', 'bucket-def', 'ghi@gmail.com', target_expiration_string_2]))


		# a 'good' existing db where the expiration date is in the future, but falls on one of our reminder days
		# here there are two projects which expire
		f5 = os.path.join(this_dir, 'test_db_file5.db')
		self.test_input_files.append(f5)
		today = datetime.datetime.now()
		target_expiration = today + datetime.timedelta(days=int(self.static_params['reminder_intervals'][1]))
		target_expiration_string = target_expiration.strftime(self.static_params['date_format'])
		with open(f5, 'w') as fout:
			fout.write('\t'.join(['abc', 'bucket-abc', 'abc@gmail.com,def@gmail.com', target_expiration_string]))
			fout.write('\n')
			fout.write('\t'.join(['def', 'bucket-def', 'ghi@gmail.com', target_expiration_string]))

		# a 'good' existing db where the expiration date is in the future, but falls on one of our reminder days
		# a second entry falls on a different reminder day
		f6 = os.path.join(this_dir, 'test_db_file6.db')
		self.test_input_files.append(f6)
		today = datetime.datetime.now()
		target_expiration = today + datetime.timedelta(days=int(self.static_params['reminder_intervals'][1]))
		target_expiration_2 = today + datetime.timedelta(days=int(self.static_params['reminder_intervals'][0]))
		target_expiration_string = target_expiration.strftime(self.static_params['date_format'])
		target_expiration_string_2 = target_expiration_2.strftime(self.static_params['date_format'])
		with open(f6, 'w') as fout:
			fout.write('\t'.join(['abc', 'bucket-abc', 'abc@gmail.com,def@gmail.com', target_expiration_string]))
			fout.write('\n')
			fout.write('\t'.join(['def', 'bucket-def', 'ghi@gmail.com', target_expiration_string_2]))


		# a 'good' existing db where the expiration date is in the future, but falls on one of our reminder days
		# a second entry falls on a different reminder day
		f7 = os.path.join(this_dir, 'test_db_file7.db')
		self.test_input_files.append(f7)
		today = datetime.datetime.now()
		target_expiration = today + datetime.timedelta(days=int(self.static_params['reminder_intervals'][1]))
		target_expiration_2 = today
		target_expiration_string = target_expiration.strftime(self.static_params['date_format'])
		target_expiration_string_2 = target_expiration_2.strftime(self.static_params['date_format'])
		with open(f7, 'w') as fout:
			fout.write('\t'.join(['abc', 'bucket-abc', 'abc@gmail.com,def@gmail.com', target_expiration_string]))
			fout.write('\n')
			fout.write('\t'.join(['def', 'bucket-def', 'ghi@gmail.com', target_expiration_string_2]))

		# a 'good' existing db where the expiration date is in the future, but falls on one of our reminder days
		# a second entry falls on a different reminder day
		f8 = os.path.join(this_dir, 'test_db_file8.db')
		self.test_input_files.append(f8)
		today = datetime.datetime.now()
		target_expiration = today
		target_expiration_2 = today + datetime.timedelta(days=int(self.static_params['reminder_intervals'][0])+1)
		target_expiration_string = target_expiration.strftime(self.static_params['date_format'])
		target_expiration_string_2 = target_expiration_2.strftime(self.static_params['date_format'])
		with open(f8, 'w') as fout:
			fout.write('\t'.join(['abc', 'bucket-abc', 'abc@gmail.com,def@gmail.com', target_expiration_string]))
			fout.write('\n')
			fout.write('\t'.join(['def', 'bucket-def', 'ghi@gmail.com', target_expiration_string_2]))

	def tearDown(self):
		for f in self.test_input_files:
			os.remove(f)

	@mock.patch('cloud_retention_scanner.utils.parse_config_file')
	def test_noninteger_reminder_intervals(self, mock_parser):
		self.static_params['reminder_intervals'] = ['14','7a','3']
		params = self.static_params.copy()
		params['data_retention_db'] = os.path.join(this_dir, 'test_db_file.db')
		params['backup_db_list'] = [os.path.join(this_dir, 'test_db_file.db'),] # make the same as the primary for testing
		mock_parser.return_value = params
		with self.assertRaises(cloud_retention_scanner.InvalidReminderCheckpointsException):
			cloud_retention_scanner.main()

	@mock.patch('cloud_retention_scanner.utils.parse_config_file')
	def test_bad_target_date_raises_exception(self, mock_parser):
		params = self.static_params.copy()
		params['data_retention_db'] = os.path.join(this_dir, 'test_db_file2.db')
		params['backup_db_list'] = [os.path.join(this_dir, 'test_db_file2.db'),] # make the same as the primary for testing
		mock_parser.return_value = params
		with self.assertRaises(cloud_retention_scanner.utils.MalformattedDateException):
			cloud_retention_scanner.main()

	@mock.patch('cloud_retention_scanner.utils.parse_config_file')
	@mock.patch('cloud_retention_scanner.send_reminder_email')
	def test_calls_reminder_email_function(self, mock_reminder_func, mock_parser):
		params = self.static_params.copy()
		params['data_retention_db'] = os.path.join(this_dir, 'test_db_file4.db')
		params['backup_db_list'] = [os.path.join(this_dir, 'test_db_file4.db'),] # the same
		mock_parser.return_value = params
		cloud_retention_scanner.main()

		# read the file contents 
		db = utils.load_database(params['data_retention_db'], params)
		mock_reminder_func.assert_called_once_with('abc', db['abc'], params, 7)

	@mock.patch('cloud_retention_scanner.utils.parse_config_file')
	@mock.patch('cloud_retention_scanner.send_reminder_email')
	def test_calls_reminder_email_function_for_multiple_on_same_day(self, mock_reminder_func, mock_parser):
		params = self.static_params.copy()
		params['data_retention_db'] = os.path.join(this_dir, 'test_db_file5.db')
		params['backup_db_list'] = [os.path.join(this_dir, 'test_db_file5.db'),] # the same
		mock_parser.return_value = params
		cloud_retention_scanner.main()

		# read the file contents 
		db = utils.load_database(params['data_retention_db'], params)
		calls = [mock.call('abc', db['abc'], params,7), mock.call('def', db['def'], params,7)]
		mock_reminder_func.assert_has_calls(calls)


	@mock.patch('cloud_retention_scanner.utils.parse_config_file')
	@mock.patch('cloud_retention_scanner.send_reminder_email')
	def test_calls_reminder_email_function_for_multiple_on_different_days(self, mock_reminder_func, mock_parser):
		params = self.static_params.copy()
		params['data_retention_db'] = os.path.join(this_dir, 'test_db_file6.db')
		params['backup_db_list'] = [os.path.join(this_dir, 'test_db_file6.db'),] # the same
		mock_parser.return_value = params
		cloud_retention_scanner.main()

		# read the file contents 
		db = utils.load_database(params['data_retention_db'], params)
		calls = [mock.call('abc', db['abc'], params, 7), mock.call('def', db['def'], params, 14)]
		mock_reminder_func.assert_has_calls(calls)


	@mock.patch('cloud_retention_scanner.utils.parse_config_file')
	@mock.patch('cloud_retention_scanner.send_reminder_email')
	@mock.patch('cloud_retention_scanner.mark_for_deletion')
	def test_calls_removal_function_and_reminder_function(self, mock_deletion_func, mock_reminder_func, mock_parser):
		params = self.static_params.copy()
		params['data_retention_db'] = os.path.join(this_dir, 'test_db_file7.db')
		params['backup_db_list'] = [os.path.join(this_dir, 'test_db_file7.db'),] # the same
		mock_parser.return_value = params
		cloud_retention_scanner.main()

		# read the file contents 
		db = utils.load_database(params['data_retention_db'], params)
		mock_reminder_func.assert_called_once_with('abc', db['abc'], params, 7)
		mock_deletion_func.assert_called_once_with('def', db['def'], params)


	@mock.patch('cloud_retention_scanner.utils.parse_config_file')
	@mock.patch('cloud_retention_scanner.send_reminder_email')
	@mock.patch('cloud_retention_scanner.mark_for_deletion')
	def test_calls_removal_function(self, mock_deletion_func, mock_reminder_func, mock_parser):
		params = self.static_params.copy()
		params['data_retention_db'] = os.path.join(this_dir, 'test_db_file8.db')
		params['backup_db_list'] = [os.path.join(this_dir, 'test_db_file8.db'),] # the same
		mock_parser.return_value = params
		cloud_retention_scanner.main()
		# read the file contents 
		db = utils.load_database(params['data_retention_db'], params)
		mock_deletion_func.assert_called_once_with('abc', db['abc'], params)


	@mock.patch('cloud_retention_scanner.utils.parse_config_file')
	def test_inconsistent_databases_raises_exception(self, mock_parser):
		params = self.static_params.copy()
		params['data_retention_db'] = os.path.join(this_dir, 'test_db_file.db')
		params['backup_db_list'] = [os.path.join(this_dir, 'test_db_file3.db'),] # this db is different, which should raise an exception
		mock_parser.return_value = params
		with self.assertRaises(cloud_retention_scanner.DatabaseInconsistencyException):
			cloud_retention_scanner.main()

	@mock.patch('cloud_retention_scanner.utils.send_notifications')
	@mock.patch('cloud_retention_scanner.utils.parse_config_file')
	def test_fills_out_reminder_email_template(self, mock_parser, mock_email_sender):
		params = self.static_params.copy()
		params['data_retention_db'] = os.path.join(this_dir, 'test_db_file4.db')
		params['backup_db_list'] = [os.path.join(this_dir, 'test_db_file4.db'),] # same
		mock_parser.return_value = params
		cloud_retention_scanner.calculate_size = mock.MagicMock(return_value=30.1)
		cloud_retention_scanner.main()
		cloud_retention_scanner.calculate_size.assert_called_once_with('bucket-abc', scale=1e9)
		self.assertTrue(mock_email_sender.called)

	@mock.patch('cloud_retention_scanner.utils.send_notifications')
	@mock.patch('cloud_retention_scanner.utils.parse_config_file')
	def test_fills_out_cccb_notification_template(self, mock_parser, mock_email_sender):
		params = self.static_params.copy()
		params['data_retention_db'] = os.path.join(this_dir, 'test_db_file8.db')
		params['backup_db_list'] = [os.path.join(this_dir, 'test_db_file8.db'),] # same
		params['data_cleanup_command_log'] = os.path.join(this_dir, 'test_delete.sh')
		mock_parser.return_value = params
		cloud_retention_scanner.main()

		file_contents = open(params['data_cleanup_command_log']).read()
		lines = file_contents.split('\n')
		self.assertEqual(lines[1], 'gsutil rm -r gs://bucket-abc/*')	
		self.assertEqual(lines[2], 'gsutil rb gs://bucket-abc')
		self.assertTrue(mock_email_sender.called)
		

