import logging
logging.disable(logging.CRITICAL)

import unittest
import mock
import __builtin__
from StringIO import StringIO

# for finding modules in the parent directory
import sys
from os import path
sys.path.append( path.dirname( path.dirname( path.abspath(__file__) ) ) )


from process_sequencing_run import *


class SampleSheetTests(unittest.TestCase):

	def test_samplesheet_prefix(self):
		# this line is missing the 'Sample_' prefix in the first field
		bad_line = 'S,S,,,A001,ATCACGA,Project_X,'
		self.assertFalse(line_is_valid(bad_line, 'Sample_'))

	def test_samplesheet_matching_sample_names(self):
		# this line has mismatching sample names between fields 1 and 2 (first is 'XX', second is 'X')
		bad_line = 'Sample_XX,X,,,A001,ATCACGA,Project_X,'
		self.assertFalse(line_is_valid(bad_line, 'Sample_'))

	def test_samplesheet_has_proper_length_index(self):
		# this line has only a 6bp index
		bad_line = 'Sample_XX,XX,,,A001,ATCACG,Project_X,'
		self.assertFalse(line_is_valid(bad_line, 'Sample_'))

	def test_samplesheet_has_project_id(self):
		# this line is missing a client/project ID
		bad_line = 'Sample_XX,XX,,,A001,ATCACGA,,'
		self.assertFalse(line_is_valid(bad_line, 'Sample_'))

	def test_samplesheet_valid_line(self):
		# this line is formatted correctly
		good_line = 'Sample_XX,XX,,,A001,ATCACGA,Project_X,'
		self.assertTrue(line_is_valid(good_line, 'Sample_'))		

	def test_samplesheet_illegal_characters(self):
		# this line has characters that are not letters, numbers, or underscore
		bad_line = 'Sample_X-X,X-X,,,A001,ATCACGA,Project_X,'
		self.assertFalse(line_is_valid(bad_line, 'Sample_'))		

	@mock.patch('process_sequencing_run.os')
	def test_samplesheet_with_missing_section(self, mock_os):
		mock_os.path.isfile.return_value = True
		dummy_text = '[section]\nabcd\n[another_section]\nefgh' # does not contain the '[Data]' section
		with mock.patch('__builtin__.open', mock.mock_open(read_data = dummy_text)) as mo: 
			with self.assertRaises(SystemExit):
				check_samplesheet('dummy1', 'dummy2')

	@mock.patch('process_sequencing_run.os')
	def test_samplesheet_returns_project_list_correctly(self, mock_os):
		mock_os.path.isfile.return_value = True
		dummy_text = '[section]\nabcd\n[Data]\n'
		dummy_text += 'Sample_ID,Sample_Name,Sample_Plate,Sample_Well,I7_Index_ID,index,Sample_Project,Description\n'
		dummy_text += 'Sample_XX,XX,,,A001,ATCACGA,Project_X,\n'
		dummy_text += 'Sample_YY,YY,,,A001,ATCACGA,Project_Y,\n'
		dummy_text += 'Sample_ZZ,ZZ,,,A001,ATCACGA,Project_Z,\n'
		with mock.patch('__builtin__.open', mock.mock_open(read_data = dummy_text)) as mo: 
			self.assertEqual(check_samplesheet('dummy1', 'Sample_'), ['Project_X','Project_Y','Project_Z'])


class ConfigParserTest(unittest.TestCase):
	
	@mock.patch('process_sequencing_run.os')
	def test_missing_config_file(self, mock_os):
		mock_os.path.dirname.return_value = '/path/to/dummy_dir'
		mock_os.listdir.return_value = []
		with self.assertRaises(SystemExit):
			parse_config_file()

	@mock.patch('process_sequencing_run.os')
	def test_multiple_config_files(self, mock_os):
		mock_os.path.dirname.return_value = '/path/to/dummy_dir'
		mock_os.listdir.return_value = ['a.cfg','b.cfg']
		with self.assertRaises(SystemExit):
			parse_config_file()
		


class TestOutputDirCreation(unittest.TestCase):

	@mock.patch('process_sequencing_run.os')
	def test_bad_input_arg(self, mock_os):
		# tests the case where the input argument for the run directory was not actually typed correctly
		mock_os.path.isdir.return_value = False
		with self.assertRaises(SystemExit):
			create_output_directory('dummy1', 'dummy2')	


	def my_join(a,b):
		return os.path.join(a,b)

	@mock.patch('process_sequencing_run.os.path.join', side_effect = my_join)
	@mock.patch('process_sequencing_run.os')
	def test_mkdir_is_called(self, mock_os, mock_join):
		mock_os.path.isdir.return_value = True
		run_dir_path = '/path/to/rundir'
		bcl2fastq2_out = 'bcl2fastq2_output'
		create_output_directory(run_dir_path, bcl2fastq2_out)			
		mock_os.mkdir.assert_called_once_with('/path/to/rundir/bcl2fastq2_output')
		mock_os.chmod.assert_called_once_with('/path/to/rundir/bcl2fastq2_output', 0774)



class TestConcatenationCall(unittest.TestCase):
	"""
	concatenate_fastq_files(output_directory_path, project_id_list, sample_dir_prefix)
	os.path.join
	os.listdir(project_dir)
	os.path.isdir(project_dir)
	os.path.basename
	glob.glob
						subprocess.check_call( call, shell = True )
	"""

	def my_join(a,b):
		return os.path.join(a,b)

	def my_basename(p):
		return os.path.basename(p)


	@mock.patch('process_sequencing_run.subprocess.check_call')
	@mock.patch('process_sequencing_run.os.path.join', side_effect = my_join)
	@mock.patch('process_sequencing_run.os.path.basename', side_effect = my_basename)
	@mock.patch('process_sequencing_run.os')
	@mock.patch('process_sequencing_run.glob')
	def test_makes_correct_cat_call_for_paired_end_protocol(self, mock_glob, mock_os, mock_basename, mock_join, mock_call):
		mock_os.path.isdir.return_value = True			
		mock_os.listdir.return_value = ['Sample_XX_1', 'Sample_YY_2']
		mock_glob.glob.side_effect = [['XX-1_S1_L001_R1_001.fastq.gz', 'XX-1_S1_L003_R1_001.fastq.gz', 'XX-1_S1_L002_R1_001.fastq.gz'],
						['XX-1_S1_L003_R2_001.fastq.gz', 'XX-1_S1_L001_R2_001.fastq.gz', 'XX-1_S1_L002_R2_001.fastq.gz'],
						['YY-2_S1_L001_R1_001.fastq.gz', 'YY-2_S1_L003_R1_001.fastq.gz', 'YY-2_S1_L002_R1_001.fastq.gz'],
						['YY-2_S1_L003_R2_001.fastq.gz', 'YY-2_S1_L001_R2_001.fastq.gz', 'YY-2_S1_L002_R2_001.fastq.gz']]
		concatenate_fastq_files('/path/to/something', ['Project_ABC'], 'Sample_')
		expected_call_A = 'cat XX-1_S1_L001_R1_001.fastq.gz XX-1_S1_L002_R1_001.fastq.gz XX-1_S1_L003_R1_001.fastq.gz >XX_1_R1_001.fastq.gz'
		expected_call_B = 'cat XX-1_S1_L001_R2_001.fastq.gz XX-1_S1_L002_R2_001.fastq.gz XX-1_S1_L003_R2_001.fastq.gz >XX_1_R2_001.fastq.gz'
		expected_call_C = 'cat YY-2_S1_L001_R1_001.fastq.gz YY-2_S1_L002_R1_001.fastq.gz YY-2_S1_L003_R1_001.fastq.gz >YY_2_R1_001.fastq.gz'
		expected_call_D = 'cat YY-2_S1_L001_R2_001.fastq.gz YY-2_S1_L002_R2_001.fastq.gz YY-2_S1_L003_R2_001.fastq.gz >YY_2_R2_001.fastq.gz'
		calls = [mock.call(expected_call_A, shell=True),
			mock.call(expected_call_B, shell=True),
			mock.call(expected_call_C, shell=True),
			mock.call(expected_call_D, shell=True)]
		mock_call.assert_has_calls(calls)


	@mock.patch('process_sequencing_run.subprocess.check_call')
	@mock.patch('process_sequencing_run.os.path.join', side_effect = my_join)
	@mock.patch('process_sequencing_run.os.path.basename', side_effect = my_basename)
	@mock.patch('process_sequencing_run.os')
	@mock.patch('process_sequencing_run.glob')
	def test_makes_correct_cat_call_for_single_end_protocol(self, mock_glob, mock_os, mock_basename, mock_join, mock_call):
		mock_os.path.isdir.return_value = True			
		mock_os.listdir.return_value = ['Sample_XX_1', 'Sample_YY_2']
		mock_glob.glob.side_effect = [['XX-1_S1_L001_R1_001.fastq.gz', 'XX-1_S1_L003_R1_001.fastq.gz', 'XX-1_S1_L002_R1_001.fastq.gz'],
						[],
						['YY-2_S1_L001_R1_001.fastq.gz', 'YY-2_S1_L003_R1_001.fastq.gz', 'YY-2_S1_L002_R1_001.fastq.gz'],
						[]]
		concatenate_fastq_files('/path/to/something', ['Project_ABC'], 'Sample_')
		expected_call_A = 'cat XX-1_S1_L001_R1_001.fastq.gz XX-1_S1_L002_R1_001.fastq.gz XX-1_S1_L003_R1_001.fastq.gz >XX_1_R1_001.fastq.gz'
		expected_call_B = 'cat YY-2_S1_L001_R1_001.fastq.gz YY-2_S1_L002_R1_001.fastq.gz YY-2_S1_L003_R1_001.fastq.gz >YY_2_R1_001.fastq.gz'
		calls = [mock.call(expected_call_A, shell=True),
			mock.call(expected_call_B, shell=True)]
		mock_call.assert_has_calls(calls)


	@mock.patch('process_sequencing_run.subprocess.check_call')
	@mock.patch('process_sequencing_run.os.path.join', side_effect = my_join)
	@mock.patch('process_sequencing_run.os.path.basename', side_effect = my_basename)
	@mock.patch('process_sequencing_run.os')
	@mock.patch('process_sequencing_run.glob')
	def test_issue_error_if_only_read2_files(self, mock_glob, mock_os, mock_basename, mock_join, mock_call):
		mock_os.path.isdir.return_value = True			
		mock_os.listdir.return_value = ['Sample_XX_1', 'Sample_YY_2']
		mock_glob.glob.side_effect = [[],
						['XX-1_S1_L003_R2_001.fastq.gz', 'XX-1_S1_L001_R2_001.fastq.gz', 'XX-1_S1_L002_R2_001.fastq.gz'],
						[],
						['YY-2_S1_L003_R2_001.fastq.gz', 'YY-2_S1_L001_R2_001.fastq.gz', 'YY-2_S1_L002_R2_001.fastq.gz']]
		with self.assertRaises(SystemExit):
			concatenate_fastq_files('/path/to/something', ['Project_ABC'], 'Sample_')



	@mock.patch('process_sequencing_run.subprocess.check_call')
	@mock.patch('process_sequencing_run.os.path.join', side_effect = my_join)
	@mock.patch('process_sequencing_run.os.path.basename', side_effect = my_basename)
	@mock.patch('process_sequencing_run.os')
	@mock.patch('process_sequencing_run.glob')
	def test_issue_error_if_differing_number_of_files_in_paired_end_protocol(self, mock_glob, mock_os, mock_basename, mock_join, mock_call):
		mock_os.path.isdir.return_value = True			
		mock_os.listdir.return_value = ['Sample_XX_1', 'Sample_YY_2']
		# note that we have only 2 entries for one of the mocked calls to glob 
		mock_glob.glob.side_effect = [['XX-1_S1_L001_R1_001.fastq.gz', 'XX-1_S1_L003_R1_001.fastq.gz', 'XX-1_S1_L002_R1_001.fastq.gz'],
						['XX-1_S1_L003_R2_001.fastq.gz', 'XX-1_S1_L001_R2_001.fastq.gz'],
						['YY-2_S1_L001_R1_001.fastq.gz', 'YY-2_S1_L003_R1_001.fastq.gz', 'YY-2_S1_L002_R1_001.fastq.gz'],
						['YY-2_S1_L003_R2_001.fastq.gz', 'YY-2_S1_L001_R2_001.fastq.gz', 'YY-2_S1_L002_R2_001.fastq.gz']]
		with self.assertRaises(SystemExit):
			concatenate_fastq_files('/path/to/something', ['Project_ABC'], 'Sample_')



	@mock.patch('process_sequencing_run.subprocess.check_call')
	@mock.patch('process_sequencing_run.os.path.join', side_effect = my_join)
	@mock.patch('process_sequencing_run.os.path.basename', side_effect = my_basename)
	@mock.patch('process_sequencing_run.os')
	@mock.patch('process_sequencing_run.glob')
	def test_issue_error_if_no_fastq_found(self, mock_glob, mock_os, mock_basename, mock_join, mock_call):
		mock_os.path.isdir.return_value = True			
		mock_os.listdir.return_value = ['Sample_XX_1', 'Sample_YY_2']
		mock_glob.glob.side_effect = [[],[],
						['YY-2_S1_L001_R1_001.fastq.gz', 'YY-2_S1_L003_R1_001.fastq.gz', 'YY-2_S1_L002_R1_001.fastq.gz'],
						['YY-2_S1_L003_R2_001.fastq.gz', 'YY-2_S1_L001_R2_001.fastq.gz', 'YY-2_S1_L002_R2_001.fastq.gz']]
		with self.assertRaises(SystemExit):
			concatenate_fastq_files('/path/to/something', ['Project_ABC'], 'Sample_')



class TestCreatingFinalLocations(unittest.TestCase):

	def my_join(*args):
		return reduce(lambda x,y: os.path.join(x,y), args)

	@mock.patch('process_sequencing_run.os.path.join', side_effect = my_join)	
	@mock.patch('process_sequencing_run.os')	
	def test_makes_into_new_directory(self, mock_os, mock_join):
		'''
		This tests that the project directory does not already have subdirectories for this year and/or month.
		Create that time-stamped directory and place the project directories in there.
		'''
		from datetime import datetime as date
		today = date.today()
		year = today.year
		month = today.month

		mock_os.path.isdir.return_value = True # the 'home' directory is there
		mock_project_dir_path = '/path/to/final_projects_dir/'
		mock_target = os.path.join(mock_project_dir_path, str(year), str(month)) # the full path to the time-stamped dir
		final_locations = create_final_locations(mock_project_dir_path, ['Project_ABC'])
		mock_os.makedirs.assert_called_with(mock_target)
		mock_os.mkdir.assert_called_with(os.path.join(mock_target, 'Project_ABC'))
		self.assertEqual(final_locations, [os.path.join(mock_target, 'Project_ABC')])


        @mock.patch('process_sequencing_run.os.path.join', side_effect = my_join)
        @mock.patch('process_sequencing_run.os')
        def test_destination_directory_exists_already(self, mock_os, mock_join):
                from datetime import datetime as date
                today = date.today()
                year = today.year
                month = today.month

                mock_os.path.isdir.return_value = True
                mock_project_dir_path = '/path/to/final_projects_dir/'
                mock_target = os.path.join(mock_project_dir_path, str(year), str(month))
                mock_os.makedirs.side_effect = OSError(17, 'foo')
                final_locations = create_final_locations(mock_project_dir_path, ['Project_ABC'])
                mock_os.makedirs.assert_called_with(mock_target)
                mock_os.mkdir.assert_called_with(os.path.join(mock_target, 'Project_ABC'))
                self.assertEqual(final_locations, [os.path.join(mock_target, 'Project_ABC')])


        @mock.patch('process_sequencing_run.os.path.join', side_effect = my_join)
        @mock.patch('process_sequencing_run.os')
        def test_cannot_create_project_dir_raises_error(self, mock_os, mock_join):
                from datetime import datetime as date
                today = date.today()
                year = today.year
                month = today.month

                mock_os.path.isdir.return_value = True
                mock_project_dir_path = '/path/to/final_projects_dir/'
                mock_target = os.path.join(mock_project_dir_path, str(year), str(month))
                mock_os.makedirs.side_effect = OSError(17, 'foo')
                mock_os.mkdir.side_effect = OSError(2, 'foo')
		with self.assertRaises(SystemExit):
	                final_locations = create_final_locations(mock_project_dir_path, ['Project_ABC'])



        @mock.patch('process_sequencing_run.os.path.join', side_effect = my_join)
        @mock.patch('process_sequencing_run.os')
        def test_moving_multiple_projects(self, mock_os, mock_join):
                from datetime import datetime as date
                today = date.today()
                year = today.year
                month = today.month

                mock_os.path.isdir.return_value = True
                mock_project_dir_path = '/path/to/final_projects_dir/'
                mock_target = os.path.join(mock_project_dir_path, str(year), str(month))
                mock_os.makedirs.side_effect = OSError(17, 'foo') # the time-stamped dir already exists
                final_locations = create_final_locations(mock_project_dir_path, ['Project_ABC', 'Project_DEF'])
                mock_os.makedirs.assert_called_with(mock_target)
		mock_calls = [
			mock.call(os.path.join(mock_target, 'Project_ABC')),
			mock.call(os.path.join(mock_target, 'Project_DEF'))
		]
                mock_os.mkdir.assert_has_calls(mock_calls)
                self.assertEqual(final_locations, [os.path.join(mock_target, 'Project_ABC'), os.path.join(mock_target, 'Project_DEF')])




        @mock.patch('process_sequencing_run.os.path.join', side_effect = my_join)
        @mock.patch('process_sequencing_run.os')
        def test_making_destination_directory_raises_unexpected_exception(self, mock_os, mock_join):
                from datetime import datetime as date
                today = date.today()
                year = today.year
                month = today.month

                mock_os.path.isdir.return_value = True
                mock_project_dir_path = '/path/to/projects_dir/'
                mock_os.makedirs.side_effect = OSError(2, 'foo')
		with self.assertRaises(SystemExit):
	                create_final_locations(mock_project_dir_path, ['Project_ABC'])



        @mock.patch('process_sequencing_run.os.path.join', side_effect = my_join)
        @mock.patch('process_sequencing_run.os')
        def test_destination_directory_gets_lost(self, mock_os, mock_join):
		'''
		This covers the case where the directory allegedly existed (so OSError 17 was raised),
		but the os.path.isdir returns 'False' for that directory when we attempt to move the files.
		May not be a real possibility, but it is extra-defensive
		'''
                from datetime import datetime as date
                today = date.today()
                year = today.year
                month = today.month

                mock_os.path.isdir.side_effect = [True, False]
                mock_project_dir_path = '/path/to/projects_dir/'
                mock_target = os.path.join(mock_project_dir_path, str(year), str(month))
                mock_os.makedirs.side_effect = OSError(17, 'foo')
                with self.assertRaises(SystemExit):
                       create_final_locations(mock_project_dir_path, ['Project_ABC'])



if __name__ == '__main__':
	unittest.main()
