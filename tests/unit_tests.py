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


if __name__ == '__main__':
	unittest.main()
