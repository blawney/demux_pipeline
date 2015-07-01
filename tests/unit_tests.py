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
		p = pipeline.NextSeqPipeline('/path/to/dummy')
		p.config_params_dict = {'sample_dir_prefix': 'Sample_'}
		self.assertFalse(p.line_is_valid(bad_line))

	def test_samplesheet_matching_sample_names(self):
		# this line has mismatching sample names between fields 1 and 2 (first is 'XX', second is 'X')
		bad_line = 'Sample_XX,X,,,A001,ATCACGA,Project_X,'
		p = pipeline.NextSeqPipeline('/path/to/dummy')
		p.config_params_dict = {'sample_dir_prefix': 'Sample_'}
		self.assertFalse(p.line_is_valid(bad_line))

	def test_samplesheet_has_proper_length_index(self):
		# this line has only a 6bp index
		bad_line = 'Sample_XX,XX,,,A001,ATCACG,Project_X,'
		p = pipeline.NextSeqPipeline('/path/to/dummy')
		p.config_params_dict = {'sample_dir_prefix': 'Sample_'}
		self.assertFalse(p.line_is_valid(bad_line))

	def test_samplesheet_has_project_id(self):
		# this line is missing a client/project ID
		bad_line = 'Sample_XX,XX,,,A001,ATCACGA,,'
		p = pipeline.NextSeqPipeline('/path/to/dummy')
		p.config_params_dict = {'sample_dir_prefix': 'Sample_'}
		self.assertFalse(p.line_is_valid(bad_line))

	def test_samplesheet_valid_line(self):
		# this line is formatted correctly
		good_line = 'Sample_XX,XX,,,A001,ATCACGA,Project_X,'
		p = pipeline.NextSeqPipeline('/path/to/dummy')
		p.config_params_dict = {'sample_dir_prefix': 'Sample_'}
		self.assertTrue(p.line_is_valid(good_line))		

	def test_samplesheet_illegal_characters(self):
		# this line has characters that are not letters, numbers, or underscore
		bad_line = 'Sample_X-X,X-X,,,A001,ATCACGA,Project_X,'
		p = pipeline.NextSeqPipeline('/path/to/dummy')
		p.config_params_dict = {'sample_dir_prefix': 'Sample_'}
		self.assertFalse(p.line_is_valid(bad_line))


	@mock.patch('pipeline.os')
	def test_samplesheet_with_missing_section(self, mock_os):
		mock_os.path.isfile.return_value = True
		dummy_text = '[section]\nabcd\n[another_section]\nefgh' # does not contain the '[Data]' section
		with mock.patch('__builtin__.open', mock.mock_open(read_data = dummy_text)) as mo: 
			with self.assertRaises(SystemExit):
				p = pipeline.NextSeqPipeline('/path/to/dummy')
				p.config_params_dict = {'sample_dir_prefix': 'Sample_'}
				p.check_samplesheet()


	@mock.patch('pipeline.os')
	def test_samplesheet_returns_project_list_correctly(self, mock_os):
		mock_os.path.isfile.return_value = True
		dummy_text = '[section]\nabcd\n[Data]\n'
		dummy_text += 'Sample_ID,Sample_Name,Sample_Plate,Sample_Well,I7_Index_ID,index,Sample_Project,Description\n'
		dummy_text += 'Sample_XX,XX,,,A001,ATCACGA,Project_X,\n'
		dummy_text += 'Sample_YY,YY,,,A001,ATCACGA,Project_Y,\n'
		dummy_text += 'Sample_ZZ,ZZ,,,A001,ATCACGA,Project_Z,\n'
		with mock.patch('__builtin__.open', mock.mock_open(read_data = dummy_text)) as mo:
			p = pipeline.NextSeqPipeline('/path/to/dummy')
			p.config_params_dict = {'sample_dir_prefix': 'Sample_'}
			p.check_samplesheet()
			expected_projects = set(['Project_X','Project_Y','Project_Z'])
			self.assertEqual(set(p.project_id_list), expected_projects)


class ConfigParserTest(unittest.TestCase):
	
	@mock.patch('pipeline.os')
	def test_missing_config_file(self, mock_os):
		mock_os.path.dirname.return_value = '/path/to/dummy_dir'
		mock_os.listdir.return_value = []
		with self.assertRaises(SystemExit):
			p = pipeline.Pipeline()
			p.instrument = 'nextseq'
			p.parse_config_file()

	@mock.patch('pipeline.os')
	def test_multiple_config_files(self, mock_os):
		mock_os.path.dirname.return_value = '/path/to/dummy_dir'
		mock_os.listdir.return_value = ['a.cfg','b.cfg']
		with self.assertRaises(SystemExit):
			p = pipeline.Pipeline()
			p.instrument = 'nextseq'
			p.parse_config_file()
		


class TestOutputDirCreation(unittest.TestCase):

	@mock.patch('pipeline.os')
	def test_bad_input_arg(self, mock_os):
		# tests the case where the input argument for the run directory was not actually typed correctly
		mock_os.path.isdir.return_value = False
		with self.assertRaises(SystemExit):
			p = pipeline.Pipeline()
			p.instrument = 'nextseq'
			p.run_directory_path = '/path/to/dummy1'
			p.config_params_dict = {'demux_output_dir': 'dummy2'}
			p.create_output_directory()	


	def my_join(a,b):
		return os.path.join(a,b)

	@mock.patch('pipeline.os.path.join', side_effect = my_join)
	@mock.patch('pipeline.os')
	def test_mkdir_is_called(self, mock_os, mock_join):
		mock_os.path.isdir.return_value = True
		run_dir_path = '/path/to/rundir'
		bcl2fastq_out = 'bcl2fastq_output'
		p = pipeline.Pipeline()
		p.run_directory_path = run_dir_path
		p.config_params_dict = {'demux_output_dir': bcl2fastq_out}
		p.create_output_directory()			
		mock_os.mkdir.assert_called_once_with('/path/to/rundir/bcl2fastq_output')



class TestFastQCCall(unittest.TestCase):
	
        def my_join(a,b):
                return os.path.join(a,b)

	@mock.patch('pipeline.subprocess.check_call')
	@mock.patch('pipeline.os.path.join', side_effect = my_join)
	@mock.patch('pipeline.os')		
	def test_fastqc_calls_are_correct(self, mock_os, mock_join, mock_check_call_method):

		project_id_list = ['Project_A', 'Project_B']
		# this mocks the generator returned by os.walk() method.
		mock_os.walk.return_value = [('/path/to/target/dir', ['Project_A', 'Project_B'], []),
						('/path/to/target/dir/Project_A', ['Sample_A2', 'Sample_A1'], []),
						('/path/to/target/dir/Project_A/Sample_A2', [], ['A2_1.final.fastq.gz', 'A2_2.final.fastq.gz']),
						('/path/to/target/dir/Project_A/Sample_A1', [], ['A1_1.final.fastq.gz', 'A1_2.final.fastq.gz']),
						('/path/to/target/dir/Project_B', ['Sample_B2', 'Sample_B1'], []),
						('/path/to/target/dir/Project_B/Sample_B2', [], ['B2_1.final.fastq.gz', 'B2_2.final.fastq.gz']),
						('/path/to/target/dir/Project_B/Sample_B1', [], ['B1_1.final.fastq.gz', 'B1_2.final.fastq.gz'])
					]
                expected_call_A = '/cccbstore-rc/projects/cccb/apps/FastQC/fastqc /path/to/target/dir/Project_A/Sample_A1/A1_1.final.fastq.gz'
		expected_call_B = '/cccbstore-rc/projects/cccb/apps/FastQC/fastqc /path/to/target/dir/Project_A/Sample_A1/A1_2.final.fastq.gz'
                expected_call_C = '/cccbstore-rc/projects/cccb/apps/FastQC/fastqc /path/to/target/dir/Project_A/Sample_A2/A2_1.final.fastq.gz'
                expected_call_D = '/cccbstore-rc/projects/cccb/apps/FastQC/fastqc /path/to/target/dir/Project_A/Sample_A2/A2_2.final.fastq.gz'
                expected_call_E = '/cccbstore-rc/projects/cccb/apps/FastQC/fastqc /path/to/target/dir/Project_B/Sample_B1/B1_1.final.fastq.gz'
                expected_call_F = '/cccbstore-rc/projects/cccb/apps/FastQC/fastqc /path/to/target/dir/Project_B/Sample_B1/B1_2.final.fastq.gz'
                expected_call_G = '/cccbstore-rc/projects/cccb/apps/FastQC/fastqc /path/to/target/dir/Project_B/Sample_B2/B2_1.final.fastq.gz'
                expected_call_H = '/cccbstore-rc/projects/cccb/apps/FastQC/fastqc /path/to/target/dir/Project_B/Sample_B2/B2_2.final.fastq.gz'

                calls = [mock.call(expected_call_A, shell=True),
                        mock.call(expected_call_B, shell=True),
                        mock.call(expected_call_C, shell=True),
                	mock.call(expected_call_D, shell=True),
                        mock.call(expected_call_E, shell=True),
                        mock.call(expected_call_F, shell=True),
                        mock.call(expected_call_G, shell=True),
                        mock.call(expected_call_H, shell=True)]

		p = pipeline.Pipeline()
		p.project_id_list = project_id_list
		p.target_dir = '/path/to/target/dir'
		p.config_params_dict = {'fastqc_path': '/cccbstore-rc/projects/cccb/apps/FastQC/fastqc'}
		p.run_fastqc()

                mock_check_call_method.assert_has_calls(calls, any_order = True)





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


	def my_join(*args):
		return reduce(lambda x,y: os.path.join(x,y), args)

	def my_basename(p):
		return os.path.basename(p)


	@mock.patch('pipeline.subprocess.check_call')
	@mock.patch('pipeline.os.path.join', side_effect = my_join)
	@mock.patch('pipeline.os.path.basename', side_effect = my_basename)
	@mock.patch('pipeline.os')
	@mock.patch('pipeline.glob')
	def test_makes_correct_cat_call_for_paired_end_protocol(self, mock_glob, mock_os, mock_basename, mock_join, mock_call):
		mock_os.path.isdir.return_value = True	
		mock_original_dir = '/path/to/original/Project_ABC/Sample_XX_1/'
		mock_target_dir = '/path/to/final_projects_dir/2015/2'		
		mock_os.listdir.return_value = ['Sample_XX_1']
		mock_glob_return_val = [['XX-1_S1_L001_R1_001.fastq.gz', 'XX-1_S1_L003_R1_001.fastq.gz', 'XX-1_S1_L002_R1_001.fastq.gz'],
						['XX-1_S1_L003_R2_001.fastq.gz', 'XX-1_S1_L001_R2_001.fastq.gz', 'XX-1_S1_L002_R2_001.fastq.gz']]
		tmp_fastq_tag = 'tmp'
		mock_glob_return_val = [[os.path.join(mock_original_dir, S) for S in R] for R in mock_glob_return_val]
		mock_glob.glob.side_effect = mock_glob_return_val

		p = pipeline.Pipeline()
		p.config_params_dict = {'demux_output_dir': '/path/to/original', 'sample_dir_prefix':'Sample_', 'tmp_fastq_tag': tmp_fastq_tag}
		p.target_dir = mock_target_dir
		p.project_id_list = ['Project_ABC']
                p.concatenate_fastq_files()

		expected_call_A = 'cat /path/to/original/Project_ABC/Sample_XX_1/XX-1_S1_L001_R1_001.fastq.gz /path/to/original/Project_ABC/Sample_XX_1/XX-1_S1_L002_R1_001.fastq.gz /path/to/original/Project_ABC/Sample_XX_1/XX-1_S1_L003_R1_001.fastq.gz >/path/to/final_projects_dir/2015/2/Project_ABC/Sample_XX_1/XX_1_R1_.' + tmp_fastq_tag + '.fastq.gz'
		expected_call_B = 'cat /path/to/original/Project_ABC/Sample_XX_1/XX-1_S1_L001_R2_001.fastq.gz /path/to/original/Project_ABC/Sample_XX_1/XX-1_S1_L002_R2_001.fastq.gz /path/to/original/Project_ABC/Sample_XX_1/XX-1_S1_L003_R2_001.fastq.gz >/path/to/final_projects_dir/2015/2/Project_ABC/Sample_XX_1/XX_1_R2_.' + tmp_fastq_tag + '.fastq.gz'
		calls = [mock.call(expected_call_A, shell=True),
			mock.call(expected_call_B, shell=True)]
		mock_call.assert_has_calls(calls)


	@mock.patch('pipeline.subprocess.check_call')
	@mock.patch('pipeline.os.path.join', side_effect = my_join)
	@mock.patch('pipeline.os.path.basename', side_effect = my_basename)
	@mock.patch('pipeline.os')
	@mock.patch('pipeline.glob')
	def test_makes_correct_cat_call_for_multiple_samples(self, mock_glob, mock_os, mock_basename, mock_join, mock_call):
		mock_os.path.isdir.return_value = True	
		mock_original_dir_1 = '/path/to/original/Project_ABC/Sample_XX_1/'
		mock_original_dir_2 = '/path/to/original/Project_ABC/Sample_YY_2/'
		mock_target_dir = '/path/to/final_projects_dir/2015/2'		
		mock_os.listdir.return_value = ['Sample_XX_1', 'Sample_YY_2']
		m1 = [['XX-1_S1_L001_R1_001.fastq.gz', 'XX-1_S1_L003_R1_001.fastq.gz', 'XX-1_S1_L002_R1_001.fastq.gz'],
						['XX-1_S1_L003_R2_001.fastq.gz', 'XX-1_S1_L001_R2_001.fastq.gz', 'XX-1_S1_L002_R2_001.fastq.gz']]
		m1 = [[os.path.join(mock_original_dir_1, S) for S in R] for R in m1]
		m2 = [['YY-2_S1_L001_R1_001.fastq.gz', 'YY-2_S1_L003_R1_001.fastq.gz', 'YY-2_S1_L002_R1_001.fastq.gz'],
						['YY-2_S1_L003_R2_001.fastq.gz', 'YY-2_S1_L001_R2_001.fastq.gz', 'YY-2_S1_L002_R2_001.fastq.gz']]
		m2 = [[os.path.join(mock_original_dir_2, S) for S in R] for R in m2]
		mock_glob.glob.side_effect = m1 + m2
		tmp_fastq_tag = 'tmp'
		p = pipeline.Pipeline()
		p.config_params_dict = {'demux_output_dir': '/path/to/original', 'sample_dir_prefix':'Sample_', 'tmp_fastq_tag': tmp_fastq_tag}
		p.target_dir = mock_target_dir
		p.project_id_list = ['Project_ABC']
                p.concatenate_fastq_files()

		expected_call_A = 'cat /path/to/original/Project_ABC/Sample_XX_1/XX-1_S1_L001_R1_001.fastq.gz /path/to/original/Project_ABC/Sample_XX_1/XX-1_S1_L002_R1_001.fastq.gz /path/to/original/Project_ABC/Sample_XX_1/XX-1_S1_L003_R1_001.fastq.gz >/path/to/final_projects_dir/2015/2/Project_ABC/Sample_XX_1/XX_1_R1_.' + tmp_fastq_tag + '.fastq.gz'
		expected_call_B = 'cat /path/to/original/Project_ABC/Sample_XX_1/XX-1_S1_L001_R2_001.fastq.gz /path/to/original/Project_ABC/Sample_XX_1/XX-1_S1_L002_R2_001.fastq.gz /path/to/original/Project_ABC/Sample_XX_1/XX-1_S1_L003_R2_001.fastq.gz >/path/to/final_projects_dir/2015/2/Project_ABC/Sample_XX_1/XX_1_R2_.' + tmp_fastq_tag + '.fastq.gz'
		expected_call_C = 'cat /path/to/original/Project_ABC/Sample_YY_2/YY-2_S1_L001_R1_001.fastq.gz /path/to/original/Project_ABC/Sample_YY_2/YY-2_S1_L002_R1_001.fastq.gz /path/to/original/Project_ABC/Sample_YY_2/YY-2_S1_L003_R1_001.fastq.gz >/path/to/final_projects_dir/2015/2/Project_ABC/Sample_YY_2/YY_2_R1_.' + tmp_fastq_tag + '.fastq.gz'
		expected_call_D = 'cat /path/to/original/Project_ABC/Sample_YY_2/YY-2_S1_L001_R2_001.fastq.gz /path/to/original/Project_ABC/Sample_YY_2/YY-2_S1_L002_R2_001.fastq.gz /path/to/original/Project_ABC/Sample_YY_2/YY-2_S1_L003_R2_001.fastq.gz >/path/to/final_projects_dir/2015/2/Project_ABC/Sample_YY_2/YY_2_R2_.' + tmp_fastq_tag + '.fastq.gz'
		calls = [mock.call(expected_call_A, shell=True),
			mock.call(expected_call_B, shell=True),
			mock.call(expected_call_C, shell=True),
			mock.call(expected_call_D, shell=True)]
		mock_call.assert_has_calls(calls)



	@mock.patch('pipeline.subprocess.check_call')
	@mock.patch('pipeline.os.path.join', side_effect = my_join)
	@mock.patch('pipeline.os.path.basename', side_effect = my_basename)
	@mock.patch('pipeline.os')
	@mock.patch('pipeline.glob')
	def test_makes_correct_cat_call_for_multiple_projects(self, mock_glob, mock_os, mock_basename, mock_join, mock_call):
		mock_os.path.isdir.return_value = True	
		mock_original_dir_1 = '/path/to/original/Project_ABC/Sample_XX_1/'
		mock_original_dir_2 = '/path/to/original/Project_DEF/Sample_YY_2/'
		mock_target_dir = '/path/to/final_projects_dir/2015/2'		
		mock_os.listdir.side_effect = [['Sample_XX_1'], ['Sample_YY_2']]
		m1 = [['XX-1_S1_L001_R1_001.fastq.gz', 'XX-1_S1_L003_R1_001.fastq.gz', 'XX-1_S1_L002_R1_001.fastq.gz'],
						['XX-1_S1_L003_R2_001.fastq.gz', 'XX-1_S1_L001_R2_001.fastq.gz', 'XX-1_S1_L002_R2_001.fastq.gz']]
		m1 = [[os.path.join(mock_original_dir_1, S) for S in R] for R in m1]
		m2 = [['YY-2_S1_L001_R1_001.fastq.gz', 'YY-2_S1_L003_R1_001.fastq.gz', 'YY-2_S1_L002_R1_001.fastq.gz'],
						['YY-2_S1_L003_R2_001.fastq.gz', 'YY-2_S1_L001_R2_001.fastq.gz', 'YY-2_S1_L002_R2_001.fastq.gz']]
		m2 = [[os.path.join(mock_original_dir_2, S) for S in R] for R in m2]
		mock_glob.glob.side_effect = m1 + m2
		tmp_fastq_tag = 'tmp'
		p = pipeline.Pipeline()
		p.config_params_dict = {'demux_output_dir': '/path/to/original', 'sample_dir_prefix':'Sample_', 'tmp_fastq_tag': tmp_fastq_tag}
		p.target_dir = mock_target_dir
		p.project_id_list = ['Project_ABC', 'Project_DEF']
                p.concatenate_fastq_files()
		expected_call_A = 'cat /path/to/original/Project_ABC/Sample_XX_1/XX-1_S1_L001_R1_001.fastq.gz /path/to/original/Project_ABC/Sample_XX_1/XX-1_S1_L002_R1_001.fastq.gz /path/to/original/Project_ABC/Sample_XX_1/XX-1_S1_L003_R1_001.fastq.gz >/path/to/final_projects_dir/2015/2/Project_ABC/Sample_XX_1/XX_1_R1_.' + tmp_fastq_tag + '.fastq.gz'
		expected_call_B = 'cat /path/to/original/Project_ABC/Sample_XX_1/XX-1_S1_L001_R2_001.fastq.gz /path/to/original/Project_ABC/Sample_XX_1/XX-1_S1_L002_R2_001.fastq.gz /path/to/original/Project_ABC/Sample_XX_1/XX-1_S1_L003_R2_001.fastq.gz >/path/to/final_projects_dir/2015/2/Project_ABC/Sample_XX_1/XX_1_R2_.' + tmp_fastq_tag + '.fastq.gz'
		expected_call_C = 'cat /path/to/original/Project_DEF/Sample_YY_2/YY-2_S1_L001_R1_001.fastq.gz /path/to/original/Project_DEF/Sample_YY_2/YY-2_S1_L002_R1_001.fastq.gz /path/to/original/Project_DEF/Sample_YY_2/YY-2_S1_L003_R1_001.fastq.gz >/path/to/final_projects_dir/2015/2/Project_DEF/Sample_YY_2/YY_2_R1_.' + tmp_fastq_tag + '.fastq.gz'
		expected_call_D = 'cat /path/to/original/Project_DEF/Sample_YY_2/YY-2_S1_L001_R2_001.fastq.gz /path/to/original/Project_DEF/Sample_YY_2/YY-2_S1_L002_R2_001.fastq.gz /path/to/original/Project_DEF/Sample_YY_2/YY-2_S1_L003_R2_001.fastq.gz >/path/to/final_projects_dir/2015/2/Project_DEF/Sample_YY_2/YY_2_R2_.' + tmp_fastq_tag + '.fastq.gz'
		calls = [mock.call(expected_call_A, shell=True),
			mock.call(expected_call_B, shell=True),
			mock.call(expected_call_C, shell=True),
			mock.call(expected_call_D, shell=True)]
		mock_call.assert_has_calls(calls)




        @mock.patch('pipeline.subprocess.check_call')
        @mock.patch('pipeline.os.path.join', side_effect = my_join)
        @mock.patch('pipeline.os.path.basename', side_effect = my_basename)
        @mock.patch('pipeline.os')
        @mock.patch('pipeline.glob')
        def test_makes_correct_cat_call_for_single_end_protocol(self, mock_glob, mock_os, mock_basename, mock_join, mock_call):
                mock_os.path.isdir.return_value = True
                mock_original_dir = '/path/to/original/Project_ABC/Sample_XX_1/'
                mock_target_dir	= '/path/to/final_projects_dir/2015/2'
                mock_os.listdir.return_value = ['Sample_XX_1']
                mock_glob_return_val = [['XX-1_S1_L001_R1_001.fastq.gz', 'XX-1_S1_L003_R1_001.fastq.gz', 'XX-1_S1_L002_R1_001.fastq.gz'],
                                                []]
		tmp_fastq_tag = 'tmp'
                mock_glob_return_val = [[os.path.join(mock_original_dir, S) for S in R] for R in mock_glob_return_val]
                mock_glob.glob.side_effect = mock_glob_return_val

		p = pipeline.Pipeline()
		p.config_params_dict = {'demux_output_dir': '/path/to/original', 'sample_dir_prefix':'Sample_', 'tmp_fastq_tag': tmp_fastq_tag}
		p.target_dir = mock_target_dir
		p.project_id_list = ['Project_ABC']

                p.concatenate_fastq_files()
		expected_call_A = 'cat /path/to/original/Project_ABC/Sample_XX_1/XX-1_S1_L001_R1_001.fastq.gz /path/to/original/Project_ABC/Sample_XX_1/XX-1_S1_L002_R1_001.fastq.gz /path/to/original/Project_ABC/Sample_XX_1/XX-1_S1_L003_R1_001.fastq.gz >/path/to/final_projects_dir/2015/2/Project_ABC/Sample_XX_1/XX_1_R1_.' + tmp_fastq_tag + '.fastq.gz'
		calls = [mock.call(expected_call_A, shell=True)]		
		mock_call.assert_has_calls(calls)


        @mock.patch('pipeline.subprocess.check_call')
        @mock.patch('pipeline.os.path.join', side_effect = my_join)
        @mock.patch('pipeline.os.path.basename', side_effect = my_basename)
        @mock.patch('pipeline.os')
        @mock.patch('pipeline.glob')
        def test_issue_error_if_only_read2_files(self, mock_glob, mock_os, mock_basename, mock_join, mock_call):
                mock_os.path.isdir.return_value = True
                mock_original_dir = '/path/to/original/Project_ABC/Sample_XX_1/'
                mock_target_dir	= '/path/to/final_projects_dir/2015/2'
                mock_os.listdir.return_value = ['Sample_XX_1']
                mock_glob_return_val = [[],['XX-1_S1_L001_R2_001.fastq.gz', 'XX-1_S1_L003_R2_001.fastq.gz', 'XX-1_S1_L002_R2_001.fastq.gz']]
                mock_glob_return_val = [[os.path.join(mock_original_dir, S) for S in R] for R in mock_glob_return_val]
                mock_glob.glob.side_effect = mock_glob_return_val
		tmp_fastq_tag = 'tmp'
		p = pipeline.Pipeline()
		p.config_params_dict = {'demux_output_dir': '/path/to/original', 'sample_dir_prefix':'Sample_', 'tmp_fastq_tag': tmp_fastq_tag}
		p.target_dir = mock_target_dir
		p.project_id_list = ['Project_ABC']

		with self.assertRaises(SystemExit):
			p.concatenate_fastq_files()





	@mock.patch('pipeline.subprocess.check_call')
	@mock.patch('pipeline.os.path.join', side_effect = my_join)
	@mock.patch('pipeline.os.path.basename', side_effect = my_basename)
	@mock.patch('pipeline.os')
	@mock.patch('pipeline.glob')
	def test_issue_error_if_differing_number_of_files_in_paired_end_protocol(self, mock_glob, mock_os, mock_basename, mock_join, mock_call):
		mock_os.path.isdir.return_value = True	
		mock_original_dir = '/path/to/original/Project_ABC/Sample_XX_1/'
		mock_target_dir = '/path/to/final_projects_dir/2015/2'		
		mock_os.listdir.return_value = ['Sample_XX_1']
		mock_glob_return_val = [['XX-1_S1_L001_R1_001.fastq.gz', 'XX-1_S1_L002_R1_001.fastq.gz'],
						['XX-1_S1_L003_R2_001.fastq.gz', 'XX-1_S1_L001_R2_001.fastq.gz', 'XX-1_S1_L002_R2_001.fastq.gz']]
		mock_glob_return_val = [[os.path.join(mock_original_dir, S) for S in R] for R in mock_glob_return_val]
		mock_glob.glob.side_effect = mock_glob_return_val
		tmp_fastq_tag = 'tmp'
		p = pipeline.Pipeline()
		p.config_params_dict = {'demux_output_dir': '/path/to/original', 'sample_dir_prefix':'Sample_', 'tmp_fastq_tag': tmp_fastq_tag}
		p.target_dir = mock_target_dir
		p.project_id_list = ['Project_ABC']
		with self.assertRaises(SystemExit):
			p.concatenate_fastq_files()




	@mock.patch('pipeline.subprocess.check_call')
	@mock.patch('pipeline.os.path.join', side_effect = my_join)
	@mock.patch('pipeline.os.path.basename', side_effect = my_basename)
	@mock.patch('pipeline.os')
	@mock.patch('pipeline.glob')
	def test_issue_error_if_no_fastq_files_found(self, mock_glob, mock_os, mock_basename, mock_join, mock_call):
		mock_os.path.isdir.return_value = True	
		mock_original_dir = '/path/to/original/Project_ABC/Sample_XX_1/'
		mock_target_dir = '/path/to/final_projects_dir/2015/2'		
		mock_os.listdir.return_value = ['Sample_XX_1']
		mock_glob_return_val = [[],[]]
		mock_glob_return_val = [[os.path.join(mock_original_dir, S) for S in R] for R in mock_glob_return_val]
		mock_glob.glob.side_effect = mock_glob_return_val

		tmp_fastq_tag = 'tmp'
		p = pipeline.Pipeline()
		p.config_params_dict = {'demux_output_dir': '/path/to/original', 'sample_dir_prefix':'Sample_', 'tmp_fastq_tag': tmp_fastq_tag}
		p.target_dir = mock_target_dir
		p.project_id_list = ['Project_ABC']
		with self.assertRaises(SystemExit):
			p.concatenate_fastq_files()









class TestCreatingFinalLocations(unittest.TestCase):

	def my_join(*args):
		return reduce(lambda x,y: os.path.join(x,y), args)


	@mock.patch('pipeline.os.path.join', side_effect = my_join)	
	@mock.patch('pipeline.os')	
	def test_create_project_structure(self, mock_os, mock_join):
		mock_target = '/path/to/final_projects_dir/'
		mock_bcl2fastq2_output_dir = '/path/to/bcl2fastq2_output'
		sample_dir_names = ['Sample_AA', 'Sample_BB']
		mock_os.listdir.return_value = sample_dir_names
		mock_project_id = 'Project_XX'
		mock_new_project_dir = os.path.join(mock_target, mock_project_id)
		mock_sample_subdirs = [os.path.join(mock_new_project_dir, s) for s in sample_dir_names]
		mock_calls = [mock.call(mock_new_project_dir)]
	        mock_calls.extend([mock.call(s) for s in mock_sample_subdirs])

		p = pipeline.Pipeline()
		p.config_params_dict = {'demux_output_dir': mock_bcl2fastq2_output_dir, 'sample_dir_prefix':'Sample_'}
		p.target_dir = mock_target
		p.create_project_structure(mock_project_id)

                mock_os.mkdir.assert_has_calls(mock_calls)


	@mock.patch('pipeline.os.path.join', side_effect = my_join)	
	@mock.patch('pipeline.os')	
	def test_error_raised_if_mkdir_has_problem(self, mock_os, mock_join):
		mock_target = '/path/to/final_projects_dir/'
		mock_bcl2fastq2_output_dir = '/path/to/bcl2fastq2_output'
		sample_dir_names = ['Sample_AA', 'Sample_BB']
		mock_os.listdir.return_value = sample_dir_names
		mock_project_id = 'Project_XX'
		mock_new_project_dir = os.path.join(mock_target, mock_project_id)
		mock_sample_subdirs = [os.path.join(mock_new_project_dir, s) for s in sample_dir_names]

		mock_os.mkdir.side_effect = [OSError(2,'foo')]
		with self.assertRaises(SystemExit):
			p = pipeline.Pipeline()
			p.config_params_dict = {'demux_output_dir': mock_bcl2fastq2_output_dir, 'sample_dir_prefix':'Sample_'}
			p.target_dir = '/path/to/target_dir'
			p.create_project_structure(mock_project_id)



	@mock.patch('pipeline.os.path.join', side_effect = my_join)
	@mock.patch('pipeline.os')
	def test_making_destination_year_directory_raises_unexpected_exception(self, mock_os, mock_join):
		from datetime import datetime as date
		today = date.today()
		year = today.year
		month = today.month

		mock_os.path.isdir.side_effect = [True, False]
		mock_project_dir_path = '/path/to/projects_dir/'
		mock_os.mkdir.side_effect = OSError(2, 'foo')
		with self.assertRaises(SystemExit):
			p = pipeline.Pipeline()
			p.config_params_dict = {'destination_path': mock_project_dir_path}
			p.project_id_list = ['ZZ_1234',]
			p.create_final_locations()


	@mock.patch('pipeline.os.path.join', side_effect = my_join)
	@mock.patch('pipeline.os')
	def test_making_destination_month_directory_raises_unexpected_exception(self, mock_os, mock_join):
		from datetime import datetime as date
		today = date.today()
		year = today.year
		month = today.month

		mock_os.path.isdir.side_effect = [True, True, False]
		mock_project_dir_path = '/path/to/projects_dir/'
		mock_os.mkdir.side_effect = OSError(2, 'foo')
		with self.assertRaises(SystemExit):
			p = pipeline.Pipeline()
			p.config_params_dict = {'destination_path': mock_project_dir_path}
			p.project_id_list = ['ZZ_1234',]
			p.create_final_locations()



	@mock.patch('pipeline.Pipeline.create_project_structure')
	@mock.patch('pipeline.os.path.join', side_effect = my_join)
	@mock.patch('pipeline.os')
	def test_makes_proper_calls_for_creating_project_hierarchy(self, mock_os, mock_join, mock_called_method):
		'''
		Ensures that the call to the method create_project_structure(...) is as expected.  That method
		is tested independently elsewhere
		'''
		from datetime import datetime as date
		today = date.today()
		year = today.year
		month = today.month

		mock_os.path.isdir.return_value = True
		mock_project_dir_path = '/path/to/final_projects_dir/'
		mock_bcl2fastq2_dir_path = '/path/to/bcl2fastq2_output_dir/'
		mock_target = os.path.join(mock_project_dir_path, str(year), str(month))
		mock_os.makedirs.side_effect = OSError(17, 'foo')

		p = pipeline.Pipeline()
		p.config_params_dict = {'destination_path': mock_project_dir_path, 'demux_output_dir': mock_bcl2fastq2_dir_path}
		p.project_id_list = ['ZZ_1234','AB_1234']
		p.create_final_locations()

		mock_calls = [mock.call(p,'ZZ_1234'),
				mock.call(p,'AB_1234')]

		mock_called_method.assert_has_calls(mock_calls)



class TestEmailNotifications(unittest.TestCase):

	def test_creates_correct_url_for_email(self):
		import re
		delivery_links = {'Project_XX':'/cccbstore-rc/projects/cccb/outgoing/frd/2015/2/Project_XX/delivery_home.html'}
		external_url = 'https://cccb-download.dfci.harvard.edu/frd/'
		internal_drop_location = '/cccbstore-rc/projects/cccb/outgoing/frd/'
		s = write_html_links(delivery_links, external_url, internal_drop_location)
		expected_url = r'https://cccb-download.dfci.harvard.edu/frd/2015/2/Project_XX/delivery_home.html'
		match = re.findall(expected_url, s, re.DOTALL)
		self.assertTrue(match[0] == expected_url )

	"""
	# this actually sends an email to the recipients-- only used for verification since it's not quite a unit test.	
	def test_creates_correct_url_for_email(self):
		delivery_links = {'Project_XX':'/cccbstore-rc/projects/cccb/outgoing/frd/2015/2/Project_XX/delivery_home.html',
				'Project_YY': '/cccbstore-rc/projects/cccb/outgoing/frd/2015/2/Project_YY/delivery_home.html'}
		external_url = 'https://cccb-download.dfci.harvard.edu/frd/'
		internal_drop_location = '/cccbstore-rc/projects/cccb/outgoing/frd/'
		recipients='dummy@domain.org'
		send_notifications(recipients, delivery_links, 'servername', 25, external_url, internal_drop_location)
	"""


class TestMultipleFlowcellFunctionality(unittest.TestCase):

	def my_join(*args):
		return reduce(lambda x,y: os.path.join(x,y), args)

	def my_basename(p):
		return os.path.basename(p)

	@mock.patch('pipeline.os.rename')
	@mock.patch('pipeline.os.path.join', side_effect = my_join)
	@mock.patch('pipeline.os.path.basename', side_effect = my_basename)
	@mock.patch('pipeline.os')
	@mock.patch('pipeline.glob')
	def test_handles_same_sample_run_on_multiple_flowcells(self, mock_glob, mock_os, mock_basename, mock_join, mock_rename):
		'''
		This tests the case where the same sample is run on multiple flowcells-- need to merge the fastq files from them
		'''
		mock_os.path.isfile.return_value = True	
		mock_sample_dir = '/path/to/original/Project_ABC/Sample_XX_1'

		# mock there already being two prior flowcells run which had this sample
		mock_glob.glob.return_value = [os.path.join(mock_sample_dir, 'XX_1_R1_.fc1.fastq.gz'), os.path.join(mock_sample_dir, 'XX_1_R1_.fc2.fastq.gz')]

		# mock the existing final fastq (which is the concatenation of all prior flowcells)
		mock_existing_final_fastq = os.path.join(mock_sample_dir, 'XX_1_R1_.final.fastq.gz')

		# a placeholder file to concatenate into (cannot concatenate into *final.fastq.gz since that is one of the files 'providing' data to the stream
		mock_tmp_fastq = os.path.join(mock_sample_dir, 'XX_1_R1_.tmp')

		# the file created by concatenating the lane-wise fastq files from the bcl2fastq process.
		mock_new_fastq = os.path.join(mock_sample_dir, 'XX_1_R1_.tmp.fastq.gz')

		# the desired name for this new fastq file since there were 2 prior runs
		mock_new_fc_fastq = os.path.join(mock_sample_dir, 'XX_1_R1_.fc3.fastq.gz')
		tmp_fastq_tag = 'tmp'
		p = pipeline.Pipeline()
		p.config_params_dict = {'sample_dir_prefix':'Sample_', 'tmp_fastq_tag': tmp_fastq_tag, 'final_fastq_tag': 'final'}
		p.execute_call = mock.Mock()
		p.merge_and_rename_fastq(mock_sample_dir, 1)
		mock_cat_call = 'cat ' + mock_existing_final_fastq + ' ' + mock_new_fc_fastq + '> ' + mock_tmp_fastq
		p.execute_call.assert_called_once_with(mock_cat_call)
		mock_calls = [mock.call(mock_new_fastq, mock_new_fc_fastq), mock.call(mock_tmp_fastq, mock_existing_final_fastq)]
		mock_rename.assert_has_calls(mock_calls)


	@mock.patch('pipeline.os.symlink')
	@mock.patch('pipeline.os.rename')
	@mock.patch('pipeline.os.path.join', side_effect = my_join)
	@mock.patch('pipeline.os.path.basename', side_effect = my_basename)
	@mock.patch('pipeline.os')
	@mock.patch('pipeline.glob')
	def test_handles_new_sample_as_expected(self, mock_glob, mock_os, mock_basename, mock_join, mock_rename, mock_symlink):
		'''
		This tests the case where the same sample is run on multiple flowcells-- need to merge the fastq files from them
		'''
		mock_os.path.isfile.return_value = False # means no existing 'final' fastq file	
		mock_sample_dir = '/path/to/original/Project_ABC/Sample_XX_1'

		# mock there being no prior flowcells run which had this sample
		mock_glob.glob.return_value = []

		# the file created by concatenating the lane-wise fastq files from the bcl2fastq process.
		mock_new_fastq = os.path.join(mock_sample_dir, 'XX_1_R1_.tmp.fastq.gz')

		# the desired name for this new fastq file since there were 2 prior runs
		mock_new_fc_fastq = os.path.join(mock_sample_dir, 'XX_1_R1_.fc1.fastq.gz')

		# the expected final fastq name:
		mock_final_fastq = os.path.join(mock_sample_dir, 'XX_1_R1_.final.fastq.gz')

		tmp_fastq_tag = 'tmp'
		p = pipeline.Pipeline()
		p.config_params_dict = {'sample_dir_prefix':'Sample_', 'tmp_fastq_tag': tmp_fastq_tag, 'final_fastq_tag': 'final'}
		p.execute_call = mock.Mock()
		p.merge_and_rename_fastq(mock_sample_dir, 1)

		mock_rename_calls = [mock.call(mock_new_fastq, mock_new_fc_fastq),]
		mock_rename.assert_has_calls(mock_rename_calls)

		mock_symlink_calls = [mock.call(mock_new_fc_fastq, mock_final_fastq),]
		mock_symlink.assert_has_calls(mock_symlink_calls)



if __name__ == '__main__':
	unittest.main()
