import unittest
import mock
import os

import publish

class SymLinkTest(unittest.TestCase):

	def my_join(*args):
		return reduce(lambda x,y: os.path.join(x,y), args)

	def my_basename(f):
		return os.path.basename(f)

	@mock.patch('publish.os.path.basename', side_effect = my_basename)
	@mock.patch('publish.os.path.join', side_effect = my_join)
	@mock.patch('publish.glob')
	@mock.patch('publish.os')
	def test_symlinks_called_correctly(self, mock_os, mock_glob, mock_join, mock_basename):

		date_stamped_delivery_dir = '/path/to/outgoing/frd/2015/2'
		origin_dir = '/path/to/origin/2015/2'
		project_id_list = ['Project_XXX']
		sample_dir_prefix = 'Sample_'
		fastqc_output_suffix = '_fastqc'

		mock_os.listdir.side_effect = [['Sample_AAA', 'Sample_BBB']]

		mock_glob.glob.side_effect = [
			['/path/to/origin/2015/2/Project_XXX/Sample_AAA/AAA_R1.fastq.gz','/path/to/origin/2015/2/Project_XXX/Sample_BBB/BBB_R1.fastq.gz'],
			['/path/to/origin/2015/2/Project_XXX/Sample_AAA/AAA_R1_fastqc','/path/to/origin/2015/2/Project_XXX/Sample_BBB/BBB_R1_fastqc']]

		expected_call_1 = mock.call('/path/to/origin/2015/2/Project_XXX/Sample_AAA/AAA_R1.fastq.gz', '/path/to/outgoing/frd/2015/2/Project_XXX/AAA_R1.fastq.gz')
		expected_call_2 = mock.call('/path/to/origin/2015/2/Project_XXX/Sample_BBB/BBB_R1.fastq.gz', '/path/to/outgoing/frd/2015/2/Project_XXX/BBB_R1.fastq.gz')
		expected_call_3 = mock.call('/path/to/origin/2015/2/Project_XXX/Sample_AAA/AAA_R1_fastqc', '/path/to/outgoing/frd/2015/2/Project_XXX/AAA_R1_fastqc')
		expected_call_4 = mock.call('/path/to/origin/2015/2/Project_XXX/Sample_BBB/BBB_R1_fastqc', '/path/to/outgoing/frd/2015/2/Project_XXX/BBB_R1_fastqc')
		mock_calls = [expected_call_1,expected_call_2,expected_call_3,expected_call_4]
		rv = publish.setup_links(date_stamped_delivery_dir, origin_dir, project_id_list, sample_dir_prefix, fastqc_output_suffix)
		mock_os.symlink.assert_has_calls(mock_calls)


class DirectoryCreationTest(unittest.TestCase):

	def my_join(*args):
		return reduce(lambda x,y: os.path.join(x,y), args)

	def my_basename(f):
		return os.path.basename(f)

	class DummyYear(object):
		def __init__(self, year, month):
			self.year = year
			self.month = month


	@mock.patch('publish.date')
	@mock.patch('publish.os.path.basename', side_effect = my_basename)
	@mock.patch('publish.os.path.join', side_effect = my_join)
	@mock.patch('publish.os')
	def test_new_year_dir(self, mock_os, mock_join, mock_basename, mock_date):
		publish.correct_permissions = mock.Mock()
		mock_os.mkdir = mock.Mock()
		mock_date.today = mock.Mock()
		mock_year = '2016'
		mock_month = '3'
		mock_date.today.return_value = self.DummyYear(int(mock_year), int(mock_month))
		mock_delivery_home = '/path/to/delivery_home'
		mock_os.path.isdir.side_effect = [False, False, True]		
		project_id_list = ['Project_ABC', 'Project_DEF']

		publish.create_delivery_locations(mock_delivery_home, project_id_list)

		mkdir_calls = [mock.call(os.path.join(mock_delivery_home, mock_year)), mock.call(os.path.join(mock_delivery_home, mock_year, mock_month)), 
			mock.call(os.path.join(mock_delivery_home, mock_year, mock_month, project_id_list[0])),
			mock.call(os.path.join(mock_delivery_home, mock_year, mock_month, project_id_list[1]))]
		correct_permission_calls = [mock.call(os.path.join(mock_delivery_home, mock_year)), mock.call(os.path.join(mock_delivery_home, mock_year, mock_month)), 
			mock.call(os.path.join(mock_delivery_home, mock_year, mock_month, project_id_list[0])),
			mock.call(os.path.join(mock_delivery_home, mock_year, mock_month, project_id_list[1]))]
		mock_os.mkdir.assert_has_calls(mkdir_calls)
		publish.correct_permissions.assert_has_calls(correct_permission_calls)


if __name__ == '__main__':
	unittest.main()
