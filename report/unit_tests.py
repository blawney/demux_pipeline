import unittest
import mock

from publish import *

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
		rv = setup_links(date_stamped_delivery_dir, origin_dir, project_id_list, sample_dir_prefix, fastqc_output_suffix)
		mock_os.symlink.assert_has_calls(mock_calls)



if __name__ == '__main__':
	unittest.main()
