import unittest

from process_sequencing_run import *

class UnitTests(unittest.TestCase):

	def test_samplesheet_prefix(self):
		# this line is missing the 'Sample_' prefix in the first field
		bad_line = 'S,S,,,A001,ATCACGA,Project_X,'
		self.assertFalse(line_is_valid(bad_line))

	def test_samplesheet_matching_sample_names(self):
		# this line has mismatching sample names between fields 1 and 2 (first is 'XX', second is 'X')
		bad_line = 'Sample_XX,X,,,A001,ATCACGA,Project_X,'
		self.assertFalse(line_is_valid(bad_line))

	def test_samplesheet_has_proper_length_index(self):
		# this line has only a 6bp index
		bad_line = 'Sample_XX,XX,,,A001,ATCACG,Project_X,'
		self.assertFalse(line_is_valid(bad_line))

	def test_samplesheet_has_project_id(self):
		# this line is missing a client/project ID
		bad_line = 'Sample_XX,XX,,,A001,ATCACGA,,'
		self.assertFalse(line_is_valid(bad_line))

	def test_samplesheet_valid_line(self):
		# this line is formatted correctly
		good_line = 'Sample_XX,XX,,,A001,ATCACGA,Project_X,'
		self.assertTrue(line_is_valid(good_line))		
		


if __name__ == '__main__':
	unittest.main()
