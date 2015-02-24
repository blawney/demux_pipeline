#!/cccbstore-rc/projects/cccb/apps/Python-2.7.1/python

import sys
import os
import re
import subprocess
import shutil
import glob
from datetime import datetime as date
from ConfigParser import SafeConfigParser


def parse_config_file():
	"""
	This function finds and parses a configuration file, which contains various constants to run this pipeline
	"""

	current_dir = os.path.dirname(os.path.realpath(__file__)) # this gets the directory of this script
	cfg_files = [f for f in os.listdir(current_dir) if f.endswith('cfg')]
	if len(cfg_files) == 1:
		parser = SafeConfigParser()
		parser.readfp(cfg_files[0])
		return parser.defaults()
	else:
		print 'There were %d config (*cfg) files found in %s.  Need exactly 1.  Correct this.' % (len(cfg_files), current_dir)
		sys.exit(1)



def parse_commandline_args():
	"""
	This method parses the commandline arguments and returns the path to the run directory
	"""

	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('-r', 
				'--rundir', 
				required = True,
				help = 'The full path to the run directory',
				dest = 'run_directory')	

	return parser.parse_args().run_directory



def create_output_directory(run_directory_path, bcl2fastq2_output_dirname):
	"""
	This method takes a path to the run directory and attempts to create an output
	directory for the bcl2fastq2 output, returning the path.  If it cannot (due to write permissions, etc)
	then it exits the script and issues a message
	"""
	if os.path.isdir(run_directory_path):
		try:
			output_directory_path = os.path.join(run_directory_path, bcl2fastq2_output_dirname)
			os.mkdir(output_directory_path)
			os.chmod(output_directory_path, 0774)
		except:
			print 'An exception occurred when attempting to create the output directory at: %s' % output_directory_path
			sys.exit(1)
	else:
		print 'The path supplied as the run directory is not actually a directory.  Please check.'
		sys.exit(1)



def line_is_valid(line, sample_dir_prefix):
	"""
	Receives a sample annotation line from the Illumina-generated SampleSheet.csv and does some quick checks to see
	that the Samplesheet.csv has the correct formatting for our pipelines
	"""
	elements = line.split(',')

	# ensure the sample name only has letters, numbers, and underscores with a greedy regex.  No other characters allowed.
	match = re.match('[a-z0-9_]+', elements[1], re.IGNORECASE)

	# if the greedy regex did not match the sample name, then something is wrong.  Only proceed if the name is valid 
	if match and match.group() == elements[1]:
		tests = []
		tests.append(elements[0].startswith(sample_dir_prefix)) # check that the Sample_ID field has the proper prefix
		tests.append(elements[0].strip(sample_dir_prefix) == elements[1]) # check that the sample names match between the first and second fields
		tests.append(len(elements[5]) == 7) # check that 7bp index was provided
		tests.append(len(elements[6])>0) # check that there is an entry in the Sample_Project field.  Important for splitting FASTQs between projects
		return all(tests)
	else:
		return False



def check_samplesheet(run_dir_path, sample_dir_prefix):
	"""
	This method checks for a samplesheet and does a quick check that it is formatted within guidelines (described elsewhere)
	"""
	samplesheet_path = os.path.join(run_dir_path, 'SampleSheet.csv')

	if os.path.isfile(samplesheet_path):
		# the following regex extracts the sample annotation section, which is started by '[Data]'
		# and continues until it reaches another section (or the end of the file)
		# re.findall() returns a list-- get the first element
		try:
			sample_annotation_section = re.findall(r'\[Data\][^\[]*', open(samplesheet_path).read())[0]	
		except IndexError:
			print 'Could not find the [Data] section in the SampleSheet.csv file'
			sys.exit(1)
 
		# this statement gets us a list, where each item in the list is the annotation line in the sample sheet.  
		# The [2:] index removes the '[Data]' and the line with the field headers 
		# Also strips off any Windows/Mac endline characters ('\r') if present
		annotation_lines = [line.rstrip('\r') for line in sample_annotation_section.split('\n') if len(line)>0][2:]

		# a list of True/False on whether the sample annotation line is valid
		lines_valid = [line_is_valid(line, sample_dir_prefix) for line in annotation_lines]
		
		if not all(lines_valid):
			problem_lines = [i+1 for i,k in enumerate(lines_valid) if k == False]
			print 'There was some problem with the SampleSheet.csv'
			print 'Problem lines: %s' % problem_lines
			sys.exit(1)

		# now get a list of all the projects that we are processing in this sampling run:
		project_id_list = [line.split(',')[6] for line in annotation_lines]
		return project_id_list
	else:
		print 'Could not locate a SampleSheet.csv file in %s ' % run_dir_path
		sys.exit(1)


def run_demux(bcl2fastq2_path, run_dir_path, output_dir_path):
	"""
	This starts off the demux process.  Catches any errors in the process via the system exit code
	"""
	try:
		call_command = bcl2fastq2_path + '--output-dir' + output_dir_path + '--runfolder-dir' + run_dir_path
		subprocess.check_call(call_command, shell = True)
	except subprocess.CalledProcessError:
		print 'The demux process had non-zero exit status.  Check the log.'
		sys.exit(1)



def concatenate_fastq_files(output_directory_path, project_id_list, sample_dir_prefix):
	"""
	This method scans the output and concatenates the fastq files for each sample and read number.
	That is, the NextSeq output (in general) has a fastq file for each lane and sample and we need to 
	concatenate the lane files into a single fastq file for that sample.  For paired-end protocol, need
	to do this for both the R1 and R2 reads.
	"""

	for project in project_id_list:
		project_dir = os.path.join(output_directory_path, project)
		
		# get the sample-specific subdirectories and append the path to the project directory in front (for full path name):
		sample_dirs = [os.path.join(project_dir, d) for d in os.listdir(project_dir) if d.startswith(sample_dir_prefix)]

		# double check that they are actually directories:
		sample_dirs = filter( lambda x: os.path.isdir(x), sample_dirs)

		for sample_dir in sample_dirs:

			# since bcl2fastq2 renames the fastq files with a different scheme, extract the sample name we want via parsing the directory name
			sample_name = os.path.basename(sample_dir).strip(sample_dir_prefix)

			# get all the fastq files as lists.  Note the sort so they are concatenated in the same order for paired-end protocol
			read_1_fastq_files = sorted(glob.glob(os.path.join(sample_dir, '*R1_001.fastq.gz')))			
			read_2_fastq_files = sorted(glob.glob(os.path.join(sample_dir, '*R2_001.fastq.gz'))) # will be empty list [] if single-end protocol

			# need at least the read 1 files to continue
			if len(read_1_fastq_files) == 0:
				print 'Did not find any fastq files in %s directory.' % sample_dir
				sys.exit(1)			

			# make file names for the merged files:
			merged_read_1_fastq = sample_name + '_R1_001.fastq.gz'
			merged_read_2_fastq = sample_name + '_R2_001.fastq.gz'

			# an inline method to compose the concatenation command:
			write_call = lambda infiles,outfile: 'cat '+ ' '.join(infiles) + ' >' + outfile

			call_list = []
			call_list.append(write_call(read_1_fastq_files, merged_read_1_fastq))
			if len(read_2_fastq) > 0:
				call_list.append(write_call(read_1_fastq_files, merged_read_1_fastq))
			
			for call in call_list:
				try:
					print 'Issuing system command: %s ' % call
					subprocess.check_call( call, shell = True )			
				except subprocess.CalledProcessError:
					print 'The concatentation of the lane-specific fastq files failed somehow'
					sys.exit(1)
					


def relocate_fastq_files(src_dir, projects_dir, project_id_list):
	"""
	This function uses the project_id_list to move the project directories to another location
	"""

	# for organizing projects by date:
	today = date.today()
	year = today.year
	month = today.month

	# double-check that the config file had a valid path to a directory
	if os.path.isdir(projects_dir):
		target_dir = os.path.join(projects_dir, year, month)

		# try to create the directory-- it may already exist, in which case we catch the exception and move on.
		# Any other errors encountered in creating the directory will cause pipeline to exit
		try:
			os.makedirs(target_dir)
		except OSError as ex:
			if ex.errno != 17: # 17 indicates that the directory was already there.
				print 'Exception occured:'
				print ex.strerror
				sys.exit(1)

		# check that we do have a 
		if os.path.isdir(target_dir):
			for project_id in project_id_list:
				shutil.move( os.path.join(src_dir, project_id), target_dir )
		else:
			print 'Target directory %s does not exist for some reason.  Maybe permissions?' % target_dir		




def correct_permissions(bcl2fastq2_outdir):
	"""
	Gives write privileges to the biocomp group (if not already there)
	"""
	for root, dirs, files in os.walk(bcl2fastq2_outdir):
		for d in dirs:
			os.chmod(os.path.join(root, d), 0774)
		for f in files:
			os.chmod(os.path.join(root, f), 0774)



def process():
	config_params_dict = parse_config_file()

	bcl2fastq2_path = config_params_dict.get('bcl2fastq2_path')
	bcl2fastq2_output_dir = config_params_dict.get('bcl2fastq2_output_dir')
	sample_dir_prefix = config_params_dict('sample_dir_prefix')
	final_destination_path = config_params_dict.get('destination_path')

	run_directory_path = parse_commandline_args()
	output_directory_path = create_output_directory(run_directory_path, bcl2fastq2_output_dir)
	project_id_list = check_samplesheet(run_directory_path, sample_dir_prefix)
	run_demux(bcl2fastq2_path, run_directory_path, output_directory_path)
	correct_permissions(output_directory_path)
	concatenate_fastq_files(output_directory_path, project_id_list, sample_dir_prefix)
	relocate_fastq_files(output_directory_path, final_destination_path, project_id_list)



if __name__ == '__main__':
	process()
