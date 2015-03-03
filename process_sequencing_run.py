#!/cccbstore-rc/projects/cccb/apps/Python-2.7.1/python

import logging
import sys
import os
import re
import subprocess
import shutil
import glob
from report.publish import publish
from datetime import datetime as date
from ConfigParser import SafeConfigParser
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def parse_config_file():
	"""
	This function finds and parses a configuration file, which contains various constants to run this pipeline
	"""

	current_dir = os.path.dirname(os.path.realpath(__file__)) # this gets the directory of this script
	cfg_files = [os.path.join(current_dir, f) for f in os.listdir(current_dir) if f.endswith('cfg')]
	if len(cfg_files) == 1:
		logging.info('Parsing configuration file: %s' % cfg_files[0])
		parser = SafeConfigParser()
		parser.read(cfg_files[0])
		return parser.defaults()
	else:
		logging.error('There were %d config (*cfg) files found in %s.  Need exactly 1.  Correct this.' % (len(cfg_files), current_dir))
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
	parser.add_argument('-e', 
				'--email', 
				required = False,
				help = 'Comma-separated list of email addresses for notifications (no spaces between entries)',
				dest = 'recipients')

	args = parser.parse_args()
	return (args.run_directory, args.recipients)



def create_output_directory(run_directory_path, bcl2fastq2_output_dirname):
	"""
	This method takes a path to the sequencing run directory and attempts to create an output
	directory for the bcl2fastq2 output.  If it cannot (due to write permissions, etc)
	then it exits the script and issues a message
	"""
	if os.path.isdir(run_directory_path):
		try:
			output_directory_path = os.path.join(run_directory_path, bcl2fastq2_output_dirname)
			logging.info('Creating output directory for demux process at %s' % output_directory_path)			
			os.mkdir(output_directory_path)
			correct_permissions(output_directory_path)
			return output_directory_path
		except:
			logging.error('An exception occurred when attempting to create the output directory at: %s' % output_directory_path)
			sys.exit(1)
	else:
		logging.error('The path supplied as the run directory is not actually a directory.  Please check.')
		sys.exit(1)



def line_is_valid(line, sample_dir_prefix):
	"""
	Receives a sample annotation line from the Illumina-generated SampleSheet.csv and does some quick checks to see
	that the Samplesheet.csv has the correct formatting for our pipelines.
	Returns a boolean: True is all the fields are within guidelines, otherwise False
	"""
	elements = line.split(',')

	# establish a greedy regex for only letters, numbers, and underscores.  No other characters allowed.
	pattern = re.compile('[a-z0-9_]+', re.IGNORECASE)

	try:
		tests = []
		tests.append(pattern.match(elements[1]).group() == elements[1]) # if the greedy regex did not match the sample name, then something is wrong.
		tests.append(elements[0].startswith(sample_dir_prefix)) # check that the Sample_ID field has the proper prefix
		tests.append(elements[0].strip(sample_dir_prefix) == elements[1]) # check that the sample names match between the first and second fields
		tests.append(len(elements[5]) == 7) # check that 7bp index was provided
		tests.append(pattern.match(elements[6]).group() == elements[6]) # if the greedy regex did not match the full project designation then something is wrong.
		return all(tests)
	except Exception:
		return False



def check_samplesheet(run_dir_path, sample_dir_prefix):
	"""
	This method checks for a samplesheet and does a quick check that it is formatted within guidelines (described elsewhere)
	Returns a list of the Project identifiers.
	"""
	samplesheet_path = os.path.join(run_dir_path, 'SampleSheet.csv')
	logging.info('Examining samplesheet at: %s' % samplesheet_path)

	if os.path.isfile(samplesheet_path):
		# the following regex extracts the sample annotation section, which is started by '[Data]'
		# and continues until it reaches another section (or the end of the file)
		# re.findall() returns a list-- get the first element
		try:
			sample_annotation_section = re.findall(r'\[Data\][^\[]*', open(samplesheet_path).read())[0]	
		except IndexError:
			logging.error('Could not find the [Data] section in the SampleSheet.csv file')
			sys.exit(1)
 
		# this statement gets us a list, where each item in the list is the annotation line in the sample sheet.  
		# The [2:] index removes the '[Data]' and the line with the field headers 
		# Also strips off any Windows/Mac endline characters ('\r') if present
		annotation_lines = [line.rstrip('\r') for line in sample_annotation_section.split('\n') if len(line)>0][2:]

		# a list of True/False on whether the sample annotation line is valid
		lines_valid = [line_is_valid(line, sample_dir_prefix) for line in annotation_lines]
		if not all(lines_valid):
			problem_lines = [i+1 for i,k in enumerate(lines_valid) if k == False]
			logging.error('There was some problem with the SampleSheet.csv')
			logging.error('Problem lines: %s' % problem_lines)
			sys.exit(1)

		# now get a list of all the projects that we are processing in this sampling run:
		project_id_list = list(set([line.split(',')[6] for line in annotation_lines]))

		logging.info('The following projects were identified from the SampleSheet.csv: %s' % project_id_list)
		return project_id_list
	else:
		logging.error('Could not locate a SampleSheet.csv file in %s ' % run_dir_path)
		sys.exit(1)



def run_demux(bcl2fastq2_path, run_dir_path, output_dir_path):
	"""
	This starts off the demux process.  Catches any errors in the process via the system exit code
	"""
	try:
		call_command = bcl2fastq2_path + ' --output-dir ' + output_dir_path + ' --runfolder-dir ' + run_dir_path
		subprocess.check_call(call_command, shell = True)
	except subprocess.CalledProcessError:
		logging.error('The demux process had non-zero exit status.  Check the log.')
		sys.exit(1)



def concatenate_fastq_files(output_directory_path, target_directory_path, project_id_list, sample_dir_prefix):
	"""
	This method scans the output and concatenates the fastq files for each sample and read number.
	That is, the NextSeq output (in general) has a fastq file for each lane and sample and we need to 
	concatenate the lane files into a single fastq file for that sample.  For paired-end protocol, need
	to do this for both the R1 and R2 reads.

	'target_directory_path' is a time-stamped subdirectory of the more general output projects directory
	This is where the empty directories were already setup.

	'output_directory_path' is the full path to the output directory from the demux process.
	"""

	for project_id in project_id_list:

		project_dir = os.path.join(output_directory_path, project_id)

		# get the sample-specific subdirectories and append the path to the project directory in front (for full path name):
		# this gives the fully qualified location of the original fastq files (by lane)
		sample_dirs = [os.path.join(project_dir, d) for d in os.listdir(project_dir) if d.startswith(sample_dir_prefix)] 

		# double check that they are actually directories:
		sample_dirs = filter( lambda x: os.path.isdir(x), sample_dirs)

		for sample_dir in sample_dirs:
			# since bcl2fastq2 renames the fastq files with a different scheme, extract the sample name we want via parsing the directory name
			sample_name_with_prefix = os.path.basename(sample_dir)
			sample_name = sample_name_with_prefix.strip(sample_dir_prefix)

			# get all the fastq files as lists.  Note the sort so they are concatenated in the same order for paired-end protocol
			read_1_fastq_files = sorted(glob.glob(os.path.join(sample_dir, '*R1_001.fastq.gz')))			
			read_2_fastq_files = sorted(glob.glob(os.path.join(sample_dir, '*R2_001.fastq.gz'))) # will be empty list [] if single-end protocol

			# need at least the read 1 files to continue
			if len(read_1_fastq_files) == 0:
				logging.error('Did not find any fastq files in %s directory.' % sample_dir)
				sys.exit(1)			

			# make file names for the merged files:
			merged_read_1_fastq = sample_name + '_R1_001.fastq.gz'
			merged_read_2_fastq = sample_name + '_R2_001.fastq.gz'

			# construct full paths to the final files:
			merged_read_1_fastq = os.path.join(target_directory_path, project_id, sample_name_with_prefix, merged_read_1_fastq)
			merged_read_2_fastq = os.path.join(target_directory_path, project_id, sample_name_with_prefix, merged_read_2_fastq)

			# an inline method to compose the concatenation command:
			write_call = lambda infiles,outfile: 'cat '+ ' '.join(infiles) + ' >' + outfile

			call_list = []
			call_list.append(write_call(read_1_fastq_files, merged_read_1_fastq))
			if len(read_2_fastq_files) > 0:
				if len(read_2_fastq_files) == len(read_1_fastq_files):
					call_list.append(write_call(read_2_fastq_files, merged_read_2_fastq))
				else:
					logging.error('Differing number of FASTQ files between R1 and R2')
					sys.exit(1)

			for call in call_list:
				try:
					logging.info('Issuing system command: %s ' % call)
					subprocess.check_call( call, shell = True )			
				except subprocess.CalledProcessError:
					logging.error('The concatentation of the lane-specific fastq files failed somehow')
					sys.exit(1)
					


def create_project_structure(target_dir, bcl2fastq2_outdir, project_id, sample_dir_prefix):
	"""
	This creates the project and sample subdirectories within the target_dir (which is time-stamped by year+month).
	Extracted out from another method for easier unit testing
	"""
	orig_project_dir_path = os.path.join(bcl2fastq2_outdir, project_id)
	sample_dirs = [sd for sd in os.listdir(orig_project_dir_path) if sd.startswith(sample_dir_prefix)]
	logging.info('Creating directory for %s at %s' % (project_id, target_dir))
	new_project_dir = os.path.join(target_dir, project_id)
	try:
		os.mkdir(new_project_dir)
		logging.info('Created directory for project %s at %s' % (project_id, new_project_dir))
		for sample_dir in sample_dirs:
			sample_dir = os.path.join(new_project_dir, sample_dir)
			os.mkdir(sample_dir)
			logging.info('Created sample directory at %s' % sample_dir)
		# change the permissions for everything underneath this new directory (including the sample-specific directories)
		correct_permissions(new_project_dir)
	except OSError as ex:
		if ex.errno == 17: # directory was already there
			logging.info('Directory was already present.  Generally this should not occur, so something is likely wrong.')
			sys.exit(1)
		else:
			logging.error('There was an issue creating a directory at %s' % new_project_dir)
			sys.exit(1)



def create_final_locations(bcl2fastq2_outdir, projects_dir, project_id_list, sample_dir_prefix):
	"""
	This function creates a time-stamped directory (only year+month)
	and sets up the empty directory structure where the final merged FASTQ files will be
	"""	

	# for organizing projects by date:
        today = date.today()
        year = today.year
        month = today.month

        # double-check that the config file had a valid path to a directory
        if os.path.isdir(projects_dir):
                target_dir = os.path.join(projects_dir, str(year), str(month))

                # try to create the directory-- it may already exist, in which case we catch the exception and move on.
                # Any other errors encountered in creating the directory will cause pipeline to exit
                try:
                    	os.makedirs(target_dir)
			logging.info('Creating final directory for the projects at: %s' % target_dir)
			correct_permissions(target_dir)
                except OSError as ex:
                        if ex.errno != 17: # 17 indicates that the directory was already there.
                                logging.error('Exception occured:')
                                logging.error(ex.strerror)
                                sys.exit(1)
			else:
				logging.info('The final (date-stamped) directory at %s already existed.' % target_dir)
                # check that we do have a destination directory to go to.
                if os.path.isdir(target_dir):
                        for project_id in project_id_list:
				create_project_structure(target_dir, bcl2fastq2_outdir, project_id, sample_dir_prefix)
			return target_dir
                else:
                     	logging.error('Target directory %s does not exist for some reason.  Maybe permissions?' % target_dir)
                        sys.exit(1)




def correct_permissions(directory):
	"""
	Gives write privileges to the biocomp group (if not already there)
	Recurses through all directories and files underneath the path passed as the argument
	"""
	for root, dirs, files in os.walk(directory):
		for d in dirs:
			os.chmod(os.path.join(root, d), 0775)
		for f in files:
			os.chmod(os.path.join(root, f), 0775)



def run_fastqc(fastqc_path, target_directory, project_id_list):
	"""
	Finds all the fastq files in 'target_directory' and runs them through fastQC with a system call
	"""
	for project_id in project_id_list:
		project_dir = os.path.join(target_directory, project_id)
		fastq_files = []
		for root, dirs, files in os.walk(project_dir):
			for f in files:
				if f.lower().endswith('fastq.gz'):
					fastq_files.append(os.path.join(root, f))

		for fq in fastq_files:
			try:
				call_command = fastqc_path + ' ' + fq
				subprocess.check_call(call_command, shell = True)
			except subprocess.CalledProcessError:
				logging.error('The fastqc process on fastq file (%s) had non-zero exit status.  Check the log.' % fq)
				sys.exit(1)



def write_html_links(delivery_links, external_url, internal_drop_location):
	"""
	A helper method for writing an email-- composes html links
	delivery_links is a dict mapping the project_id to the data location
	"""
	template = '<p> #PROJECT#: <a href="#LINK#">#LINK#</a></p>'
	text = ''
	for project in delivery_links.keys():
		final_link = re.sub(internal_drop_location, external_url, delivery_links[project])
		this_text = re.sub('#LINK#', final_link, template)
		this_text = re.sub('#PROJECT#', project, this_text)
		text += this_text
	return text
	


def send_notifications(recipients, delivery_links, smtp_server_name, smtp_server_port, external_url, internal_drop_location):
	"""
	Sends an email with the data links to addresses in the 'recipients' arg.
	Note that the recipients arg is a comma-separated string parsed from the commandline
	"""

	# first, compose the message:
	body_text = '<html><head></head><body><p>The following projects have completed processing and are available:</p>'
	body_text += write_html_links(delivery_links, external_url, internal_drop_location)
	body_text += '</body></html>'

	# need a list of the email addresses:
	address_list = recipients.split(',') # this is the ACTUAL list of emails to send to
	address_to_string = ', '.join(address_list) # this is just for the mail header

	# this address gets .harvard.edu appended to it.  Doesn't really matter what this is
	fromaddr = 'nextseq@cccb'

	msg = MIMEMultipart('alternative')
	msg['Subject'] = '[NextSeq] Data processing complete'
	msg['From'] = fromaddr
	msg['To'] = address_to_string

	msg.attach(MIMEText(body_text, 'html'))
	try:
		server = smtplib.SMTP(smtp_server_name, smtp_server_port)
		server.sendmail(fromaddr, address_list, msg.as_string())
	except Exception:
		logging.error('There was an error composing the email to the recipients.')
		sys.exit(1)



def process():

	# kickoff the processing:
	run_directory_path, recipients = parse_commandline_args()

	# setup a logger that prints to stdout:
	root = logging.getLogger()
	root.setLevel(logging.INFO)
	ch = logging.StreamHandler(sys.stdout)
	ch.setLevel(logging.DEBUG)
	formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
	ch.setFormatter(formatter)
	root.addHandler(ch)

	# read the configuration file to get the constants:
	config_params_dict = parse_config_file()
	bcl2fastq2_path = config_params_dict.get('bcl2fastq2_path')
	bcl2fastq2_output_dir = config_params_dict.get('bcl2fastq2_output_dir')
	sample_dir_prefix = config_params_dict.get('sample_dir_prefix')
	final_destination_path = config_params_dict.get('destination_path')
	fastqc_path = config_params_dict.get('fastqc_path')
	smtp_server_name = config_params_dict.get('smtp_server')
	smtp_server_port = int(config_params_dict.get('smtp_port'))
	delivery_home = config_params_dict.get('delivery_home')
	external_url = config_params_dict.get('external_url')

	logging.info('Parameters parsed from configuration file: %s' % config_params_dict)

	# create the location where the demux process will drop the fastq files:
	output_directory_path = create_output_directory(run_directory_path, bcl2fastq2_output_dir)

	# parse out the projects and also do a check on the SampleSheet.csv to ensure the correct parameters have been entered
	project_id_list = check_samplesheet(run_directory_path, sample_dir_prefix)

	# actually start the demux process:
	run_demux(bcl2fastq2_path, run_directory_path, output_directory_path)

	# sets up the directory structure for the final, merged fastq files.
	target_directory = create_final_locations(output_directory_path, final_destination_path, project_id_list, sample_dir_prefix)

	# concatenate the fastq.gz files and place them in the final destination (NOT in the instrument directory)
	concatenate_fastq_files(output_directory_path, target_directory, project_id_list, sample_dir_prefix)

	# run the fastQC process:
	run_fastqc(fastqc_path, target_directory, project_id_list)

	# write the HTML output and create the delivery:
	delivery_links = publish(target_directory, project_id_list, sample_dir_prefix, delivery_home)	

	# send email to indicate processing is complete:
	if recipients:
		send_notifications(recipients, delivery_links, smtp_server_name, smtp_server_port, external_url, delivery_home)



if __name__ == '__main__':
	process()
