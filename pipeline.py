import os
import sys
import re
import glob
import shutil
import logging
from datetime import datetime as date
from ConfigParser import SafeConfigParser
import subprocess


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


class Pipeline(object):

	def __init__(self):
		pass


	def parse_config_file(self):
		"""
		This function finds and parses a configuration file, which contains various constants to run this pipeline
		"""
		current_dir = os.path.dirname(os.path.realpath(__file__)) # this gets the directory of this script
		cfg_files = [os.path.join(current_dir, f) for f in os.listdir(current_dir) if f.endswith('cfg')]
		if len(cfg_files) == 1:
			logging.info('Parsing configuration file: %s' % cfg_files[0])
			parser = SafeConfigParser()
			parser.read(cfg_files[0])

			self.config_params_dict = {}
			for opt in parser.options(self.instrument):
				value = parser.get(self.instrument, opt)
				self.config_params_dict[opt] = value
			logging.info('Parameters parsed from configuration file: ')
			logging.info(self.config_params_dict)
		else:
			logging.error('There were %d config (*cfg) files found in %s.  Need exactly 1.  Correct this.' % (len(cfg_files), current_dir))
			sys.exit(1)


	def create_output_directory(self):
		"""
		This method takes a path to the sequencing run directory and attempts to create an output
		directory for the bcl2fastq2 output.  If it cannot (due to write permissions, etc)
		then it exits the script and issues a message
		"""

		if os.path.isdir(self.run_directory_path):
			try:
				output_directory_path = os.path.join(self.run_directory_path, self.config_params_dict.get('demux_output_dir'))
				logging.info('Creating output directory for demux process at %s' % output_directory_path)			
				os.mkdir(output_directory_path)
				correct_permissions(output_directory_path)
				self.config_params_dict['demux_output_dir'] = output_directory_path
			except:
				logging.error('An exception occurred when attempting to create the output directory at: %s' % output_directory_path)
				sys.exit(1)
		else:
			logging.error('The path supplied as the run directory is not actually a directory.  Please check.')
			sys.exit(1)


	def execute_call(self, call_command):
		"""
		This executes the call passed via the call_command the argument.  Catches any errors in the process via the system exit code
		"""
		try:
			logging.info('Executing the following call to the shell:\n %s ' % call_command)
			subprocess.check_call(call_command, shell = True)
		except subprocess.CalledProcessError:
			logging.error('The called process had non-zero exit status.  Check the log.')
			sys.exit(1)


	def create_project_structure(self, project_id):
		"""
		This creates the project and sample subdirectories within the time-stamped destination directory.
		"""
		orig_project_dir_path = os.path.join(self.config_params_dict.get('demux_output_dir'), project_id)
		sample_dirs = [sd for sd in os.listdir(orig_project_dir_path) if sd.startswith(self.config_params_dict.get('sample_dir_prefix'))]
		logging.info('Creating directory for %s at %s' % (project_id, self.target_dir))
		new_project_dir = os.path.join(self.target_dir, project_id)
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


	def create_final_locations(self):
		"""
		This function creates a time-stamped directory (only year+month)
		and sets up the empty directory structure where the final FASTQ files will be
		"""	

		# for organizing projects by date:
		today = date.today()
		year = today.year
		month = today.month

		# double-check that the config file had a valid path to a directory
		if os.path.isdir(self.config_params_dict.get('destination_path')):
			target_dir = os.path.join(self.config_params_dict.get('destination_path'), str(year), str(month))

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
				self.target_dir = target_dir
				for project_id in self.project_id_list:
					Pipeline.create_project_structure(self, project_id)
			else:
				logging.error('Target directory %s does not exist for some reason.  Maybe permissions?' % target_dir)
				sys.exit(1)
		else:
			logging.error('The path supplied as the final destination base directory is not, in fact, a directory')
			sys.exit(1)



	def run_fastqc(self):
		"""
		Finds all the fastq files in 'target_directory' and runs them through fastQC with a system call
		"""
		for project_id in self.project_id_list:
			project_dir = os.path.join(self.target_dir, project_id)
			fastq_files = []
			for root, dirs, files in os.walk(project_dir):
				for f in files:
					if f.lower().endswith('fastq.gz'):
						fastq_files.append(os.path.join(root, f))

			for fq in fastq_files:
				try:
					call_command = self.config_params_dict['fastqc_path'] + ' ' + fq
					subprocess.check_call(call_command, shell = True)
				except subprocess.CalledProcessError:
					logging.error('The fastqc process on fastq file (%s) had non-zero exit status.  Check the log.' % fq)
					sys.exit(1)







class NextSeqPipeline(Pipeline):
	
	def __init__(self, run_directory_path):
		self.run_directory_path = run_directory_path
		self.instrument = 'nextseq'

	def run(self):
		Pipeline.parse_config_file(self)
		Pipeline.create_output_directory(self)

		# parse out the projects and also do a check on the SampleSheet.csv to ensure the correct parameters have been entered
		self.check_samplesheet()

		# actually start the demux process:
		self.run_demux()
	
		# sets up the directory structure for the final, merged fastq files.
		Pipeline.create_final_locations(self)

		# the NextSeq has each sample in multiple lanes- concat those
		self.concatenate_fastq_files()
	
		# run the fastQC process:
		Pipeline.run_fastqc(self)


	def run_demux(self):
		"""
		This writes the call for the demux process.  Uses the parent to execute the actual process.
		"""
		call_command = self.config_params_dict['demux_path'] + ' --output-dir ' + self.config_params_dict['demux_output_dir'] + ' --runfolder-dir ' + self.run_directory_path
		Pipeline.execute_call(self, call_command)


	def line_is_valid(self, line):
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
			tests.append(elements[0].startswith(self.config_params_dict.get('sample_dir_prefix'))) # check that the Sample_ID field has the proper prefix
			tests.append(elements[0].strip(self.config_params_dict.get('sample_dir_prefix')) == elements[1]) # check that the sample names match between the first and second fields
			# tests.append(len(elements[5]) == 7) # check that 7bp index was provided
			tests.append(pattern.match(elements[6]).group() == elements[6]) # if the greedy regex did not match the full project designation then something is wrong.
			return all(tests)
		except Exception:
			return False



	def check_samplesheet(self):
		"""
		This method checks for a samplesheet and does a quick check that it is formatted within guidelines (described elsewhere)
		Returns a list of the Project identifiers.
		"""
		samplesheet_path = os.path.join(self.run_directory_path, 'SampleSheet.csv')
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
			lines_valid = [self.line_is_valid(line) for line in annotation_lines]
			if not all(lines_valid):
				problem_lines = [i+1 for i,k in enumerate(lines_valid) if k == False]
				logging.error('There was some problem with the SampleSheet.csv')
				logging.error('Problem lines: %s' % problem_lines)
				sys.exit(1)

			# now get a list of all the projects that we are processing in this sampling run:
			project_id_list = list(set([line.split(',')[6] for line in annotation_lines]))

			logging.info('The following projects were identified from the SampleSheet.csv: %s' % project_id_list)
			self.project_id_list = project_id_list
		else:
			logging.error('Could not locate a SampleSheet.csv file in %s ' % self.run_directory_path)
			sys.exit(1)



	def concatenate_fastq_files(self):
		"""
		This method scans the output and concatenates the fastq files for each sample and read number.
		That is, the NextSeq output (in general) has a fastq file for each lane and sample and we need to 
		concatenate the lane files into a single fastq file for that sample.  For paired-end protocol, need
		to do this for both the R1 and R2 reads.
		"""

		for project_id in self.project_id_list:

			project_dir = os.path.join(self.config_params_dict.get('demux_output_dir'), project_id)

			# get the sample-specific subdirectories and append the path to the project directory in front (for full path name):
			# this gives the fully qualified location of the original fastq files (by lane)
			sample_dirs = [os.path.join(project_dir, d) for d in os.listdir(project_dir) if d.startswith(self.config_params_dict.get('sample_dir_prefix'))] 

			# double check that they are actually directories:
			sample_dirs = filter( lambda x: os.path.isdir(x), sample_dirs)

			for sample_dir in sample_dirs:
				# since bcl2fastq2 renames the fastq files with a different scheme, extract the sample name we want via parsing the directory name
				sample_name_with_prefix = os.path.basename(sample_dir)
				sample_name = sample_name_with_prefix.strip(self.config_params_dict.get('sample_dir_prefix'))

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
				merged_read_1_fastq = os.path.join(self.target_dir, project_id, sample_name_with_prefix, merged_read_1_fastq)
				merged_read_2_fastq = os.path.join(self.target_dir, project_id, sample_name_with_prefix, merged_read_2_fastq)

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
					



class HiSeqPipeline(Pipeline):

	def __init__(self, run_directory_path):
		self.run_directory_path = run_directory_path
		self.instrument = 'hiseq'

	def run(self):
		Pipeline.parse_config_file(self)
		self.config_params_dict['demux_output_dir'] = os.path.join(self.run_directory_path, self.config_params_dict.get('demux_output_dir'))

		# parse out the projects and also do a check on the SampleSheet.csv to ensure the correct parameters have been entered
		self.check_samplesheet()

		# actually start the demux process:
		#self.run_demux()
	
		# sets up the directory structure for the final fastq files.
		Pipeline.create_final_locations(self)

		# concatenate the fastq files
		self.concat_fastq_files()

		# move the fastq files to the final location:
		self.move_other_files()
	
		# run the fastQC process:
		Pipeline.run_fastqc(self)



	def check_samplesheet(self):

		samplesheet_path = os.path.join(self.run_directory_path, 'SampleSheet.csv')
		logging.info('Examining samplesheet at: %s' % samplesheet_path)

		if os.path.isfile(samplesheet_path):
			self.samplesheet_path = samplesheet_path
			# parse out the list of project IDs and store in a set
			project_set = set()
			with open(samplesheet_path) as ss_file:
				for i, line in enumerate(ss_file):
					if i > 0:
						project_set.add(line.strip().split(',')[9])

			project_id_list = list(project_set)

			# bcl2fastq software automatically appends a prefix to the 'project' field.  
			# e.g. If field 10 (index 9) is 'XXX', then bcl2fastq makes a directory called 'Project_XXX'
			project_id_list = [self.config_params_dict['project_prefix'] + x for x in project_id_list]
			logging.info('The following projects were identified from the SampleSheet.csv: %s' % project_id_list)
			self.project_id_list = project_id_list

		else:
			logging.error('Could not locate a SampleSheet.csv file in %s ' % self.run_directory_path)
			sys.exit(1)


	def run_demux(self):
		"""
		This writes the call for the demux process.  Uses the parent object to execute the actual process.
		"""
		# the run directory was passed via commandline.  Append the default path to the BaseCalls directory:
		input_dir = os.path.join(self.run_directory_path, self.config_params_dict['basecalls_dir_rel_path'])
		if os.path.isdir(input_dir):

			# execute the perl script to set everything up:
			call_command = self.config_params_dict['demux_path'] + ' --output-dir ' + self.config_params_dict['demux_output_dir'] + ' --input-dir ' + input_dir + ' --sample-sheet ' + self.samplesheet_path + ' --mismatches 1 '
			Pipeline.execute_call(self, call_command)
				
			try:
				i = int(self.config_params_dict['demux_jobs'])
			except ValueError:
				logging.info('The value for "demux_jobs" given in the configuration file needs to be an integer.  Ignoring and setting "-j 8" by default.')
				self.config_params_dict['demux_jobs'] = '8' # an integer given as a string (since it will be concatenated to a string object below).

			# create the make command for starting the actual demux.  Pass the -C flag so we do not have to change directories, etc.  
			call_command = 'make -C ' + self.config_params_dict['demux_output_dir'] + ' -j ' + self.config_params_dict['demux_jobs']
			Pipeline.execute_call(self, call_command)

		else:
			logging.error('The HiSeq run output is non-standard-- the BaseCalls directory was supposed to be located at: %s' % input_dir)
			sys.exit(1)


	def move_other_files(self):
		"""
		Move other files (not fastq files) created by demux process into a final destination
		"""
		for project_id in self.project_id_list:
			orig_project_dir_path = os.path.join(self.config_params_dict.get('demux_output_dir'), project_id)
			orig_samplesheet_files = glob.glob(os.path.join(orig_project_dir_path, '*', 'SampleSheet.csv'))
			dest_samplesheet_files = [re.sub(self.config_params_dict.get('demux_output_dir'), self.target_dir, x) for x in orig_samplesheet_files]
			for i in range(len(orig_samplesheet_files)):
				shutil.copyfile(orig_samplesheet_files[i], dest_samplesheet_files[i])



	def concat_fastq_files(self):
		'''
		Merges the (many) fastq files in the demux output directory.  Concatenates them to the final location (e.g. not in the demux dir)
		'''

		for project_id in self.project_id_list:

			project_dir = os.path.join(self.config_params_dict.get('demux_output_dir'), project_id)

			# get the sample-specific subdirectories and append the path to the project directory in front (for full path name):
			# this gives the fully qualified location of the original fastq files (by lane)
			sample_dirs = [os.path.join(project_dir, d) for d in os.listdir(project_dir) if d.startswith(self.config_params_dict.get('sample_dir_prefix'))] 

			# double check that they are actually directories:
			sample_dirs = filter( lambda x: os.path.isdir(x), sample_dirs)

			for sample_dir in sample_dirs:
				# since bcl2fastq renames the fastq files with a different scheme, extract the sample name we want via parsing the directory name
				sample_name_with_prefix = os.path.basename(sample_dir)
				sample_name = sample_name_with_prefix.strip(self.config_params_dict.get('sample_dir_prefix'))

				# get all the fastq files as lists.  Note the sort so they are concatenated in the same order for paired-end protocol
				# in future, may want to update to regex-- perhaps it is possible that edge cases could glob incorrectly?
				read_1_fastq_files = sorted(glob.glob(os.path.join(sample_dir, '*_R1_*.fastq.gz')))			
				read_2_fastq_files = sorted(glob.glob(os.path.join(sample_dir, '*_R2_*.fastq.gz'))) # will be empty list [] if single-end protocol

				# need at least the read 1 files to continue
				if len(read_1_fastq_files) == 0:
					logging.error('Did not find any fastq files in %s directory.' % sample_dir)
					sys.exit(1)			

				# make file names for the merged files:
				merged_read_1_fastq = sample_name + '_R1_001.fastq.gz'
				merged_read_2_fastq = sample_name + '_R2_001.fastq.gz'

				# construct full paths to the final files:
				merged_read_1_fastq = os.path.join(self.target_dir, project_id, sample_name_with_prefix, merged_read_1_fastq)
				merged_read_2_fastq = os.path.join(self.target_dir, project_id, sample_name_with_prefix, merged_read_2_fastq)

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
						logging.error('The concatentation of the segmented fastq files failed somehow')
						sys.exit(1)
					










