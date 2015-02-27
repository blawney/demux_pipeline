import re
import logging
import os
import sys
from datetime import datetime as date
from ConfigParser import SafeConfigParser


def parse_config_file():
	"""
	This function finds and parses a configuration file, which contains various constants 
	"""

	current_dir = os.path.dirname(os.path.realpath(__file__)) # this gets the directory of this script
	cfg_files = [f for f in os.listdir(current_dir) if f.endswith('cfg')]
	if len(cfg_files) == 1:
		parser = SafeConfigParser()
		parser.readfp(cfg_files[0])
		return parser.defaults()
	else:
		logging.error('There were %d config (*cfg) files found in %s.  Need exactly 1.  Correct this.' % (len(cfg_files), current_dir))
		sys.exit(1)



def create_delivery_locations(delivery_home, project_id_list):
	"""
	This function creates a time-stamped directory (only year+month) for the delivery locations
	"""	

	# for organizing projects by date:
        today = date.today()
        year = today.year
        month = today.month

        destination_dir = os.path.join(delivery_home, str(year), str(month))

        # try to create the directory-- it may already exist, in which case we catch the exception and move on.
        # Any other errors encountered in creating the directory will cause pipeline to exit
        try:
                os.makedirs(destination_dir)
		correct_permissions(destination_dir)
        except OSError as ex:
		if ex.errno != 17: # 17 indicates that the directory was already there.
		        logging.error('Exception occured:')
		        logging.error(ex.strerror)
		        sys.exit(1)
        # check that we do have a destination directory to go to.
        if os.path.isdir(destination_dir):
                for project_id in project_id_list:
			try:
				os.mkdir(os.path.join(destination_dir, project_id))
        		except OSError as ex:
				logging.error('Could not create the directory for the project %s inside %s' % (project_id, destination_dir))
				sys.exit(1)
		return destination_dir

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


def setup_links(date_stamped_delivery_dir, origin_dir, project_id_list, sample_dir_prefix, fastqc_output_suffix):

	for project_id in project_id_list:
		original_project_dir = os.path.join(origin_dir, project_id)
		final_destination = os.path.join(date_stamped_delivery_dir, project_id)
		fastq_files = glob.glob(os.path.join(original_project_dir, sample_dir_prefix + '*', '*.fastq.gz'))
		fastqc_dirs = glob.glob(os.path.join(original_project_dir, sample_dir_prefix + '*', '*'+fastqc_output_suffix))
		[os.symlink(fq, os.path.basename(fq)) for fq in fastq_files]
		[os.symlink(qc_dir, os.path.basename(qc_dir)) for qc_dir in fastqc_dirs]


def publish(origin_dir, project_id_list, sample_dir_prefix): 
	
	# read the configuration parameters
	parameters_dict = parse_config_file()

	delivery_home = parameters_dict.get('delivery_home')
	fastqc_output_suffix = parameters_dict.get('fastqc_output_suffix')

	# create the output directory (with the year + month).  Project dirs will be located underneath
	date_stamped_delivery_dir = create_delivery_locations(delivery_home, project_id_list)

	# link to the orignal files:
	setup_links(date_stamped_delivery_dir, origin_dir, project_id_list, sample_dir_prefix, fastqc_output_suffix)

	# write_html	


