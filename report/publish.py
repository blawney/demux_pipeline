import re
import logging
import os
import sys
import glob
import shutil
from datetime import datetime as date
from ConfigParser import SafeConfigParser


def parse_config_file(current_dir):
	"""
	This function finds and parses a configuration file, which contains various constants 
	"""
	cfg_files = [os.path.join(current_dir,f) for f in os.listdir(current_dir) if f.endswith('cfg')]
	if len(cfg_files) == 1:
		logging.info('Parsing report configuration file at %s' % cfg_files[0])
		parser = SafeConfigParser()
		parser.read(cfg_files[0])
		return parser.defaults()
	else:
		logging.error('There were %d config (*cfg) files found in %s.  Need exactly 1.  Correct this.' % (len(cfg_files), current_dir))
		sys.exit(1)



def create_delivery_locations(delivery_home, project_id_list):
	"""
	This function creates a time-stamped directory (only year+month) for the delivery locations.
	It also attempts to create subdirectories for each of the projects.  Each of those directories will eventually
	contain symlinks to the project files.

	Returns a path to the time-stamped delivery directory
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
		logging.info('Created a time-stamped destination directory at %s ' % destination_dir)
		# to give the correct permissions to the full hierarchy of files, have to correct the permissions from the 'year'-level directory.
		# correcting from the 'month' level directory could leave a new 'year'-level directory without the correct permissions.
		# Not a big deal if this weren't implemented since this case is only encountered for the first project of a new year.
		correct_permissions(os.path.realpath(os.path.join(destination_dir,os.path.pardir)))
        except OSError as ex:
		if ex.errno != 17: # 17 indicates that the directory was already there.
		        logging.error('Exception occured:')
		        logging.error(ex.strerror)
		        sys.exit(1)
		else:
			logging.info('The directory %s already existed. ' % destination_dir)
        # check that we do have a destination directory to go to.
        if os.path.isdir(destination_dir):
                for project_id in project_id_list:
			try:
				new_project_dir = os.path.join(destination_dir, project_id)
				os.mkdir(new_project_dir)
				correct_permissions(new_project_dir)
				logging.info('Created a project directory at %s' % os.path.join(destination_dir, project_id))
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
	logging.info('Giving 0775 permissions to all directories underneath %s' % directory)
	for root, dirs, files in os.walk(directory):
		for d in dirs:
			os.chmod(os.path.join(root, d), 0775)
		for f in files:
			os.chmod(os.path.join(root, f), 0775)



def setup_links(date_stamped_delivery_dir, origin_dir, project_id_list, sample_dir_prefix, fastqc_output_suffix):
	"""
	Creates symbolic links to the fastq and fastQC output directories from the project delivery location
	Returns a dictionary with the project id mapping to a list of the sample names.
	"""

	logging.info('Creating symlinks to the project files (fastq, fastQC)')
	project_to_sample_mappings = {}
	for project_id in project_id_list:
		original_project_dir = os.path.join(origin_dir, project_id)
		sample_names = [s.strip(sample_dir_prefix) for s in os.listdir(original_project_dir) if s.startswith(sample_dir_prefix)]
		final_destination = os.path.join(date_stamped_delivery_dir, project_id)
		fastq_files = glob.glob(os.path.join(original_project_dir, sample_dir_prefix + '*', '*.fastq.gz'))
		fastqc_dirs = glob.glob(os.path.join(original_project_dir, sample_dir_prefix + '*', '*'+fastqc_output_suffix))
		[os.symlink(fq, os.path.join(final_destination, os.path.basename(fq))) for fq in fastq_files]
		[os.symlink(qc_dir, os.path.join(final_destination, os.path.basename(qc_dir))) for qc_dir in fastqc_dirs]
		project_to_sample_mappings[project_id] = sample_names
	logging.info('Found the following project to sample mappings: ')
	logging.info(project_to_sample_mappings)
	return project_to_sample_mappings



def extract_section(text, regex_pattern):
	"""
	Uses the passed regex pattern to search within 'text'.  Returns the extracted section
	"""
	match = re.findall(regex_pattern, text, re.DOTALL)
	if len(match) == 1:
		return match[0]
	else:
		logging.error('Could not find the pattern %s in the text block.' % regex_pattern)
		sys.exit(1)



def fill_link_section(files, template, operation = lambda x: x):
	"""
	Fills in the the template for anchor <a> HTML elements.  
	'files' is a list of file paths (these go in the href='' part of the <a> element)
	'template' is a text string for the <a> element that we will fill-in with the correct links/names
	'operation' is a function that tells how to 'map' the file path (in general, a path to the resource) into
		a name to display.  This way, long links, etc. can be translated to simple text to display as the link text
		By default, it makes the link text and href the same.  

	Returns a long string with all the <a> elements for all the files 
	"""
	final_text = ''
	for f in files:
		new_text = template
		new_text = re.sub('#LINK#', f, new_text)
		new_text = re.sub('#LINK_TEXT#', operation(f), new_text)
		final_text += new_text
	return final_text



def replace_section(body_text, new_text, section_id):
	"""
	Uses a regex to find a block of text enclosed/delimited by our special tag (a HTML comment)
	Replaces that block with 'new_text' and returns the result
	"""
	regex_pattern = '<!--\s*'+str(section_id)+'.*'+str(section_id)+'\s*-->'
	return re.sub(regex_pattern, new_text, body_text, flags = re.DOTALL)



def write_reports(report_template_dir, date_stamped_delivery_dir, parameters_dict, project_to_sample_mappings):
	"""
	The main method for orchestrating the writing of the HTML delivery page.
	"""

	project_delivery_links = {}

	# try to read the HTML template which we will fill in
	try:
		html_template_string = open(os.path.join(report_template_dir, parameters_dict.get('html_template'))).read()
	except IOError:
		logging.error('Could not find the html template file: %s' % parameters_dict.get('html_template'))
		sys.exit(1)

	
	# pull out the template sections-- we will use these templates to fill-in the final HTML page.
	menu_section_template = extract_section(html_template_string, '<!-- \s*#SAMPLE_MENU#.*#SAMPLE_MENU#\s*-->')
	file_link_template = extract_section(html_template_string, '<!-- \s*#FILE_LINK_SECTION#.*#FILE_LINK_SECTION#\s*-->')
	fastqc_report_link_template = extract_section(html_template_string, '<!-- \s*#FASTQC_REPORT_LINK_SECTION#.*#FASTQC_REPORT_LINK_SECTION#\s*-->')

	for project_id in project_to_sample_mappings.keys():
		project_delivery_dir = os.path.join(date_stamped_delivery_dir, project_id)

		# make a copy of the template for this project:
		html_report = html_template_string

		all_sample_sections = ''
		for sample in project_to_sample_mappings[project_id]:

			# for each of the samples in a particular project, we create a 'section' which is a menu containing links to the fastq and fastQC files
			sample_section = menu_section_template # copy
			sample_section = re.sub('#SAMPLE_ID#', sample, sample_section)

			# get the files and their relative paths:
			fastq_files = glob.glob(os.path.join(project_delivery_dir, str(sample) + '*.fastq.gz'))
			fastq_files = [os.path.relpath(p, project_delivery_dir) for p in fastq_files]
			fastqc_report_files = glob.glob(os.path.join(project_delivery_dir, str(sample)+ '*', parameters_dict.get('fastqc_html_report')))
			fastqc_report_files = [os.path.relpath(p, project_delivery_dir) for p in fastqc_report_files]

			# with those file links, create the necessary <a> elements for the links to the fastq.gz files:
			fastq_links_html = fill_link_section(fastq_files, file_link_template)

			# link to the fastQC reports-- note the inline method to convert the relative path into a meaningful link name
			fastqc_output_suffix = parameters_dict.get('fastqc_output_suffix')
			fastqc_report_links_html = fill_link_section(fastqc_report_files, fastqc_report_link_template, lambda g: os.path.dirname(g).strip(fastqc_output_suffix)+' FastQC')
			
			# for this sample, inject in those links to the relevant files:
			sample_section = replace_section(sample_section, fastq_links_html, '#FILE_LINK_SECTION#')			
			sample_section = replace_section(sample_section, fastqc_report_links_html, '#FASTQC_REPORT_LINK_SECTION#')			

			# add this section to the 'master' string for all of the samples in this project
			all_sample_sections += sample_section


		# now, fill in the samples corresponding to the project:
		html_report = replace_section(html_report, all_sample_sections, '#SAMPLE_MENU#')

		# write the html report into the delivery directory:
		final_html_report = os.path.join(project_delivery_dir, parameters_dict.get('output_html_report'))
		with open(final_html_report, 'w') as outfile:
			outfile.write(html_report)			
		logging.info('Final delivery location for project %s is at %s' % (project_id, final_html_report))
		project_delivery_links[project_id] = final_html_report
	return project_delivery_links



def copy_libraries(report_directory, lib_dirname, date_stamped_delivery_dir, project_id_list):
	"""
	Copies the necessary css/js/etc libraries to each project directory
	'report_directory' refers to the directory of THIS file.  
	"""
	library_location = os.path.join(report_directory, lib_dirname)
	final_project_dirs = [os.path.join(date_stamped_delivery_dir, d) for d in project_id_list]
	[shutil.copytree(library_location, os.path.join(fpd, lib_dirname)) for fpd in final_project_dirs]
	logging.info('Copying js/css/etc. libraries from %s' % library_location)



def publish(origin_dir, project_id_list, sample_dir_prefix, delivery_home): 
	
	current_dir = os.path.dirname(os.path.realpath(__file__)) # this gets the directory of this script

	# read the configuration parameters
	parameters_dict = parse_config_file(current_dir)
	fastqc_output_suffix = parameters_dict.get('fastqc_output_suffix')

	# create the output directory (with the year + month).  Project dirs will be located underneath
	date_stamped_delivery_dir = create_delivery_locations(delivery_home, project_id_list)

	# link to the orignal files:
	project_to_sample_map = setup_links(date_stamped_delivery_dir, origin_dir, project_id_list, sample_dir_prefix, fastqc_output_suffix)

	# copy the necessary libraries for the HTML report
	copy_libraries(current_dir, parameters_dict.get('libraries'), date_stamped_delivery_dir, project_id_list)

	# create the HTML reports
	project_links = write_reports(current_dir, date_stamped_delivery_dir, parameters_dict, project_to_sample_map)	
	
	return project_links

