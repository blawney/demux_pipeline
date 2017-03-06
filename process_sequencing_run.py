#!/ifs/labs/cccb/projects/cccb/pipelines/demux_pipeline_current/venv/bin/python

import logging
import os
import sys
import re
import pipeline
from report.publish import publish
import smtplib
import datetime
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import subprocess

# names of the machines for global reference:
AVAILABLE_INSTRUMENTS = ['nextseq',]


def process():

	# kickoff the processing:
	run_directory_path, recipients, instrument, log_dir = parse_commandline_args()

	timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
	if log_dir:
		logfile = os.path.join(log_dir, str(timestamp)+".demux.log")
	else:
		logfile = os.path.join(run_directory_path, str(timestamp)+".demux.log")

	logging.basicConfig(filename=logfile, level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")

	# before starting anything- check that the cmdline arg was valid:
	if not os.path.isdir(run_directory_path):
		logging.error('The path to the run directory (%s) was not valid.' % run_directory_path)
		sys.exit(1)

	if instrument == 'nextseq':
		p = pipeline.NextSeqPipeline(run_directory_path)
	elif instrument == 'hiseq':
		logging.warning('HiSeq processing could be out of date due to decommission.')
		p = pipeline.HiSeqPipeline(run_directory_path)
	else:
		logging.error('Processing logic not implemented for this instrument.  Exiting')
		sys.exit(1)

	# run the processing steps (demux, fastQC)
	try:
		p.run()
	except Exception as ex:
		if isinstance(ex, pipeline.SampleSheetException):
			send_notifications(recipients, 
						p.config_params_dict.get('smtp_server'), 
						p.config_params_dict['smtp_port'],
						ex.message,
						error = True)
		else:
			# if it's not a samplesheet error, someone on the comp side should be fixing it, not the lab.
			# In that case, just exit with code 1 and the cron process that called this script will send an email
			# to the comp side
			sys.exit(1)


	# prepares the sample annotation file which is used in downstream analysis processes
	for project_id in p.project_id_list:
		original_project_dir = os.path.join(p.target_dir, project_id)
		sample_names = [s[len(p.config_params_dict.get('sample_dir_prefix')):] for s in os.listdir(original_project_dir) if s.startswith(p.config_params_dict.get('sample_dir_prefix'))]
		sample_filepath = os.path.join(original_project_dir, p.config_params_dict.get('default_sample_listing_filename'))
		with open(sample_filepath, 'w') as outfile:
			# for each sample, write the name and a dash (as a dummy for the annotation group)
			outfile.write('\n'.join(map(lambda x: str(x) + '\t-', sample_names)) + '\n') # trailing \n for the purpose of making wc -l operations 'correct'


	# also write out the projec to lane-specific mapping lane_specific_fastq_mapping
	fastq_mapping_filepath = os.path.join(original_project_dir, p.config_params_dict.get('fastq_lane_map_file'))
	with open(fastq_mapping_filepath, 'a') as outfile:
		project_mapping = p.lane_specific_fastq_mapping[project_id] # this gets a map of sample names pointing at lists of lane-specific fastq files
		for sample in project_mapping.keys():
			outfile.write('\n'.join([sample + '\t' + x for x in project_mapping[sample]]))
			outfile.write('\n')


	# write the HTML output and create the delivery:
	delivery_links = publish(p.target_dir, 
				p.project_id_list, 
				p.config_params_dict.get('sample_dir_prefix'), 
				p.config_params_dict.get('delivery_home'))	


	# send email to indicate processing is complete:
	if recipients:
		# compose the body text for the email
		body_text = '<html><head></head><body><p>The following projects have completed processing and are available:</p>'
		body_text += write_html_links(delivery_links, p.config_params_dict.get('external_url'), p.config_params_dict.get('delivery_home'))
		body_text += '</body></html>'
		send_notifications(recipients, 
				p.config_params_dict.get('smtp_server'), 
				p.config_params_dict.get('smtp_port'), 
				body_text)


	# kick off alignment processes, etc. for appropriately marked samples
	for project_id, target in p.project_to_targets:
		logging.info('Project %s was marked as having a downstream target to run: %s' % (project_id, target))
		process, genome = target.split(':')
		if process.lower() == 'rna':
			run_rnaseq_pipeline(p, project_id, genome)




def run_rnaseq_pipeline(p, project_id, genome):
	"""
	Pieces together the command to call out to the rnaseq pipeline.  That will perform an initial alignment, quantification, report, etc.
	Does NOT do any differential expression testing- that can be started manually at a later time.
	"""

	project_directory = os.path.join(p.target_dir, project_id)
	call = p.config_params_dict.get('rnaseq_pipeline_script')
	args = []
	args.append('run')
	args.extend(['-d', project_directory])
	args.extend(['-s', os.path.join(project_directory, p.config_params_dict.get('default_sample_listing_filename'))])
	args.extend(['-o', os.path.join(project_directory, 'rnaseq_pipeline_output')])
	args.extend(['-g', genome])
	args.append('-skip_analysis')
	arg_string = ' '.join(args)
	cmd = call + ' ' + arg_string

	logging.info('Call out to rna-seq pipeline script with: ')
	logging.info(cmd)
	process = subprocess.Popen(cmd, shell = True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
	stdout, stderr = process.communicate()
	if process.returncode != 0:
		logging.error('There was an error encountered during execution of the RNA-Seq pipeline for project %s ' % project_id)


def parse_commandline_args():
	"""
	This method parses the commandline arguments and returns a tuple of the passed args
	"""

	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('-r', 
				'--rundir', 
				required = True,
				help = 'The full path to the run directory',
				dest = 'run_directory')	
	parser.add_argument('-i', 
				'--instrument', 
				required = True,
				choices = AVAILABLE_INSTRUMENTS,
				help = 'The instrument used- use "-h" argument to see acceptable values.',
				dest = 'instrument')	
	parser.add_argument('-e', 
				'--email', 
				required = False,
				help = 'Comma-separated list of email addresses for notifications (no spaces between entries)',
				dest = 'recipients')

	parser.add_argument('-l',
				'--log',
				required = False,
				help = 'Directory in which to write the logfile.  Defaults to the run directory (-r) arg.',
				dest = 'log_dir')

	args = parser.parse_args()
	return (args.run_directory, args.recipients, args.instrument, args.log_dir)




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



def send_notifications(recipients, smtp_server_name, smtp_server_port, body_text, error = False):
	"""
	Sends an email with the data links to addresses in the 'recipients' arg.
	Note that the recipients arg is a comma-separated string parsed from the commandline
	"""

	# need a list of the email addresses:
	address_list = recipients.split(',') # this is the ACTUAL list of emails to send to
	address_to_string = ', '.join(address_list) # this is just for the mail header

	# this address gets .harvard.edu appended to it.  Doesn't really matter what this is
	fromaddr = 'sequencer@cccb'

	msg = MIMEMultipart('alternative')
	if error:
		msg['Subject'] = '[DEMUX] SampleSheet error'
	else:
		msg['Subject'] = '[DEMUX] Data processing complete'
	msg['From'] = fromaddr
	msg['To'] = address_to_string

	msg.attach(MIMEText(body_text, 'html'))

	not_sent = True
	attempts = 0
	max_attempts = 3
	server = smtplib.SMTP(smtp_server_name, smtp_server_port)
	while not_sent and attempts < max_attempts:
		try:
			server.sendmail(fromaddr, address_list, msg.as_string())
			not_sent = False
		except Exception as ex:
			attempts += 1
			logging.info("Type of exception: " + str(type(ex)))
			logging.info('There was an error composing the email to the recipients.  Trying again after sleeping')
			logging.info(ex.message)
			logging.exception("Error!")
			time.sleep(5)

	if not_sent:
		logging.error('After %s attempts, still could not send email.' % max_attempts)



if __name__ == '__main__':
	process()
