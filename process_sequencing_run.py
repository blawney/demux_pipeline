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
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# names of the machines for global reference:
AVAILABLE_INSTRUMENTS = ['nextseq', 'hiseq']

def parse_commandline_args():
	"""
	This method parses the commandline arguments
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

	args = parser.parse_args()
	return (args.run_directory, args.recipients, args.instruments)





















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
	run_directory_path, recipients, instrument = parse_commandline_args()

	# setup a logger that prints to stdout:
	root = logging.getLogger()
	root.setLevel(logging.INFO)
	ch = logging.StreamHandler(sys.stdout)
	ch.setLevel(logging.DEBUG)
	formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
	ch.setFormatter(formatter)
	root.addHandler(ch)

	if instrument == 'nextseq':
		pipeline = pipeline.NextSeqPipeline()
	elif instrument == 'hiseq':
		pipeline = pipeline.HiSeqPipeline()
	else:
		logging.error('Processing logic not implements for this instrument.  Exiting')
		sys.exit(1)

	pipeline.run()




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
