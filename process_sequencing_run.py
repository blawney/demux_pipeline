#!/cccbstore-rc/projects/cccb/apps/python27/bin/python

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

# names of the machines for global reference:
AVAILABLE_INSTRUMENTS = ['nextseq', 'hiseq']


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
		p = pipeline.HiSeqPipeline(run_directory_path)
	else:
		logging.error('Processing logic not implemented for this instrument.  Exiting')
		sys.exit(1)

	# run the processing steps (demux, fastQC)
	p.run()


	# write the HTML output and create the delivery:
	delivery_links = publish(p.target_dir, 
				p.project_id_list, 
				p.config_params_dict.get('sample_dir_prefix'), 
				p.config_params_dict.get('delivery_home'))	


	# send email to indicate processing is complete:
	if recipients:
		send_notifications(recipients, delivery_links, 
				p.config_params_dict.get('smtp_server'), 
				p.config_params_dict.get('smtp_port'), 
				p.config_params_dict.get('external_url'), 
				p.config_params_dict.get('delivery_home'))




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
	fromaddr = 'sequencer@cccb'

	msg = MIMEMultipart('alternative')
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
