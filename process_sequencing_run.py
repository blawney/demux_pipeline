#!/cccbstore-rc/projects/cccb/apps/Python-2.7.1/python

import logging
import sys
import re
from report.publish import publish
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# names of the machines for global reference:
AVAILABLE_INSTRUMENTS = ['nextseq', 'hiseq']


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

	# before starting anything- check that the cmdline arg was valid:
	if not os.path.isdir(run_directory_path):
		logging.error('The path to the run directory (%s) was not valid.' % run_directory_path)
		sys.exit(1)

	if instrument == 'nextseq':
		pipeline = pipeline.NextSeqPipeline(run_directory_path)
	elif instrument == 'hiseq':
		pipeline = pipeline.HiSeqPipeline(run_directory_path)
	else:
		logging.error('Processing logic not implemented for this instrument.  Exiting')
		sys.exit(1)

	# run the processing steps (demux, fastQC)
	pipeline.run()


	# write the HTML output and create the delivery:
	delivery_links = publish(pipeline.target_dir, 
				pipeline.project_id_list, 
				pipeline.config_params_dict.get('sample_dir_prefix'), 
				pipeline.config_params_dict.get('delivery_home'))	


	# send email to indicate processing is complete:
	if recipients:
		send_notifications(recipients, delivery_links, 
				pipeline.config_params_dict.get('smtp_server_name'), 
				pipeline.config_params_dict.get('smtp_server_port'), 
				pipeline.config_params_dict.get('external_url'), 
				pipeline.config_params_dict.get('delivery_home'))




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

	args = parser.parse_args()
	return (args.run_directory, args.recipients, args.instruments)




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
	try:
		server = smtplib.SMTP(smtp_server_name, smtp_server_port)
		server.sendmail(fromaddr, address_list, msg.as_string())
	except Exception:
		logging.error('There was an error composing the email to the recipients.')
		sys.exit(1)




if __name__ == '__main__':
	process()
