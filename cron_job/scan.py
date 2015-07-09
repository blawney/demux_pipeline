#!/cccbstore-rc/projects/cccb/apps/python27/bin/python

import os
import sys
from ConfigParser import SafeConfigParser
import subprocess
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import datetime


def send_error_email(subscribers, flowcell_directory, server, port):
	"""
	Sends an email with the data links to addresses in the 'subscribers' arg.
	"""

	# first, compose the message:
	body_text = '<html><head></head><body><p>The following flowcell directory experienced an error during demultiplexing and needs to be checked manually:</p>'
	body_text += flowcell_directory
	body_text += '</body></html>'

	# need a list of the email addresses:
	address_to_string = ', '.join(subscribers) # this is just for the mail header

	# this address gets .harvard.edu appended to it.  Doesn't really matter what this is
	fromaddr = 'sequencer@cccb'

	msg = MIMEMultipart('alternative')
	msg['Subject'] = '[DEMUX] Error report'
	msg['From'] = fromaddr
	msg['To'] = address_to_string

	msg.attach(MIMEText(body_text, 'html'))

	server = smtplib.SMTP(server, port)
	server.sendmail(fromaddr, subscribers, msg.as_string())


def parse_vals(val_string):
	vals = val_string.split(',')
	if len(vals) > 1:
		return [v.strip() for v in vals]
	else:
		return val_string


def parse_config():
	"""
	This function finds and parses a configuration file, which contains various constants to run this pipeline
	"""
	current_dir = os.path.dirname(os.path.realpath(__file__)) # this gets the directory of this script
	cfg_files = [os.path.join(current_dir, f) for f in os.listdir(current_dir) if f.endswith('cfg')]
	if len(cfg_files) == 1:
		parser = SafeConfigParser()
		parser.read(cfg_files[0])
		config_params = parser.defaults()
		config_params = {k:parse_vals(config_params[k]) for k in config_params.keys()}
		return config_params
	else:
		sys.exit(1)


def create_progress_file(f, command):
	with open(f, 'a') as out:
		out.write('Started at %s \n' % datetime.datetime.now().strftime("%Y%m%d_%H%M"))
		out.write(command)


def main(params):
	# Get the absolute path to this directory
	this_dir = os.path.dirname(os.path.abspath(__file__))

	instrument_dirs = params.get('instrument_dirs')

	# if only checking a single directory, this is a string-- to use properly in a 'for' loop
	# make this into a list:
	if isinstance(instrument_dirs, str):
		instrument_dirs = [instrument_dirs,]

	# iterate through the instruments, collecting the paths:
	all_flowcell_dirs = []
	for instrument in instrument_dirs:
		all_flowcell_dirs += [os.path.join(instrument,d) for d in os.listdir(instrument)]
	
	# filter to retain only directories	
	all_flowcell_dirs = set(filter(lambda x: os.path.isdir(x), all_flowcell_dirs))

	# Load the pipeline_cache file, which tells us which directories have already been processed
	CACHE = os.path.join(this_dir, params.get('cache_file'))
	processed_flowcells = set([d.strip() for d in open(CACHE)])

	# resolve symlinks, etc. by using realpath method:
	all_flowcell_dirs = set(map(os.path.realpath, all_flowcell_dirs))
	processed_flowcells = set(map(os.path.realpath, processed_flowcells))

	# Detect new/unprocessed directories by looking at the set difference:
	unprocessed_dirs = all_flowcell_dirs.difference(processed_flowcells)

	# have to handle whether the config file had a list of just a single item-- we ultimately need a list to pass to
	# the notification/email methods, so have to convert strings to a single-item list.
	converter = lambda x: [x] if isinstance(x, str) else x
	subscribers = converter(params.get('subscribers'))
	comp_subscribers = converter(params.get('comp_subscribers'))

	for d in unprocessed_dirs:
		progress_file = os.path.join(d, params.get('in_progress_file'))
		if os.path.isfile(os.path.join(d, params.get('target_file'))) and not os.path.isfile(progress_file):
			command = os.path.realpath(os.path.join(this_dir, os.pardir, params.get('demux_script'))) + ' '
			args = ['-r', d, '-i', 'nextseq', '-e', ','.join(subscribers)]
			command += ' '.join(args)
			create_progress_file(progress_file, command)
			os.chmod(progress_file, 0775)
			process = subprocess.Popen(command, shell = True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
			stdout, stderr = process.communicate()
			if process.returncode == 0:
				processed_flowcells.add(d)
			else:
				send_error_email(comp_subscribers, d, params.get('smtp_server'), params.get('smtp_port'))
			
			
	with open(CACHE, 'w') as cache_file:
		cache_file.write('\n'.join(processed_flowcells))

		

if __name__ == '__main__':
	config_params = parse_config()
	main(config_params)
