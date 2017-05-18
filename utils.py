import sys
import os
import datetime
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from ConfigParser import SafeConfigParser
import logging


class DatabaseParseException(Exception):
	pass


class MalformattedDateException(Exception):
	pass


def send_notifications(recipients, smtp_server_name, smtp_server_port, subject, body_text):
	"""
	Sends an email with the data links to addresses in the 'recipients' arg.
	Note that the recipients arg is a comma-separated string
	"""

	# need a list of the email addresses:
	address_list = recipients.split(',') # this is the ACTUAL list of emails to send to
	address_to_string = ', '.join(address_list) # this is just for the mail header

	# this address gets .harvard.edu appended to it.  Doesn't really matter what this is
	fromaddr = 'sequencer@cccb'

	msg = MIMEMultipart('alternative')
	msg['Subject'] = subject
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


def parse_config_file(section):
	"""
	This function parses a configuration file and returns a dictionary 
	The 'section' argument tells which section to parse out of a configuration file.  For example, if you pass 'FOO',
	then it will find the section marked with [FOO] in the configuration file.  Note that when you do this, it ALSO returns
	the parameters listed under the [DEFAULT] section.
	"""
	current_dir = os.path.dirname(os.path.realpath(__file__)) # this gets the directory of this script
	cfg_files = [os.path.join(current_dir, f) for f in os.listdir(current_dir) if f.endswith('cfg')]
	logging.info('Configuration files %s' % cfg_files)
	if len(cfg_files) == 1:
		logging.info('Single configuration file found: %s' % cfg_files[0])
		parser = SafeConfigParser()
		parser.read(cfg_files[0])

		config_params_dict = {}
		logging.info('Looking to parse configuration section %s' % section)
		if section in parser.sections():
			for opt in parser.options(section):
				value = parser.get(section, opt)
				try:
					# if no comma is found, then it will throw an exception.  
					value.index(',')
					value = tuple([v.strip() for v in value.split(',') if len(v.strip()) > 0])
					config_params_dict[opt] = value
				except ValueError as ex:
					config_params_dict[opt] = value
			return config_params_dict
		else:
			logging.error('The section [%s] was not found in the configuration file at %s' % (section, cfg_files[0]))
			sys.exit(1)		
	else:
		logging.error('There were multiple configuration files.  Exiting')
		sys.exit(1)


def load_database(db_file, params):
	"""
	We read the formatted file and return a dictionary with the following structure:
	project id maps to a dictionary with the bucket name and client emails as a list.
	"""
	logging.info('Parsing database file at %s' % db_file)	
	db = {}
	for line in open(db_file):
		if len(line.strip()) > 0:
			try:
				ilab_id, bucket_name, client_emails, target_date_str = line.strip().split('\t')
			except Exception as ex:
				logging.error('Problem with line: (%s)' % line)
				raise DatabaseParseException('Error when parsing database.  Specific exception was %s' % ex.message)
			# convert the datestring to a datetime object:
			try:
				target_date = datetime.datetime.strptime(target_date_str, params['date_format'])
			except ValueError:
				logging.error('Could not properly parse a date from the retention database file.  Line was %s' % line)
				raise MalformattedDateException('The database is corrupted')
				#TODO send email to someone who can fix it.  Until then, reset the date to NOW + the retention period.
				#target_date = datetime.datetime.now() + datetime.timedelta(days=RETENTION_PERIOD)
			sub_dict = {'bucket': bucket_name, 'client_emails': client_emails.split(','), 'target_date':target_date}
			db[ilab_id] = sub_dict
	return db


def write_to_db(project_mapping, params):
	db_filepath = params['data_retention_db']
	logging.info('Write the database contents to the file at %s' % db_filepath)
	with open(db_filepath, 'w') as fout:
		for project_id, info_dict in project_mapping.items():
			logging.info('Project id: %s with info_dict: %s' % (project_id, info_dict))
			line = '\t'.join([project_id, info_dict['bucket'], ','.join(info_dict['client_emails']), info_dict['target_date'].strftime(params['date_format'])])
			logging.info('Constructed line: %s' % line)
			fout.write('%s\n' % line)
