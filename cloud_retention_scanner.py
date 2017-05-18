#!/ifs/labs/cccb/projects/cccb/pipelines/demux_and_delivery/venv/bin/python

"""
This module scans the database of projects and determines those that are approaching deletion.  
Sends emails are prescribed intervals regarding deletion of files
Does not actually perform deletion of the files.  Merely writes the commands into a file that
someone has to manually run.
"""

import os
import sys
import subprocess
import datetime
import logging
import utils
import jinja2
import subprocess


class InvalidReminderCheckpointsException(Exception):
	pass

class MalformattedDateException(Exception):
	pass

class DatabaseInconsistencyException(Exception):
	pass


def calculate_size(bucket_name, scale = 1e9):
	cmd = 'gsutil du gs://%s' % bucket_name
	p = subprocess.Popen(cmd, shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
	stdout, stderr = p.communicate()
	results = stdout.split('\n')
	total_size = 0
	for r in results:
		if len(r) > 0:
			size, f = r.split()
			total_size += int(size)
	return total_size/scale

def parse_all_db_files(params):
	"""
	Using the config params, determines all the locations where our database files live.
	Then, parses them each and stores them in a list.  In the end, it returns a list of 
	dictionaries.  Each entry in that list are the database contents from the different sources.
	"""
	db_list = []
	logging.info('Parsing primary database at %s' % params['data_retention_db'])
	db_list.append( utils.load_database(params['data_retention_db'], params))
	for db_file in params['backup_db_list']:
		logging.info('Parsing backup database file at %s' % db_file)
		db_list.append(utils.load_database(db_file, params))
	logging.info('List of database info: %s' % db_list)
	return db_list


def ensure_consistency(db_list):
	"""
	The input arg is a list of dictionaries.  Each item in that list is essentially the database
	contents from a particular database source (e.g. our local file, a cloud-based file, etc).
	This function checks to make sure those databases are consistent.  If they are consistent, return
	the data structure.  Else, raise an exception
	"""
	primary_db = db_list[0]
	for db in db_list:
		if db != primary_db:
			logging.error('The database (%s) did not match (%s)' % (db, primary_db))
			raise DatabaseInconsistencyException('Databases were not the same.  Need to manually ensure consistency.  Check consistency between')
	logging.info('Databases were consistent.  Contents: %s' % primary_db)
	return primary_db
		

def send_reminder_email(project_id, info_dict, params, remaining_days):
	this_directory = os.path.dirname(os.path.realpath(__file__))
	env = jinja2.Environment(loader=jinja2.FileSystemLoader(this_directory))
	template = env.get_template(params['reminder_email_template'])
	context = {}
	context['project_id'] = project_id.upper()
	context['target_date'] = info_dict['target_date'].strftime('%B %d, %Y')
	context['remaining_days'] = remaining_days
	size_in_gb = calculate_size(info_dict['bucket'], scale = 1e9)
	context['size_in_gb'] = '%.2f' % size_in_gb
	cost_per_gb_per_month = float(params['storage_cost'])
	estimated_cost = size_in_gb * cost_per_gb_per_month
	context['estimated_monthly_cost'] = '%.2f' % estimated_cost
	context['cccb_download_site'] = params['cccb_download_site']
	email_text = template.render(context)
	#print email_text
	client_emails_as_str = ','.join(info_dict['client_emails'])
	utils.send_notifications(client_emails_as_str, params['smtp_server'], params['smtp_port'], params['client_notification_subject'], email_text)


def mark_for_deletion(project_id, info_dict, params):
	"""
	This puts a remove command into a special file and informs the CCCB staff via email
	"""
	this_directory = os.path.dirname(os.path.realpath(__file__))
	bucket_name = info_dict['bucket']
	client_emails_as_str = ','.join(info_dict['client_emails'])
	gsutil_rm_files_command = 'gsutil rm -r gs://%s/*' % bucket_name
	gsutil_rm_bucket_command = 'gsutil rb gs://%s' % bucket_name
	with open(os.path.join(this_directory, params['data_cleanup_command_log']), 'a') as fout:
		fout.write('# Removing project %s, contained in bucket gs://%s, with client emails %s\n' % (project_id, bucket_name, client_emails_as_str))
		fout.write(gsutil_rm_files_command + '\n')
		fout.write(gsutil_rm_bucket_command + '\n')

	env = jinja2.Environment(loader=jinja2.FileSystemLoader(this_directory))
	template = env.get_template(params['cccb_deletion_notification_template'])
	context = {'project_id':project_id, 'deletion_command_file': os.path.join(this_directory, params['data_cleanup_command_log'])}
	email_text = template.render(context)
	cccb_emails_as_str = ','.join(params['cccb_internal_notification_list'])
	utils.send_notifications(cccb_emails_as_str, params['smtp_server'], params['smtp_port'], '[CCCB] There is data to cleanup', email_text)


def scan_db(db_contents, params):
	"""
	This is the workhorse function.  It scans through all the content and sends reminder emails, populates deletion files, etc.
	db_contents is a dictionary (keyed by iLab ID or some other unique project identifier).  The key points at a dictionary containing
	information about the client emails, the project's bucket, and the target deletion date
	"""
	try:
		reminder_checkpoints = [int(x) for x in params['reminder_intervals']]
	except ValueError as ex:
		logging.error('Could not parse integers from the checkpoints')
		raise InvalidReminderCheckpointsException('Could not parse integers for the reminder checkpoints.')

	now = datetime.datetime.now()

	for project_id, info_dict in db_contents.items():
		# get the time until deletion:
		target_date = info_dict['target_date']
		time_delta = target_date - now
		days_until_deletion = time_delta.days + 1
		if days_until_deletion in reminder_checkpoints:
			logging.info('Project %s is due for a reminder' % project_id)
			# send email
			send_reminder_email(project_id, info_dict, params, days_until_deletion)
		elif days_until_deletion == 0:
			logging.info('Project %s is due to be removed')
			# place into a removal file
			mark_for_deletion(project_id, info_dict, params)


def main():
	
	# load some config parameters
	params = utils.parse_config_file('TRACKING')

	# look in the various places to determine that all database files are consistent
	db_collection = parse_all_db_files(params)

	# get the database contents- see function for details
	db_contents = ensure_consistency(db_collection)

	# scan the contents, perform the appropriate actions
	scan_db(db_contents, params)

	
	
