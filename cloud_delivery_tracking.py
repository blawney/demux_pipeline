"""
This script is called when the demultiplex has successfully completed.

In short, it adds the project to a flat-file database which keeps track of when demux happened, and when the data will expire for download.  

For further details, look through the comments below.  
"""

import logging
import datetime
import os
import sys
import utils


class MultipleBucketsForSameProjectException(Exception):
	pass


class MissingPrimaryDatabaseException(Exception):
	pass


def main(project_mapping):
	"""
	project_mapping is a two-level nested dict.
	The first level's keys are the iLab project IDs and each one maps to a dict
	Each 'second level' dict has a bucket and client_emails key, which give the bucket name gs://<bucket name>
	and a list of emails, respectively
	"""

	logging.info('In cloud tracking module')
	logging.info('Project mapping: %s' % project_mapping)

	# get some configuration parameters
	params = utils.parse_config_file('TRACKING')
	
	# need to cleanup some of the parameters:
	try:
		params['retention_period'] = int(params['retention_period'])
		logging.info('Params read from config: %s' % params)
		logging.info('Retention period set to %s days' % params['retention_period'])
	except:
		logging.error('Could not interpret one of the configuration parameters correctly.  Check that the intended data types match those in the config file')
		sys.exit(1)		

	# set the expiration date
	target_date = datetime.datetime.now() + datetime.timedelta(days=params['retention_period'])

	# read the database file
	this_dir = os.path.dirname(os.path.realpath(__file__))
	params['data_retention_db'] = os.path.join(this_dir, params['data_retention_db'])
	if os.path.isfile(params['data_retention_db']):
		logging.info('About to parse database file' )
		project_database = utils.load_database(params['data_retention_db'], params)
		logging.info('Parsed from the database: %s' % project_database)
	else:
		logging.error('Could not find a database file at %s' % params['data_retention_db'])
		raise MissingPrimaryDatabaseException('The primary database file is missing.  Fix that.')

	for project_id, info_dict in project_mapping.items():

		logging.info('Checking project with iLab ID: %s' % project_id)
		# perhaps we have an ongoing project- then a bucket for this iLab ID probably already exists
		if project_id in project_database:

			logging.info('project with ID %s was already in our database. Plan to update the deletion date' % project_id)
			# get the info we have about this in our database
			db_entry = project_database[project_id]
			
			# ensure the bucket names match.  If they do, simply update the retention target date and the email contacts
			if info_dict['bucket'] == db_entry['bucket']:
				logging.info('The delivery buckets matched, as expected')
				logging.info('Changing deletion date from %s to %s' % (db_entry['target_date'].strftime(params['date_format']), target_date.strftime(params['date_format'])))
				db_entry['target_date'] = target_date
				existing_emails = set(db_entry['client_emails'])
				new_emails = set(info_dict['client_emails'])
				total_emails = existing_emails.union(new_emails)
				logging.info('Original emails were: %s' % existing_emails)
				logging.info('New emails were: %s' % new_emails)
				logging.info('The union of those sets of emails is %s' % total_emails)
				db_entry['client_emails'] = list(total_emails)
			else:
				# somehow the same iLab project was placed into a different bucket.  Shouldn't happen, so raise an exception.  We 
				# retain 1-to-1 mapping beween ilab and buckets IDs.  Maybe later we change this behavior based on a particular use-case
				logging.error('The bucket name did not match that of a prior project with the same iLab ID.  This should not happen.')
				logging.error('The bucket found in the database was: %s' % db_entry['bucket'])
				logging.error('The bucket that was just uploaded the demux to was: %s' % info_dict['bucket'])
				raise MultipleBucketsForSameProjectException('The iLab IDs were the same, but the bucket was somehow different.  Someone needs to check this!')
				#TODO- send a message for someone to fix it.

		else:
			logging.info('A new project will be added to the database.')
			logging.info('update info dict.  Before %s, then add %s' % (info_dict, target_date))
			info_dict.update({'target_date': target_date})
			project_database[project_id] = info_dict

	logging.info('Project database: %s' % project_database)
				
	# now write to the database file:
	utils.write_to_db(project_database, params)
