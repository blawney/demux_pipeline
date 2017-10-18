#!/ifs/labs/cccb/projects/cccb/pipelines/demux_and_delivery/demux_venv/bin/python
import sys
import os
import json
import libcloud
from libcloud.storage.types import Provider, ContainerDoesNotExistError
from libcloud.storage.drivers.google_storage import ObjectPermissions
from libcloud.utils.py3 import urlquote
import re
import glob
import logging
import subprocess

import utils

import update_db_manually

class InvalidBucketName(Exception):
	pass


def read_credentials(credential_file):
	"""
	Read the credentials file and return a tuple containing the client ID and the client secret.  Used for the Oauth2 flow.
	"""
	sys.stdout.write('Read credential file at %s' % credential_file)
	j = json.load(open(credential_file))
	sys.stdout.write('file contents: %s' % j)
	return (j['client_id'], j['secret'])


def get_connection_driver(params, google_project):
	"""
	Returns a storage driver for the Apache libcloud library, which exposes methods to operate on buckets
	"""
	sys.stdout.write('Getting connection driver')
	cls = libcloud.storage.providers.get_driver('google_storage')
	client_id, secret = read_credentials(params['credential_file'])
	sys.stdout.write('%s, %s' % (client_id, secret))
	driver = cls(key=client_id, secret=secret, project=google_project)
	sys.stdout.write('Created connection driver, returning')
	return driver


def get_bucket_and_add_permission(bucket_name, driver, users):
	"""
	Grabs bucket for this project.
	Adds READ permissions for the emails passed in users list.
	"""
	try:
		c = driver.get_container(bucket_name)
	except ContainerDoesNotExistError:
		# container did not exist.
		sys.stdout.write('The bucket (%s) does not exist.' % bucket_name)
		sys.exit(1)

	# add or update permissions
	for user in users:
		driver.ex_set_permissions(c.name, 
			entity="user-%s" % user, 
			role=ObjectPermissions.READER)
	return c 		

def give_permissions(driver, container, users):
	"""
	Give reader permissions to the users.
	driver is an instance of the storage driver
	container is a container object instance
	users is a list of strings (email addresses)
	"""
	for o in container.iterate_objects():
		print 'checking %s' % os.path.basename(o.name)
		if os.path.basename(o.name).split('.')[-1] != 'json':
			for user in users:
				driver.ex_set_permissions(container.name, object_name = o.name, entity="user-%s" % user, role=ObjectPermissions.READER)
		else:
			print 'skipping %s' % o.name

def parse_commandline_args():
	"""
	This method parses the commandline arguments and returns a tuple of the passed args
	"""

	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('-b', 
				'--bucket', 
				required = True,
				help = 'The name of the bucket',
				dest = 'bucket_name')	
	parser.add_argument('-u', 
				'--users', 
				required = True,
				help = 'Emails for which to add READ permissions.  If more than 1, a comma-separated list of the emails.  The list can either have no space, or must be surrounded by quotes.',
				dest = 'user_email_csv', type=str)	
	parser.add_argument('-p',
				'--project',
				required = False,
				help = 'The google project name.  If not given, there is a default (see -h)',
				default = 'cccb-data-delivery',
				dest = 'google_project')

	args = parser.parse_args()
	emails = [x.strip() for x in args.user_email_csv.split(',')]
	return (args.bucket_name, emails, args.google_project)


if __name__ == '__main__':

	# want the bucket, the emails, the google project
	bucket_name, users, google_project = parse_commandline_args()

	# update the param dict to include the cloud-specific parameters
	params = utils.parse_config_file('CLOUD')
	sys.stdout.write('Params parsed from config file: %s' % params)

	driver = get_connection_driver(params, google_project)
	bucket_obj = get_bucket_and_add_permission(bucket_name, driver, users)

	# give permissions:
	give_permissions(driver, bucket_obj, users)

	# get the objects in that bucket and keeps their names in a list:
	objects = [x.name for x in bucket_obj.list_objects() if os.path.basename(x.name).split('.')[-1] != 'json']

	# update the web app's database
	update_db_manually.send_request(bucket_obj.name, objects, users)
