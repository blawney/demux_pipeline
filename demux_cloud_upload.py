import sys
import numpy as np
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

class InvalidBucketName(Exception):
	pass


class MockObject(object):
	"""
	When I initially wrote this module, I had Apache libcloud doing the uploads.  The uploads kept failing, so we decided to 
	switch to gsutil utilities.  When we upload via gsutil we do NOT get back a google storage "object" instance
	representing the uploaded object.  Other functions call attributes on those (e.g. uploaded_object.name) to
	do things like reset permissions, etc..  

	To avoid rewriting those working functions, simply make a wrapper object which has the 'name' attribute
	For each file uploaded via gsutil, create an instance of this class, giving it the proper name.
	"""
	def __init__(self, name):
		self.name = name


def read_credentials(credential_file):
	"""
	Read the credentials file and return a tuple containing the client ID and the client secret.  Used for the Oauth2 flow.
	"""
	logging.info('Read credential file at %s' % credential_file)
	j = json.load(open(credential_file))
	logging.info('file contents: %s' % j)
	return (j['client_id'], j['secret'])


def get_connection_driver(params):
	"""
	Returns a storage driver for the Apache libcloud library, which exposes methods to operate on buckets
	"""
	logging.info('Getting connection driver')
	cls = libcloud.storage.providers.get_driver('google_storage')
	client_id, secret = read_credentials(params['credential_file'])
	logging.info('%s, %s' % (client_id, secret))
	driver = cls(key=client_id, secret=secret, project=params['google_project_id'])
	logging.info('Created connection driver, returning')
	return driver


def prep_fastqc_files(fqd, destination_bucket, params):
	"""
	Prepares the files to be hosted in storage.
	Need to update the relative links to absolute gooogle storage links
	Each fastQC report is structured as follows:
		-fastqc_data.txt
		-fastqc_report.html
		-summary.txt
		-Images/
			- has 'dynamics' images that are sample-specific
		-Icons/
			- images that are common between projects
	Hence, the fastqc_report.html references images that are src="Icons/..." or src="Images/..."
	Update the src="Icons/..." to be served from a main bucket 
	Update the src="Images/..." to be from this project's bucket 

	fqd is absolute path to the directory containing the fastQC files for a sample (+read if paired reads)
	destination bucket is the name of the bucket for this sequencing project.
	"""
	report = os.path.join(fqd, params['fastqc_report'])
	report_dirname = os.path.basename(fqd)
	contents = open(report, 'r').read()
	#contents = contents.replace('Icons', '%s/%s' % (params['storage_root'], params['master_bucket'])) # all icons served from same location	
	contents = contents.replace('Images', '%s/%s/%s/%s/Images' % (params['storage_root'], destination_bucket, params['cloud_fastqc_root'], report_dirname))
	contents = contents.replace('Icons', '%s/%s/%s/%s/Icons' % (params['storage_root'], destination_bucket, params['cloud_fastqc_root'], report_dirname))
	# TODO: change this once we know it works.  Then just write to teh same html file
	with open(report + params['gcloud_edit_suffix'], 'w') as fout: 
		fout.write(contents)


def get_or_create_bucket(ilab_id, driver, users, params):
	"""
	Grabs or creates bucket for this project.
	Adds READ permissions for the emails passed in users list.
	"""
	destination_bucket_name = '%s-%s' % (params['master_bucket'], ilab_id)
	logging.info('upload to bucket: %s' % destination_bucket_name)
	if is_valid_bucket_name(destination_bucket_name):
		# check for existing bucket:
		try:
			c = driver.get_container(destination_bucket_name)
		except ContainerDoesNotExistError:
			# container did not exist.  Make it.
			c = driver.create_container(destination_bucket_name)

		# add or update permissions
		for user in users:
			driver.ex_set_permissions(c.name, 
				entity="user-%s" % user, 
				role=ObjectPermissions.READER)
		# TODO: unless something strange happened, we have a container now.  Maybe double-check?
		logging.info('final container: %s' % c.name)
		return c 		
	else:
		raise InvalidBucketName('Bucket name (%s) did not pass all requirements' % destination_bucket_name)


def is_valid_bucket_name(x):
	"""
	returns True if bucket name is OK, else False
	see https://cloud.google.com/storage/docs/naming
	"""
	# test the length
	length_test = (len(x)>=3) & (len(x) <= 63)

	# test that start and end are alphanumeric
	start_and_end_test = ( str.isalnum(x[0]) ) & ( str.isalnum(x[-1]) ) 

	# test the characters:
	pattern = '[a-z0-9\-]+' # only allow lowercase, numbers, and dashes, which is more restrictive than google.
	m = re.search(pattern, x)
	char_test = m.group(0) == x

	return all([length_test, start_and_end_test, char_test])


def collect_files(project_dir, filetype, params):
	"""
	project_dir gives the absolute path to the directory
	filetype gives what sort of file we're looking for by matching the end
	"""
	filetype = params[filetype]
	return glob.glob(os.path.join(project_dir, '%s*' % params['sample_dir_prefix'],'*%s' % filetype)) # project_dir is the path on OUR local filesystem
	

def upload_fastq_dir(upload_items, project_dir, container, root_location, params):
	"""
	Since the fastq files are large, we required another way to upload them reliably.  The quick solution was to
	symlink the 'final' fastq files from a single location and use gsutil's rsync functionality to send them up

	upload_items is a list of filepaths to the fastq files.
	container is a Apache libcloud Container instance
	root_location is a directory, relative to the root of the bucket, where the fastq files will go
		for instance, if root_location is a/b/c (and the bucket is mybucket, then the fastq files will go to
		gs://mybucket/a/b/c and fastq files will be at gs://mybucket/a/b/c/myfastq.fastq.gz
	params is a dictionary with configuration parameters
	"""
	# first symlink all the necessary files
	try:
		final_symlinked_directory = os.path.join(project_dir, params['final_symlinked_fastq_directory'])
		os.mkdir(final_symlinked_directory)
	except OSError as ex:
		if ex.errno == 17:
			logging.info('The symlink fastq directory already existed at %s' % final_symlinked_directory)
			pass
		else:
			logging.error('Exception thrown when making symlinked fastq directory: %s' % ex.message)
			raise ex

	for item in upload_items:
		try:
			basename = os.path.basename(item)
			os.symlink(item, os.path.join(final_symlinked_directory, basename))
		except OSError as ex:
			if ex.errno == 17:
				logging.info('A symlink for %s fastq already existed' % basename)
				pass
			else:
				logging.error('Exception thrown when linking fastq file: %s' % ex.message)
				raise ex
		
	# now upload using gsutil rsync
	rsync_cmd = 'gsutil rsync -r %s gs://%s/%s' % (final_symlinked_directory, container.name, root_location)
	logging.info('Issue system command for uploading fastq files: %s' % rsync_cmd)
	process = subprocess.Popen(rsync_cmd, shell = True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
	stdout, stderr = process.communicate()
	logging.info('STDOUT from gsutil fastq upload: ')
	logging.info(stdout)
	logging.info('STDERR from gsutil fastq upload: ')
	logging.info(stderr)
	if process.returncode != 0:
		logging.error('There was an error while uploading with gsutil.  Check the logs.')
		raise Exception('Error during gsutil upload module.')
	else:
		uploaded_objects = []
		for item in upload_items:
			basename = os.path.basename(item)
			object_name = os.path.join(root_location, basename)
			uploaded_objects.append(MockObject(object_name))
	return uploaded_objects
	

def upload(upload_items, container, root_location, params):
	"""
	files is a list of paths on our local filesystem.  Can be actual files OR directories
	container is a Container instance
	root_location is the name of a "directory" relative to the bucket
		e.g. if we want to put a file c.txt at <bucket>/a/b/c.txt, then 
		root_location would be "a/b"
	"""
	uploaded_objects = []
	for resource in upload_items:
		# check if we're uploading a directory. If that's the case, we have to walk the directory and upload the files one-by-one
		# The reason for this is that the fastQC html files need to have their relative links rewritten (e.g. links to the images, css files, etc).
		# Otherwise, the pages will not render correctly.  Because of that, we can't just do a cp -r operation.  
		if os.path.isdir(resource):
			for r, d, files in os.walk(resource):
				for f in files:
					extra = None
					if not f == params['fastqc_report']: # don't upload the original html report
						fp=os.path.join(r, f)
						upload_name = os.path.join( root_location,  fp[len(os.path.dirname(resource)) +1:] ) # the conventional upload lcoation
						# if the upload is the edited html report, we need to change the name
						if f == params['fastqc_report'] + params['gcloud_edit_suffix']:
							# fp /ifs/labs/cccb/.../AB_01272017_1136/Sample_WK11_K/WK11_K_R1_.final_fastqc/fastqc_report.html.gcloud_edit
							# upload_name starts like fastQC/WK11_R1_.final_fastqc/fastqc_report.html.gcloud_edit
							# want fastQC/WK11_R1_.final_fastqc/WK11_R1_.html
							sample_and_read_fastQC_dir = os.path.basename(os.path.dirname(fp))
							sample_and_read_id = sample_and_read_fastQC_dir[:-len(params['fastqc_dir_suffix'])] # something like <sample ID>_R1_. (includes that final dot)
							upload_name = os.path.join( root_location, sample_and_read_fastQC_dir, sample_and_read_id + 'html' )
							#upload_name = upload_name[:-len(params['gcloud_edit_suffix'])]
							extra = {'content_type': 'text/html'}
						upload_cmd = 'gsutil -h "Content-Type:text/html" cp %s gs://%s/%s' % (fp, container.name, upload_name)
						logging.info('Issuing the following system command: %s' % upload_cmd)
						process = subprocess.Popen(upload_cmd, shell = True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
						stdout, stderr = process.communicate()
						logging.info('STDOUT from gsutil upload: ')
						logging.info(stdout)
						logging.info('STDERR from gsutil upload: ')
						logging.info(stderr)
						if process.returncode != 0:
							logging.error('There was an error while uploading with gsutil.  Check the logs.')
							raise Exception('Error during gsutil upload module.')
						else:
							uploaded_objects.append(MockObject(upload_name))
		else:
			# TODO (maybe): Change this over to a gsutil command.  It's just a simple file copy (on small files) so it works reliably at the moment
			uploaded_objects.append(container.upload_object(resource, os.path.join(root_location,  os.path.basename(resource))))
			logging.info('Upload %s to %s' % (resource, os.path.join(root_location,  os.path.basename(resource))))
	return uploaded_objects


def get_client_emails(project_dir, params):
	contents = json.load(open(os.path.join(project_dir, params['project_descriptor'])))
	client_email_addresses = []
	for email in contents['client_emails']:
		#TODO generalize/parameterize this once we know the full scope of allowed accounts
		if email.endswith('gmail.com'):
			client_email_addresses.append(email)
		if email.endswith('mail.dfci.harvard.edu'):
			client_email_addresses.append(email)
	if len(client_email_addresses) == 0:
		logging.info('Warning: no email addresses were added, so only CCCB has access')
	return client_email_addresses


def give_permissions(driver, container, object_list, users):
	"""
	Give reader permissions to the users.
	driver is an instance of the storage driver
	container is a container object instance
	object_list is a list of Object instances
	users is a list of strings (email addresses)
	"""
	for o in object_list:
		logging.info('adjusting privileges for %s' % o.name)
		for user in users:
			driver.ex_set_permissions(container.name, object_name = o.name, entity="user-%s" % user, role=ObjectPermissions.READER)
				

def update_project_mappings(storage_driver, bucket, users, params):
	"""
	This downloads the file which maps the users to their buckets, updates it, and re-uploads it
	"""

	try:
		obj = storage_driver.get_object(container_name = params['master_bucket'], object_name = params['master_file'])
		download_location = os.path.join('.', obj.name)
		if obj.download(destination_path=download_location, overwrite_existing=True):
			j = json.load(open(download_location))
			records = j['client']

			# read json into a dictionary
			email_to_buckets_map = {}
			for record in records:
				email_to_buckets_map[ record['email']] = record['project_buckets']
			for user in users:
				if user in email_to_buckets_map.keys():
					# check if user already 'has' this bucket
					if not bucket in email_to_buckets_map[user]:
						email_to_buckets_map[user].append(bucket.name)
					else:
						logging.info('Bucket %s WAS in the original list: %s' % (bucket, email_to_buckets_map[user]))
				else:
					email_to_buckets_map[user] = [bucket.name]

			# now write the dictionary as JSON:
			updated_map = {}
			client_list = []
			for user in email_to_buckets_map.keys():
				client_list.append( {'email':user, 'project_buckets':list(set(email_to_buckets_map[user]))} )
			updated_map['client'] = client_list
			json.dump(updated_map, open(download_location,'w'))

			# need to get the master bucket:
			mb = storage_driver.get_container(params['master_bucket'])
			mb.upload_object(download_location, os.path.join(params['master_file']))
			logging.info('Upload %s to %s' % (download_location, os.path.join(params['master_file'])))

	except Exception as ex:
		logging.error('Something went wrong in updating the master file')
		logging.error(ex.message)


def calculate_upload_size(uploaded_objects, scale=1e9):
	"""
	(If used).  Uploaded objects is a list of objects which have a 'size' attribute representing 
	their size in bytes.  Will return the sum of the bytes in all lists, scaled by the appropriate factor 
	(e.g. 1e9 for GB) for human readability.
	"""
	all_sizes = [o.size for o in uploaded_objects]	
	total_size_in_bytes = np.sum(all_sizes)
	return total_size_in_bytes/float(scale)		


def entry_method(project_dir, params):
	"""
	project_dir is a string giving the full path to the project directory
	params is a dictionary of configuration parameters.
	"""

	# update the param dict to include the cloud-specific parameters
	cloud_specific_params = utils.parse_config_file('CLOUD')
	params.update(cloud_specific_params)
	logging.info('Updated params in cloud upload script: %s' % params)

	# correct the fastQC directory suffix name
	params['fastqc_dir_suffix'] = params['final_fastq_tag'] + params['fastqc_dir_suffix']

	params['fastq_file_suffix'] = '.' + params['final_fastq_tag'] + '.fastq.gz'

	client_email_addresses = get_client_emails(project_dir, params)
	ilab_id = os.path.basename(project_dir).replace('_', '-').lower()

	driver = get_connection_driver(params)
	bucket_obj = get_or_create_bucket(ilab_id, driver, client_email_addresses, params)

	#TODO: collect lane-specific fastq

	fastq_files = collect_files(project_dir, 'fastq_file_suffix', params)
	fastQC_dirs = collect_files(project_dir, 'fastqc_dir_suffix', params)
	for fqd in fastQC_dirs:
		prep_fastqc_files(fqd, bucket_obj.name, params)
	
	# do uploads
	uploaded_objects = []
	uploaded_objects.extend(upload_fastq_dir(fastq_files, project_dir, bucket_obj, params['cloud_fastq_root'], params))
	#uploaded_objects.extend(upload(fastq_files, bucket_obj, params['cloud_fastq_root'], params))
	uploaded_objects.extend(upload(fastQC_dirs, bucket_obj, params['cloud_fastqc_root'], params))

	# give permissions:
	give_permissions(driver, bucket_obj, uploaded_objects, client_email_addresses)

	# get the total upload size
	# commented out since using rsync
	#upload_size = calculate_upload_size(uploaded_objects)
	#logging.info('upload size in GB: %s' % upload_size)

	# upload the metadata file:
	upload([os.path.join(project_dir, params['project_descriptor']),], bucket_obj, '', params)	

	# handle master metadata file
	update_project_mappings(driver, bucket_obj, client_email_addresses, params)

	# return the bucket name and the email addresses- need this information for the data retention process
	return bucket_obj.name, client_email_addresses
