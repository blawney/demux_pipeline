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

class InvalidBucketName(Exception):
	pass

def read_credentials(credential_file):
	logging.info('Read credential file at %s' % credential_file)
	j = json.load(open(credential_file))
	logging.info('file contents: %s' % j)
	return (j['client_id'], j['secret'])


def get_connection_driver(params):
	"""
	Returns a storage driver, which exposes methods to operate on buckets
	"""
	logging.info('Getting connection driver')
	cls = libcloud.storage.providers.get_driver('google_storage')
	client_id, secret = read_credentials(params['credential_file'])
	logging.info('%s, %s' % (client_id, secret))
	driver = cls(key=client_id, secret=secret)
	driver.project_id = 'dfci-cccb'
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
						uploaded_objects.append(container.upload_object(fp, upload_name, extra=extra))
						logging.info('Upload %s to %s' % (fp, upload_name))
		else:
			uploaded_objects.append(container.upload_object(resource, os.path.join(root_location,  os.path.basename(resource))))
			logging.info('Upload %s to %s' % (resource, os.path.join(root_location,  os.path.basename(resource))))
	return uploaded_objects


def get_client_emails(project_dir, params):
	contents = json.load(open(os.path.join(project_dir, params['project_descriptor'])))
	gmail_addresses = []
	for email in contents['client_emails']:
		if email.endswith('gmail.com'):
			gmail_addresses.append(email)
	if len(gmail_addresses) == 0:
		logging.info('Warning: no email addresses were added, so only CCCB has access')
	return gmail_addresses


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
	bucket is 
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
	all_sizes = [o.size for o in uploaded_objects]	
	total_size_in_bytes = np.sum(all_sizes)
	return total_size_in_bytes/float(scale)		


def entry_method(project_dir, params):
	"""
	project_dir is a string giving the full path to the project directory
	"""

	# assume params is a passed dictionary 

	# correct the fastQC directory suffix name
	params['fastqc_dir_suffix'] = params['final_fastq_tag'] + params['fastqc_dir_suffix']

	params['fastq_file_suffix'] = '.' + params['final_fastq_tag'] + '.fastq.gz'

	gmail_addresses = get_client_emails(project_dir, params)
	ilab_id = os.path.basename(project_dir).replace('_', '-').lower()

	driver = get_connection_driver(params)
	bucket_obj = get_or_create_bucket(ilab_id, driver, gmail_addresses, params)

	#TODO: collect lane-specific fastq

	fastq_files = collect_files(project_dir, 'fastq_file_suffix', params)
	fastQC_dirs = collect_files(project_dir, 'fastqc_dir_suffix', params)
	for fqd in fastQC_dirs:
		prep_fastqc_files(fqd, bucket_obj.name, params)
	
	# do uploads
	uploaded_objects = []
	uploaded_objects.extend(upload(fastq_files, bucket_obj, params['cloud_fastq_root'], params))
	uploaded_objects.extend(upload(fastQC_dirs, bucket_obj, params['cloud_fastqc_root'], params))

	# give permissions:
	give_permissions(driver, bucket_obj, uploaded_objects, gmail_addresses)

	# get the total upload size
	upload_size = calculate_upload_size(uploaded_objects)
	logging.info('upload size in GB: %s' % upload_size)

	# upload the metadata file:
	upload([os.path.join(project_dir, params['project_descriptor']),], bucket_obj, '', params)	

	# handle master metadata file
	update_project_mappings(driver, bucket_obj, gmail_addresses, params)


