import urllib2
import urllib
import json
from Crypto.Cipher import DES
import base64

class Container(object):
	def __init__(self, name):
		self.name = name


class UploadedObject(object):
	def __init__(self, name):
		self.name = name


params = {}
params['simple_auth_key'] = 'FKiyOOJKRwKV0QFi6L2x37ki97oUz7wVq4UQKXlNSQ3Aa8iq'
params['encryption_key'] = 'UfsdTD8h'
params['web_application_notification_endpoint'] = 'https://cccb-delivery.tm4.org/update/'


def send_request(container_name, uploaded_objects, emails):

	# container name is a string, uploaded_object is a list of strings, and emails is a list of strings

	client_email_addresses = emails

	container = Container(container_name)
	object_list = [UploadedObject(x) for x in uploaded_objects]
	upload_list = [] 
	for o in object_list:
		upload_dict = {"basename": o.name, 'bucket_name':container.name, 'owners':client_email_addresses}
		upload_list.append(upload_dict)
	d = {}
	d2 = {}
	d2['uploaded_objects'] = upload_list
	d['uploads'] = json.dumps(d2)

        auth_key = params['simple_auth_key']
        obj=DES.new(params['encryption_key'], DES.MODE_ECB)
        enc_token = obj.encrypt(auth_key)
	b64_str = base64.encodestring(enc_token)
        d['token'] = b64_str
        print d
        endpoint_url = params['web_application_notification_endpoint']
	data = urllib.urlencode(d)
        req = urllib2.Request(endpoint_url, {'Content-Type': 'application/json'})
        f = urllib2.urlopen(req, data=data)
        response = f.read()
	print response
