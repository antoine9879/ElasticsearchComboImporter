from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import string

def get_mask(s):
	mask = ""
	for c in s:
		if c.isdigit():
			mask += "?d"
		elif c.islower():
			mask += "?l"
		elif c.isupper():
			mask += "?u"
		else:
			mask += "?s"
	return mask

def check_special(s):
	for c in s:
		if c in string.punctuation or c.isspace():
			return True
	return False

def check_upper(s):
	return any(i.isupper() for i in s)

def check_lower(s):
	return any(i.islower() for i in s)

def check_digit(s):
    return any(i.isdigit() for i in s)

def parseCredential(email, password):
	doc = {}
	doc["email"] = email
	doc["emailLength"] = len(email)
	doc["password"] = password
	doc["domain"] = email.split('@')[-1]
	doc["length"] = len(password)
	doc["passwordMask"] = get_mask(password)
	doc["containsDigits"] = check_digit(password)
	doc["containsLowerCase"] = check_lower(password)
	doc["containsUpperCase"] = check_upper(password)
	doc["containsSpecial"] = check_special(password)
	return doc

def set_data(input_file, index_name = "", doc_type_name = "credential"):
	for line in open(input_file):
		try:
			c = line.strip().split(":")
			email = c[0].lower().encode('utf-8')
			password = c[1].decode('utf-8')
			if password:
				doc = parseCredential(email, password)
				yield {
					"_index": index_name,
					"_type": doc_type_name,
					"_source": doc
				}
		except Exception as ex:
			pass

def load(es, input_file, **kwargs):
	print '[*] Indexing file: %s' % input_file
	success, _ = bulk(es, set_data(input_file, **kwargs), request_timeout = 60, raise_on_exception = False)

es = Elasticsearch("ip:port")
input_file = "file.txt"
load(es, input_file)