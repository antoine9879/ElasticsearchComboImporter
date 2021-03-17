from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import string

def parseCredential(email, hash):
	doc = {}
	doc["email"] = email
	doc["hash"] = hash
	doc["domain"] = email.split('@')[-1]
	doc["emailLength"] = len(email)
	return doc

def set_data(input_file, index_name = "", doc_type_name = "credential"):
	for line in open(input_file):
		try:
			c = line.strip().split(":")
			email = c[0].lower().encode('utf-8')
			hash = c[1].decode('utf-8')
			if hash:
				doc = parseCredential(email, hash)
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