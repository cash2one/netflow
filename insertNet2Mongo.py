#!/usr/bin/python
import datetime
import pymongo
from pymongo import Connection
#c = Connection('172.30.250.135', 27017)
c = Connection('172.28.5.122', 27017) 
#c = Connection('172.29.203.64', 27017) 
db = c['ceilometer']



def insertDB(subnet_id,network_id,network_name):
	m = db.resource.find({"_id":subnet_id})
	if len(list(m)) == 0:
		p = db.resource.save({"_id":subnet_id,"type":"added","metadata":{"network_id":network_id}})

	n = db.resource.find({"_id":network_id})
	if len(list(n)) == 0:
		q = db.resource.save({"_id":network_id,"type":"added","metadata":{"name":network_name}})
	


f = open('beijing_net', 'r')

for i in f.readlines():
	ii = i.split(',')
	insertDB(ii[0],ii[1],ii[2].strip())
