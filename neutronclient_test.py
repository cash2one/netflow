#!/usr/bin/python

import novaclient.v1_1.client
import neutronclient.v2_0.client

#c = neutronclient.v2_0.client.Client(username='admin', password='NanHui_Cloud@PWXadmin', 
#	tenant_name='admin', region_name='Nanhui', auth_url='http://172.30.251.13:5000/v2.0/')

#print type(c.list_networks())


n = novaclient.v1_1.client.Client(username='admin', api_key='test',
	#project_id='620e1f372fa84bd09f42e9062ae8e621', region_name='zhenru', auth_url='http://172.30.251.13:5000/v2.0/')
	project_id='admin', region_name='zhenru', auth_url='http://172.30.251.13:5000/v2.0/')

for i in  n.servers.interface_list("7bfd6915-1455-46f8-aaa3-e14918d3999e"):
	print i._info
