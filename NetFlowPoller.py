#!/usr/bin/python
import MySQLdb
import datetime
import re
import os
import sys
import time
import mana_log
import threading
from pymongo import Connection
import ConfigParser
import keystoneclient.v2_0.client
import novaclient.v1_1.client 
import neutronclient.v2_0.client


class NetFlowPoller:

    def __init__(self, region):
        self.project_id_list = []
        self.region_name_dict = {
                                  "Nanhui": "172.30.250.135",
                                  "zhenru": "172.29.203.64",
                                  "beijing": "172.28.5.122"
                                }
        self.mhost = self.region_name_dict[region]
        self.region = region
        try:
            self.mconn = Connection(self.mhost, 27017)
        except:
            print 'conn errot'
        self.db = self.mconn['ceilometer']
        self.conn = MySQLdb.connect(host='localhost',
                                    user='cloud',
                                    passwd='NanHui-F2-Cloud!@#',
                                    port=6020)

        self.conn.select_db('netflow')
        self.cur = self.conn.cursor()
        self.keyc = keystoneclient.v2_0.client.Client(username='admin',
                                                password='test',
                                                tenant_name='admin',
                                                auth_url='http://172.30.251.13:35357/v2.0/')
        self.novac = novaclient.v1_1.client.Client(username='admin', api_key='test',
                                                     project_id='admin', region_name='%s' % self.region, 
                                                     auth_url='http://172.30.251.13:5000/v2.0/')
        self.neutronc = neutronclient.v2_0.client.Client(username='admin', password='test',
                                                         tenant_name='admin', region_name='%s' % self.region, 
                                                         auth_url='http://172.30.251.13:5000/v2.0/')
        self.net_list = self.neutronc.list_networks()["networks"]


    def readConf(self):
        cf = ConfigParser.ConfigParser()
        cf.read("/opt/projects/mana_backend/mana_backend/NetFlow.ini")
        network = cf.get(self.region, "network")
        network_list = network.split(';')
        return network_list
    
    def networkIsMatch(self, network_name):
        network_list = self.readConf()
        isMatch = False
        for i in network_list:
            if re.match(i.strip(), network_name):
                isMatch = True
        return isMatch

    def getProjectList(self):
        for i in self.keyc.tenants.list():
            self.project_id_list.append(i.id)
        return self.project_id_list

    '''
    Nanhui use this function
    def getProjectList(self):
        if not self.project_id_list
            r = self.db.project.find()
            for i in list(r):
                self.project_id_list.append(i["_id"])
        return self.project_id_list
    '''

    def get_instance_long_id_by_project_id(self, project_id):
        allLongID = []
        for i in list(self.db.resource.find({"meter.counter_name":"network.outgoing.bytes", "project_id": project_id})):
            allLongID.append(i["_id"])
        return allLongID

    def getTotalNetFlow(self, project_id, end_time, begin_time):
        allLongID = self.get_instance_long_id_by_project_id(project_id)
        totalin = 0.0
        totalout = 0.0
        delta = datetime.timedelta(seconds=600)
        end_time_obj = end_time
        end_time_obj_10 = end_time_obj - delta
        begin_time_obj = begin_time
        begin_time_obj_10 = begin_time_obj - delta

        for j in allLongID:
            try:
                r = self.db.meter.find({"resource_id": j, "counter_name":"network.incoming.bytes",
                                        "timestamp": {"$gt": end_time_obj_10, "$lt": end_time_obj}}).limit(1)
                end_in = list(r)[0]["counter_volume"]
            except IndexError:
                end_in = 0.0

            try:
                rr = self.db.meter.find({"resource_id": j, "counter_name":"network.incoming.bytes",
                                         "timestamp": {"$gt": begin_time_obj_10, "$lt": begin_time_obj}}).limit(1)
                begin_in = list(rr)[0]["counter_volume"]
            except IndexError:
                #print '{"resource_id": %s, "counter_name":"network.incoming.bytes", "timestamp": {"$lt": %s}}' % (j, begin_time_obj)
                begin_in = 0.0

            totalin += end_in - begin_in

            ####-----------------------------------------------------------------------------------------###

            try:
                m = self.db.meter.find({"resource_id": j, "counter_name":"network.outgoing.bytes",
                                        "timestamp": {"$gt": end_time_obj_10, "$lt": end_time_obj}}).limit(1)
                end_out = list(m)[0]["counter_volume"]
            except IndexError:
                end_out = 0.0

            try:
                mm = self.db.meter.find({"resource_id": j, "counter_name":"network.outgoing.bytes",
                                         "timestamp": {"$gt": begin_time_obj_10, "$lt": begin_time_obj}}).limit(1)
                begin_out = list(mm)[0]["counter_volume"]
            except IndexError:
                begin_out = 0.0

            totalout += end_out - begin_out

        return(totalin, totalout)

    def getMaxRate(self, project_id, now_time):
        #now_time='%Y-%m-%d %H:%M:%S'
        now_time_obj = now_time
        delta = datetime.timedelta(seconds=600)
        previous_time_obj = now_time_obj - delta


        max_in_rate = 0.0
        max_out_rate = 0.0

        try:
            q = self.db.meter.find({"project_id": project_id, "counter_name":"network.incoming.bytes.rate",
                                    "timestamp": {"$lt": now_time_obj, "$gte":  previous_time_obj}})
            for i in list(q):
                network_name, ipaddr = self.getNetworkNameByLongID(i["resource_id"])
                if network_name and self.networkIsMatch(network_name):
                    max_in_rate += i["counter_volume"]
                else:
                    continue

        except IndexError:
            LOG.error('project statistics list failed')
            max_in_rate = 0.0

        try:
            p = self.db.meter.find({"project_id": project_id, "counter_name":"network.outgoing.bytes.rate",
                                    "timestamp": {"$lt": now_time_obj, "$gte":  previous_time_obj}})
            for i in list(p):
                network_name, ipaddr = self.getNetworkNameByLongID(i["resource_id"])
                if network_name and self.networkIsMatch(network_name):
                    max_out_rate += i["counter_volume"]
                else:
                    continue

        except IndexError:
            LOG.error('project statistics list failed')
            max_out_rate = 0.0

        return(max_in_rate, max_out_rate, previous_time_obj, now_time_obj)

    def getNetworkNameByLongID(self, LongID):
        instance_id = LongID[18:54]
        port_id = LongID[58:69]

        try:
            for i in self.novac.servers.interface_list(instance_id):
                if re.match(port_id, i._info["port_id"]):
                    net_id = i._info["net_id"]
                    if i._info["fixed_ips"]:
                        ipaddr = i._info["fixed_ips"][0]["ip_address"]
                    else:
                        ipaddr = None
                    break
            for n in self.net_list:
                if n["id"] == net_id:
                    network_name = n["name"]
                    break
            return network_name, ipaddr
        except:
            return None, None

    def getInstanceJson(self, project_id):

        allLongID = self.get_instance_long_id_by_project_id(project_id)

        json_str = {}

        for i in allLongID:
            resource_id = i[18:54]
            network_name, ipaddr = self.getNetworkNameByLongID(i)
            if network_name and self.networkIsMatch(network_name):
                try:
                    instance = list(self.db.resource.find({"_id": resource_id}).limit(1))[0]
                    json_str[i] = {}
                    json_str[i]["name"] = instance["metadata"]["display_name"]
                    json_str[i]["id"] = instance["_id"]
                    json_str[i]["iface_id"] = i
                    onehour_ago = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
                    if instance["last_sample_timestamp"] < onehour_ago:
                        json_str[i]["status"] = "DELETED"
                    else:
                        json_str[i]["status"] = "ACTIVE"
                    json_str[i]["ipaddr"] = ipaddr
		
                    json_str[i]["network_name"] = network_name
		
                except Exception,e:
                    LOG.exception('json_str store error because of %s' % e)
                    json_str[i] = {}
                    json_str[i]["name"] = resource_id
                    json_str[i]["id"] = resource_id
                    json_str[i]["iface_id"] = i
                    json_str[i]["status"] = 'UNKNOW'
                    json_str[i]["ipaddr"] = ''
                    json_str[i]["network_name"] = ''

        return json_str

    def getMaxRateDetail(self, meter, project_id, query_time_start, query_time_end):

        allLongID = self.get_instance_long_id_by_project_id(project_id)

        json_str = self.getInstanceJson(project_id)

        for i in allLongID:
            try:
                if meter == 'inrate':
                    meter_name = 'network.incoming.bytes.rate'
                elif meter == 'outrate':
                    meter_name = 'network.outgoing.bytes.rate'
                r = self.db.meter.find({"resource_id": i, "counter_name": meter_name,
                                        "timestamp": {"$lt": query_time_end, "$gt": query_time_start}}).limit(1)
                rate = list(r)[0]["counter_volume"]
            except IndexError:
                #LOG.error('resource statistics list failed line 163')
                rate = 0.0
            if rate == 0.0 and json_str.has_key(i):
                json_str.pop(i)
            if json_str.has_key(i):
                json_str[i][meter] = rate

        return json_str

    def updateDB4Total(self, project_id, end_time, begin_time):
        this_month = datetime.datetime.utcnow().strftime('%Y-%m')
        current_region_project_month = self.cur.execute('select * from netflow where date="%s" and region="%s" '
                                                        'and project_id="%s"' % (this_month, self.region, project_id))
        totalin, totalout = self.getTotalNetFlow(project_id, end_time, begin_time)
        #print 'totalin:%s   totalout:%s' % (totalin, totalout)
        if not current_region_project_month:
            self.cur.execute('insert into netflow(date,total_in,total_out,region,project_id) '
                             'values("%s","%s","%s","%s","%s")' %
                             (this_month, totalin, totalout, self.region, project_id))
            self.conn.commit()
        else:
            self.cur.execute('update netflow set total_in="%s",total_out="%s" where date="%s" '
                             'and region="%s" and project_id="%s"' %
                             (totalin, totalout, this_month, self.region, project_id))
            self.conn.commit()

    def updateDB4Rate(self, project_id, now_time):
        this_month = datetime.datetime.utcnow().strftime('%Y-%m')
        this_date = now_time.strftime('%Y-%m-%d')

        current_region_project_month = self.cur.execute('select * from netflow where date="%s" '
                                                        'and region="%s" and project_id="%s"' %
                                                        (this_month, self.region, project_id))
        max_in_rate, max_out_rate, query_time_start, query_time_end = self.getMaxRate(project_id,now_time)
        self.cur.execute('insert into netrate_project(in_rate,out_rate,date,region,project_id,'
                         'begin_rate_date,end_rate_date) values("%.2f","%.2f","%s","%s","%s","%s","%s")' %
                         (float(max_in_rate), float(max_out_rate), this_date,
                          self.region, project_id, query_time_start, query_time_end))
        self.conn.commit()
        if not current_region_project_month:
            self.cur.execute('insert into netflow(date,max_in_rate,max_out_rate,region,'
                             'project_id,max_in_rate_date,max_out_rate_date) '
                             'values("%s","%s","%s","%s","%s",now(),now())' %
                             (this_month, max_in_rate, max_out_rate, self.region, project_id))
            json_str_in = self.getMaxRateDetail("inrate", project_id, query_time_start, query_time_end)
            json_str_out = self.getMaxRateDetail("outrate", project_id, query_time_start, query_time_end)
            self.cur.execute('insert into netrate_detail(date,json_str,rate_type,'
                             'region,project_id,update_at) values("%s","%s","in",'
                             '"%s","%s",now())' %
                             (this_month, json_str_in, self.region, project_id))
            self.cur.execute('insert into netrate_detail(date,json_str,rate_type,'
                             'region,project_id,update_at) values("%s","%s","out","%s","%s",now())' %
                             (this_month, json_str_out, self.region, project_id))
            self.conn.commit()
        else:
            self.cur.execute('select max_in_rate from netflow where date="%s" '
                             'and region="%s" and project_id="%s"' % (this_month, self.region, project_id))
            current_in_rate = float(self.cur.fetchone()[0])
            if float(max_in_rate) > current_in_rate:
                json_i = self.getMaxRateDetail("inrate", project_id, query_time_start, query_time_end)
                self.cur.execute('update netflow set max_in_rate="%s" , '
                                 'max_in_rate_date=now() where date="%s" and '
                                 'region="%s" and project_id="%s"' %
                                 (max_in_rate, this_month, self.region, project_id))
                exist_in_netrate_detail = self.cur.execute('select * from netrate_detail '
                                                           'where date="%s" and rate_type="in" '
                                                           'and region="%s" and project_id="%s"' %
                                                           (this_month, self.region, project_id))
                if not exist_in_netrate_detail:
                    self.cur.execute('insert into netrate_detail(date,json_str,rate_type,'
                                     'region,project_id,update_at) values("%s","%s","in","%s","%s",now())' %
                                     (this_month, json_i, self.region, project_id))
                else:
                    self.cur.execute('update netrate_detail set json_str="%s", '
                                     'update_at=now()  where date="%s" and rate_type="in" '
                                     'and region="%s" and project_id="%s"' %
                                     (json_i, this_month, self.region, project_id))
                self.conn.commit()

            self.cur.execute('select max_out_rate from netflow where date="%s" and '
                             'region="%s" and project_id="%s"' % (this_month, self.region, project_id))
            current_out_rate = float(self.cur.fetchone()[0])
            if float(max_out_rate) > current_out_rate:
                json_o = self.getMaxRateDetail("outrate", project_id, query_time_start, query_time_end)
                self.cur.execute('update netflow set max_out_rate="%s", max_out_rate_date=now()  '
                                 'where date="%s" and region="%s" and project_id="%s"' %
                                 (max_out_rate, this_month, self.region, project_id))
                exist_out_netrate_detail = self.cur.execute('select * from netrate_detail '
                                                            'where date="%s" and rate_type="out" and '
                                                            'region="%s" and project_id="%s"' %
                                                            (this_month, self.region, project_id))
                if not exist_out_netrate_detail:
                    self.cur.execute('insert into netrate_detail(date,json_str,rate_type,'
                                     'region,project_id,update_at) values("%s","%s","out","%s","%s",now())' %
                                     (this_month, json_o, self.region, project_id))
                else:
                    self.cur.execute('update netrate_detail set json_str="%s", update_at=now()  '
                                     'where date="%s" and rate_type="out" and region="%s" and project_id="%s"' %
                                     (json_o, this_month, self.region, project_id))
                self.conn.commit()


class myThread(threading.Thread):
        def __init__(self, region, project_id, end_time, begin_name):
                super(myThread, self).__init__()
                self.project_id = project_id
                self.begin_time = begin_name
                self.end_time = end_time
                self.region = region

        def run(self):
                instance = NetFlowPoller(self.region)
                instance.updateDB4Total(self.project_id, self.end_time, self.begin_time)
                instance.updateDB4Rate(self.project_id, self.end_time)

def main(region):
        thpool = []
        begin_time = datetime.datetime.strptime(datetime.datetime.utcnow().strftime('%Y-%m-01 00:00:00'),
                                                '%Y-%m-%d %H:%M:%S')
        end_time = datetime.datetime.strptime(datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                                                '%Y-%m-%d %H:%M:%S')
        instance = NetFlowPoller(region)
        p = instance.getProjectList()
        for i in p:
                thpool.append(myThread(region, i, end_time, begin_time))

        for th in thpool:
                th.start()

        for th in thpool:
                th.join()

if __name__ == '__main__':
    if len(sys.argv) == 2:
        region = sys.argv[1].strip()
    
    LOG = mana_log.GetLog('net_flow_poller_%s' % region, __name__)
   
    '''
    try:
        pid = os.fork()
        if pid == 0:
            while True:
                LOG.info('%s poll begin' % region)
                d1 = datetime.datetime.now()

                main(region)

                d2 = datetime.datetime.now()
                d = d2-d1
                LOG.info('%s poll end' % region)
                LOG.info('use %s sec' % d.seconds)
                LOG.info('')
                time.sleep(600-d.seconds)
    except OSError, e:
        LOG.error('%s' % e)
        pass
    '''

    try:
        while True:
            LOG.info('%s poll begin' % region)
            d1 = datetime.datetime.now()

            main(region)

            d2 = datetime.datetime.now()
            d = d2-d1
            LOG.info('%s poll end' % region)
            LOG.info('use %s sec' % d.seconds)
            LOG.info('')
            time.sleep(600-d.seconds)
    except OSError, e:
        LOG.error('%s' % e)
        pass
