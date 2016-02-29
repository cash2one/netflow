#!/usr/bin/python
import sys
#from pymongo import MongoClient  # for windows
from pymongo import Connection # for linux
import MySQLdb
import datetime
import re
import sys
import mana_log
import threading
import ConfigParser
import calendar
from multiprocessing import Process
import keystoneclient.v2_0.client
import novaclient.v1_1.client 
import neutronclient.v2_0.client

NETWORK_NAME = {}

LOG = mana_log.GetLog('net_flow_manager', __name__)

class NetFlowManager(object):
    def __init__(self, region):
        self.project_id_list = []
        self.region_name_dict = {
            
                                  "Nanhui": "172.30.250.135",
                                  "zhenru": "172.29.203.64",
                                  "beijing": "172.28.5.122"
        }
        self.mhost = self.region_name_dict[region]
        self.region = region
        self.mconn = Connection(self.mhost, 27017)
        self.db = self.mconn['ceilometer']
        self.conn = MySQLdb.connect(host='localhost',
                                    user='cloud',
                                    passwd='test',
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

    def get_instance_long_id_by_project_id(self, project_id):
        allLongID = []
        for i in list(
                self.db.resource.find({"meter.counter_name": "network.outgoing.bytes", "project_id": project_id})):
            allLongID.append(i["_id"])
        return allLongID

    def getMaxRate(self, project_id, begin_time, until_time, begin_time_1, until_time_1):
        begin_time_obj = begin_time
        end_time_obj = until_time
        max_in_rate = 0.0
        max_out_rate = 0.0
        this_date = end_time_obj.strftime('%Y-%m-%d')
        # end_time_obj_plus_10m = end_time_obj + delta
        # record_exist = cur.execute('select id from netrate_project where begin_rate_date <= "%s" and
        # end_rate_date >= "%s" and region = "%s" and project_id = "%s"' % (end_time_obj, end_time_obj,
        # self.region, project_id))
        record_exist = self.cur.execute('select id from netrate_project where begin_rate_date '
                                        'between "%s" and "%s" and end_rate_date between "%s" '
                                        'and "%s" and region = "%s" and project_id = "%s"' %
                                        (begin_time_obj, end_time_obj, begin_time_1, until_time_1, self.region,
                                         project_id))

        if not record_exist:
            try:
                q = self.db.meter.find({"project_id": project_id, "counter_name": "network.incoming.bytes.rate",
                                        "timestamp": {"$lte": end_time_obj, "$gte": begin_time_obj}})
                for i in list(q):
                    if NETWORK_NAME.has_key(i["resource_id"]):
                        network_name = NETWORK_NAME[i["resource_id"]]
                        if network_name and self.networkIsMatch(network_name):
                            max_in_rate += i["counter_volume"]
                        else:
                            continue
                    else:
                        network_name = self.getNetworkNameByLongID(i["resource_id"])
                        NETWORK_NAME[i["resource_id"]] = network_name
                        if network_name and self.networkIsMatch(network_name):
                            max_in_rate += i["counter_volume"]
                        else:
                            continue

            except IndexError:
                print 'project statistics list failed'
                max_in_rate = 0.0

            try:
                q = self.db.meter.find({"project_id": project_id, "counter_name": "network.outgoing.bytes.rate",
                                        "timestamp": {"$lte": end_time_obj, "$gte": begin_time_obj}})
                for i in list(q):
                    if NETWORK_NAME.has_key(i["resource_id"]):
                        network_name = NETWORK_NAME[i["resource_id"]]
                        if network_name and self.networkIsMatch(network_name):
                            max_out_rate += i["counter_volume"]
                        else:
                            continue
                    else:
                        network_name = self.getNetworkNameByLongID(i["resource_id"])
                        NETWORK_NAME[i["resource_id"]] = network_name
                        if network_name and self.networkIsMatch(network_name):
                            max_out_rate += i["counter_volume"]
                        else:
                            continue

            except IndexError:
                LOG.error('project statistics list failed')
                max_in_rate = 0.0

            self.cur.execute('insert into netrate_project(in_rate,out_rate,date,region,project_id,'
                             'begin_rate_date,end_rate_date) '
                             'values("%.2f","%.2f","%s","%s","%s","%s","%s")' %
                             (float(max_in_rate), float(max_out_rate), this_date, self.region, project_id,
                              begin_time_obj, begin_time_1))
            LOG.info('insert into netrate_project(in_rate,out_rate,date,region,project_id,' \
                  'begin_rate_date,end_rate_date) ' \
                  'values("%.2f","%.2f","%s","%s","%s","%s","%s")' % \
                  (float(max_in_rate), float(max_out_rate), this_date, self.region, project_id, begin_time_obj,
                   begin_time_1))
            self.conn.commit()

            # #######################
            # select * from netrate_project where begin_rate_date < '2015-08-11 10:10:00'
            # and end_rate_date > '2015-08-11 10:10:00';

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

    def insertMonthlyData(self, project_id, month):
        month_list = month.split('-')

        week, month_last_day = calendar.monthrange(int(month_list[0]), int(month_list[1]))

        current_region_project_month = self.cur.execute(
            'select * from netflow where date="%s" and region="%s" and project_id="%s"' % (
                month, self.region, project_id))

        if not current_region_project_month:
            self.cur.execute('select max(in_rate) from netrate_project where region = "%s" '
                             'and date >= "%s-01" and date <="%s-%s" and project_id="%s"' %
                             (self.region, month, month, month_last_day, project_id))
            max_in_rate = self.cur.fetchone()[0]
            allLongID = self.get_instance_long_id_by_project_id(project_id)
            if max_in_rate:
                self.cur.execute('select begin_rate_date,end_rate_date from netrate_project '
                                 'where in_rate = "%s" and region = "%s" and project_id="%s" '
                                 'and date >= "%s-01" and date <="%s-%s"' %
                                 (max_in_rate, self.region, project_id, month, month, month_last_day))
                max_in_rate_begin_date, max_in_rate_end_date = self.cur.fetchone()
                json_str_in = self.getInstanceJson(project_id)
                for i in allLongID:
                    try:
                        r = self.db.meter.find({
                            "resource_id": i,
                            "counter_name": "network.incoming.bytes.rate",
                            "timestamp": {"$lt": max_in_rate_end_date, "$gt": max_in_rate_begin_date}
                        }).limit(1)
                        rate = list(r)[0]["counter_volume"]
                    except IndexError:
                        rate = 0.0

                    if rate == 0.0 and json_str_in.has_key(i):
                        json_str_in.pop(i)
                    if json_str_in.has_key(i):
                        json_str_in[i]["inrate"] = rate

                self.cur.execute('insert into netrate_detail(date,json_str,rate_type,region,project_id,update_at) '
                                 'values("%s","%s","in","%s","%s","%s")' %
                                 (month, json_str_in, self.region, project_id, max_in_rate_end_date))
                self.conn.commit()
            else:
                max_in_rate_end_date = None
            # ################# out_rate #############################
            self.cur.execute('select max(out_rate) from netrate_project where region = "%s" '
                             'and date >= "%s-01" and date <="%s-%s" and project_id="%s"' %
                             (self.region, month, month, month_last_day, project_id))
            max_out_rate = self.cur.fetchone()[0]
            if max_out_rate:
                self.cur.execute('select begin_rate_date,end_rate_date from netrate_project '
                                 'where out_rate = "%s" and region = "%s" and project_id="%s" '
                                 'and date >= "%s-01" and date <="%s-%s"' %
                                 (max_out_rate, self.region, project_id, month, month, month_last_day))
                max_out_rate_begin_date, max_out_rate_end_date = self.cur.fetchone()

                json_str_out = self.getInstanceJson(project_id)
                for i in allLongID:
                    try:
                        r = self.db.meter.find({
                            "resource_id": i,
                            "counter_name": "network.outgoing.bytes.rate",
                            "timestamp": {"$lt": max_out_rate_end_date, "$gt": max_out_rate_begin_date}
                        }).limit(1)
                        rate = list(r)[0]["counter_volume"]
                    except IndexError:
                        rate = 0.0

                    if rate == 0.0 and json_str_out.has_key(i):
                        json_str_out.pop(i)
                    if json_str_out.has_key(i):
                        json_str_out[i]["outrate"] = rate

                self.cur.execute('insert into netrate_detail(date,json_str,rate_type,region,project_id,update_at) '
                                 'values("%s","%s","out","%s","%s","%s")' %
                                 (month, json_str_out, self.region, project_id, max_out_rate_end_date))
            else:
                max_out_rate_end_date = None

            if max_out_rate or max_in_rate:
                self.cur.execute(
                    'insert into netflow(date,max_in_rate,max_out_rate,region,'
                    'project_id,max_in_rate_date,max_out_rate_date) '
                    'values("%s","%s","%s","%s","%s","%s","%s")' %
                    (month, max_in_rate, max_out_rate, self.region, project_id, max_in_rate_end_date,
                     max_out_rate_end_date))

            self.conn.commit()

        else:
            self.cur.execute(
                'select max_in_rate,max_out_rate from netflow where '
                'date="%s" and region="%s" and project_id="%s"' %
                (month, self.region, project_id))
            now_max_in_rate, now_max_out_rate = self.cur.fetchone()

            self.cur.execute('select max(in_rate) from netrate_project where region = "%s" '
                             'and date >= "%s-01" and date <="%s-%s" and project_id="%s"' %
                             (self.region, month, month, month_last_day, project_id))
            max_in_rate = self.cur.fetchone()[0]

            if max_in_rate > now_max_in_rate:
                self.cur.execute('select begin_rate_date,end_rate_date from netrate_project '
                                 'where in_rate = "%s" and region = "%s" and project_id="%s" '
                                 'and date >= "%s-01" and date <="%s-%s"' %
                                 (max_in_rate, self.region, project_id, month, month, month_last_day))
                max_in_rate_begin_date, max_in_rate_end_date = self.cur.fetchone()

                allLongID = self.get_instance_long_id_by_project_id(project_id)
                json_str_in = self.getInstanceJson(project_id)
                for i in allLongID:
                    try:
                        r = self.db.meter.find({
                            "resource_id": i,
                            "counter_name": "network.incoming.bytes.rate",
                            "timestamp": {"$lt": max_in_rate_end_date, "$gt": max_in_rate_begin_date}
                        }).limit(1)
                        rate = list(r)[0]["counter_volume"]
                    except IndexError:
                        rate = 0.0

                    if rate == 0.0 and json_str_in.has_key(i):
                        json_str_in.pop(i)
                    if json_str_in.has_key(i):
                        json_str_in[i]["inrate"] = rate
                self.cur.execute('update netrate_detail set json_str="%s",update_at="%s" where date="%s"'
                                 ' and rate_type="in" and region="%s" and project_id="%s"' %
                                 (json_str_in, max_in_rate_end_date, month, self.region, project_id))
                self.cur.execute('update netflow set max_in_rate="%s",max_in_rate_date="%s" where date="%s"'
                                 ' and region="%s" and project_id="%s"' %
                                 (max_in_rate, max_in_rate_end_date, month, self.region, project_id))
                LOG.info('update netflow set max_in_rate="%s",max_in_rate_date="%s" where date="%s"'
                         ' and region="%s" and project_id="%s" (old data:%s )' %
                         (max_in_rate, max_in_rate_end_date, month, self.region, project_id, now_max_in_rate))
                self.conn.commit()
            # #################out_rate#############################
            self.cur.execute('select max(out_rate) from netrate_project where region = "%s" '
                             'and date >= "%s-01" and date <="%s-%s" and project_id="%s"' %
                             (self.region, month, month, month_last_day, project_id))
            max_out_rate = self.cur.fetchone()[0]

            if max_out_rate > now_max_out_rate:
                self.cur.execute('select begin_rate_date,end_rate_date from netrate_project '
                                 'where out_rate = "%s" and region = "%s" and project_id="%s" '
                                 'and date >= "%s-01" and date <="%s-%s"' %
                                 (max_out_rate, self.region, project_id, month, month, month_last_day))
                max_out_rate_begin_date, max_out_rate_end_date = self.cur.fetchone()

                allLongID = self.get_instance_long_id_by_project_id(project_id)
                json_str_out = self.getInstanceJson(project_id)
                for i in allLongID:
                    try:
                        r = self.db.meter.find({
                            "resource_id": i,
                            "counter_name": "network.outgoing.bytes.rate",
                            "timestamp": {"$lt": max_out_rate_end_date, "$gt": max_out_rate_begin_date}
                        }).limit(1)
                        rate = list(r)[0]["counter_volume"]
                    except IndexError:
                        rate = 0.0

                    if rate == 0.0 and json_str_out.has_key(i):
                        json_str_out.pop(i)
                    if json_str_out.has_key(i):
                        json_str_out[i]["outrate"] = rate

                self.cur.execute('update netrate_detail set json_str="%s",update_at="%s" where date="%s"'
                                 ' and rate_type="out" and region="%s" and project_id="%s"' %
                                 (json_str_out, max_out_rate_end_date, month, self.region, project_id))
                self.cur.execute('update netflow set max_out_rate="%s",max_out_rate_date="%s" where date="%s"'
                                 ' and region="%s" and project_id="%s"' %
                                 (max_out_rate, max_out_rate_end_date, month, self.region, project_id))

                LOG.info('update netflow set max_out_rate="%s",max_out_rate_date="%s" where date="%s"'
                         ' and region="%s" and project_id="%s" (old data: %s)' %
                        (max_out_rate, max_out_rate_end_date, month, self.region, project_id, now_max_out_rate))
                self.conn.commit()


class monthThread(threading.Thread):
    def __init__(self, region, project_id, month):
        super(monthThread, self).__init__()
        self.project_id = project_id
        self.month = month
        self.region = region

    def run(self):
        instance = NetFlowManager(self.region)
        instance.insertMonthlyData(self.project_id, self.month)


def tasks(region, project_id, time_list):
    instance = NetFlowManager(region)
    for i, j, m, n in time_list:
        instance.getMaxRate(project_id, i, j, m, n)


def getTimeList(begin_time, end_time):
    delta_599 = datetime.timedelta(seconds=599)
    delta = datetime.timedelta(seconds=600)
    time_list = []
    single_time_list = []
    start = begin_time
    end = begin_time + delta
    while end <= end_time:
        start_1 = start + delta_599
        end_1 = end + delta_599
        single_time_list = [start, start_1, end, end_1]
        time_list.append(single_time_list)
        start = end
        end = end + delta

    return time_list


def works(func, region, p_id):
    proc_record = []
    delta = datetime.timedelta(days=1)
    now = datetime.datetime.utcnow()
    yesterday = now - delta
    today_begin = now.strftime('%Y-%m-%d 00:00:00')
    yesterday_begin = yesterday.strftime('%Y-%m-%d 00:00:00')
    begin_time = datetime.datetime.strptime(yesterday_begin, '%Y-%m-%d %H:%M:%S')
    today = datetime.datetime.strptime(today_begin, '%Y-%m-%d %H:%M:%S')
    end_time = begin_time + delta

    # define $begin_time and $today can change range date
    while end_time <= today:
        time_list = getTimeList(begin_time, end_time)
        for i in p_id:
            p = Process(target=func, args=(region, i, time_list))
            p.start()
            proc_record.append(p)
        if end_time == today:
            break
        begin_time = end_time
        end_time = end_time + delta
        if end_time > today:
            end_time = today

    for p in proc_record:
        p.join()


def updateMonthData(region, month):
    thpool = []
    instance = NetFlowManager(region)
    p_id = instance.getProjectList()
    for i in p_id:
        thpool.append(monthThread(region, i, month))
    for th in thpool:
        th.start()
    for th in thpool:
        th.join()


if __name__ == '__main__':
    # insert netrate_project use Process()
    # insert netflow and netrate_detail use Thread()
    if len(sys.argv) == 2:
        LOG.info('########################## Start ###########################')
        region = sys.argv[1].strip()
        # begin_time = datetime.datetime.strptime('2015-05-01 00:00:00','%Y-%m-%d %H:%M:%S')
        instance = NetFlowManager(region)
        p_id = instance.getProjectList()
        works(tasks, region, p_id)
        month = datetime.datetime.utcnow().strftime('%Y-%m')
        updateMonthData(region, month)
        LOG.info('######################### End ############################')

    if len(sys.argv) == 3:
        region = sys.argv[1].strip()
        month = sys.argv[2].strip()
        updateMonthData(region, month)
