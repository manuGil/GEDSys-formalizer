"""
Routine for instantiating a simple geographic event into GEDSys.
"""

import json
from bin import gevent
import socket
import time
import logging

# config:
threads = 1
update_interval = 5 # For buffer. seconds
cycles = 1

log = logging.getLogger('formalizer')
log.setLevel(logging.INFO)
fh = logging.FileHandler('../logs/sensor_vs_time.log')
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(created)f | %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
log.addHandler(fh)
log.addHandler(ch)


log.info('STARTING EXECUTION: sensor vs time')
log.info('Configuration: threads (%s), cycles (%s), update interval (%s)' % (str(threads), str(cycles), str(update_interval)))

# 1. sensor api
sensor_api = gevent.SensorApi('smart-santander', 'http://130.89.217.201:8080/frost-server/v1.0')
print(sensor_api.test())

# 2. configuration file
conf_f = '../bin/config.json'
with open(conf_f) as c:
    cf = c.read()
    conf = json.loads(cf)

log.info('start intantiation')

# 3. Open event definition file
e_def = '../tests/simple_event_def.json'
with open(e_def) as ed:
    f = ed.read()
    f = json.loads(f)

# 4. create g-event
e = gevent.GEvent(f)

# 6. create event handler per g-event
handler = gevent.EventHandler(e, conf)

# 7. Deploy configuration files in CEP
# 7.1 URL target for publisher
publisher_target = 'http://' + socket.gethostbyname(socket.gethostname()) + ':9090'
log.info('end instantiation')

# 7.2 deploy to CEP
log.info('start cep config deployment')
handler.deploy_cep_configuration(publisher_target)
delay = 10
print('.....Waiting for deployment %s seconds....' % delay)
time.sleep(delay)  # (seconds) induce delay, to allow CEP server to deploy files

log.info('End cep config deployment')

# 8. find receiver endpoint:
name = handler.deployed_files["receivers"][0]

pos = name.find('receiver-')
name = name[pos + 9:]
pos2 = name.find('_')
name = name[:pos2]
# print(name)
re = conf["geosmart.sys"]["cep"]["root url"] + '/' + 'httpReceiver' + name
re = re + '1'

# 9. Crate observations buffer
data_request = gevent.prepare_observations_request(sensor_api.url, e.extent, e.phenomena_names()[0])
log.info("Buffering data")
data_buffer = gevent.Buffer(data_request, update_interval)
log.info("Data buffer ready")
log.info("Number of sensors in buffer: %s", str(len(data_buffer.data)))

expiration = '2018-12-31T10:00:00Z'
generators = []

g = gevent.StreamGenerator(data_buffer.data, expiration, re, update_frequency=0)

log.info("number of generators created: %s", str(len(generators)))

# 10 Starts stream using a thread per generator:
log.info('Start streaming')

s_start = time.time()

g.stream_to_cep(workers=threads)

e_end = time.time()

log.info("Total data push time (s):" + str(e_end - s_start))

# print('buffer size: ', data_buffer.size, ' ET streaming time: ', (e_end - s_start))

# 11 un-deploy configuration files
handler.undeploy_cep_configuration()
log.info("Un-deployed files for gevent: " +str(g._id))

# ('process has finished')
