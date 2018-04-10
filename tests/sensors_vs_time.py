"""
Accounting for perfomance when using multithreading to push data in to CEP

Using one simple event.
Every test was performed twice, to account to the time delays when instantiating the resources for first time.

Endpoint: HTTP server with 100 threads

"""

import json
from bin import gevent
import socket
import concurrent.futures
import time
import datetime
import logging

# config:
# no_sensors =
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
e_def = '../tests/composite_event_def.json'
with open(e_def) as ed:
    f = ed.read()
    f = json.loads(f)

# 4. check connection to CEP service
# currently it raise a SSL verification error due to an old SSL certificate in the server.
# spkipping this step for now
# try:
#     gevent.test_remote_connection(conf["geosmart.sys"]["cep"]["root url"])
# except Exception as e:
#     raise ('CEP server connection raised an exception', e)

# 5. create g-event
e = gevent.GEvent(f)

# 6. create event handler per g-event
handler = gevent.EventHandler(e, conf)

# 7. Deploy configuration files in CEP
# 7.1 URL target for publisher
publisher_target = 'http://' + socket.gethostbyname(socket.gethostname()) + ':9090'

log.info('end instantiation')

# 7.2 deployment

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
# print('data will be send to: ', re)

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

print('buffer size: ', data_buffer.size, ' ET streaming time: ', (e_end - s_start))

# time  script will keep running
# log.info('Checkpoint, sleep time started')
# if no_sensors < 100:
#     time.sleep(5+no_sensors*1.2)  # If time is too small streaming will be terminated before completed
# elif no_sensors < 200:
#     time.sleep(no_sensors/2)
# elif no_sensors < 1000:
#     time.sleep(no_sensors/4)
# else:
#     time.sleep(no_sensors/10)

# 11 Undeploy configuration files
handler.undeploy_cep_configuration()

print('process has finished')
