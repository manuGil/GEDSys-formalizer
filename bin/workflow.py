
import cep
import json
import gevent
import socket
import concurrent.futures
import datetime
import time

start = datetime.datetime.now()
print('process started at: ', start)

# 1. sensor api
data = gevent.SensorApi('smart-santander', 'http://130.89.217.201:8080/SensorThingsServer/v1.0')
print(data.test())

# 2. configuration file
conf_f = './config.json'
with open(conf_f) as c:
    cf = c.read()
    conf = json.loads(cf)

# 3. Open event definition file
e_def = '../tests/event_def_test.json'
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
publisher_target = 'http://' + socket.gethostbyname(socket.gethostname()) + ':80'

# 7.2 deployment
start_deploy = datetime.datetime.now()
print('deploying cep files at: ', start_deploy - start)
handler.deploy_cep_configuration(publisher_target)
delay = 10
print('.....Waiting for deployment %s seconds....' % delay)
time.sleep(delay) # (seconds) induce delay, to allow CEP server to deploy files


# 8. find receiver endpoint:
name = handler.deployed_files["receivers"][0]
print('raw name ', name)
pos = name.find('receiver-')
name = name[pos + 9:]
pos2 = name.find('_')
name = name[:pos2]
# print(name)
re = conf["geosmart.sys"]["cep"]["root url"] + '/' + 'httpReceiver' + name
re = re + '1'
print('data will be send to: ', re)

# 9. create stream generators
# print(e.extent)
stream_ids = gevent.find_datastreams(data.url, e.extent, e.phenomena_names()[0])

# name must match name in Sensor API
expiration = '2018-12-31T10:00:00Z'
generators = []
for s in stream_ids:
    g = gevent.StreamGenerator(s, expiration, re)
    generators.append(g)

# 10 Starts stream using a thread per generator:

print('streaming started at: ', datetime.datetime.now() - start)
with concurrent.futures.ThreadPoolExecutor(max_workers=len(generators)) as executor:
    future_to_generator = {executor.submit(g.start_streaming()): g for g in generators}
    for future in concurrent.futures.as_completed(future_to_generator):
        generator = future_to_generator[future]
        try:
            data = future.done()
        except Exception as exc:
            print('%r generated an exception: %s' % (generator,exc))
        else:
            print('%s was started' % generator)

# time  script will keep running
time.sleep(40)


# 11 Undelploy configuration files
handler.undeploy_cep_configuration()

print('process has finish')
