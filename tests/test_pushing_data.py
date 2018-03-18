"""
Project: Test pushing data to a receiver
Author: Manuel G
Created: 19/02/2018 12:13
License: MIT
"""

import requests
import gevent
import threading
import time

reciever_name = 'http://130.89.217.201:9763/endpoints/httpReceiver0a22b1d89c7d40b8a2825a8cb9e26ce91'

observation = {'event': {'metaData': {'observation_id': 857, 'result_time': '2016-07-23T02:15:14.000Z', 'symbol': 'ºC'}, 'correlationData': {'event_id': '18ff25ca-a6f0-445f-b56c-ea2dccf8958f'}, 'payloadData': {'Temperature': -32.96, 'x_coord': -3.81364, 'y_coord': 43.45706}}}

# g = gevent.StreamGenerator('http://130.89.217.201:8080/SensorThingsServer/v1.0/Datastreams(4)', '2018-07-23T02:15:14Z', update_frequency=3000)
#
#
# g.start_streaming(reciever_name)

# req = requests.post(reciever_name, json=observation, verify=False)
# print(req)
# req.raise_for_status()

class printer:
    def __init__(self,id_, frequency = 1):
        self.id_ =id_
        self.status = False
        self.frequency = frequency

    def start(self):
        thread = threading.Thread(target=self.printing, args=())
        thread.daemon = True
        thread.start()

    def printing(self):
        self.status = True
        while True:
            print('Hello ', str(self.id_))
            time.sleep(self.frequency)


ids = [1,2,3,4,5]

printers = []
print('instantiation')
for i in ids:
    p = printer(i, 2)
    printers.append(p)

for p in printers:
    p.start()


time.sleep(20)




