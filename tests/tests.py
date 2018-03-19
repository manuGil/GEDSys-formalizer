
import unittest

import json
from bin import gevent as ge
from bin import cep
import os
import copy
import tempfile

file_dir = os.path.dirname(__file__)
test_event = file_dir + '\event_def_test.json' # test data
with open(test_event) as file:
    def_string = file.read()
    definition = json.loads(def_string)

conf_file = 'C:/phd_dev/GeoSmart_sys/Formalizer/bin/config.json'
with open(conf_file) as ffile:
    open_file = ffile.read()
    conf = json.loads(open_file)

reciever_url = 'http://localhost:9763/endpoints/httpReceiver001'

stream_def_test= {
  "name": "geosmart.remote.test100",
  "version": "1.0.0",
  "nickName": "streamTest",
  "description": "stream test",
  "metaData": [
    {
      "name": "observation_id",
      "type": "LONG"
    },
    {
      "name": "result_time",
      "type": "STRING"
    },
    {
      "name": "symbol",
      "type": "STRING"
    }
  ],
  "correlationData": [
    {
      "name": "generator_id",
      "type": "STRING"
    }
  ],
  "payloadData": [
    {
      "name": "temperature",
      "type": "DOUBLE"
    },
    {
      "name": "x_coord",
      "type": "DOUBLE"
    },
    {
      "name": "y_coord",
      "type": "DOUBLE"
    }
  ]
}

class TestGevent(unittest.TestCase):

    def test_Gevent(self):
        event = ge.GEvent(definition)
        self.assertIsInstance(event, ge.GEvent, 'Event definition is not correct')

    def test_phenomena_names_Gevent(self):
        event = ge.GEvent(definition)
        names = event.phenomena_names()
        self.assertEqual(names, ['Temperature'], 'Function phenomena_names() failed to produce the right output')

    def test_EventHandler(self):
        gevent = ge.GEvent(definition)
        handler = ge.EventHandler(gevent, conf)
        self.assertIsInstance(handler, ge.EventHandler, 'Failed to instantiate EventHandler')

        # test file deployment
        url_target = 'http://localhost:80'
        deploy = handler.deploy_cep_configuration(url_target)
        self.assertTrue(deploy, 'Some files were not deployed. Check the log for details')

        # # test file undeploy
        # undeploy = handler.undeploy_cep_configuration()
        # self.assertFalse(undeploy, 'undeploy_cep_configuration() failed to remove files')

    def test_StreamGenerator(self):
        expiration = '2018-01-31T10:00:00Z'
        g = ge.StreamGenerator('http://130.89.217.201:8080/SensorThingsServer/v1.0/Datastreams(4)', expiration, receiver_endpoint=reciever_url)
        self.assertIsInstance(g, ge.StreamGenerator, 'Failed to instantiate StreamGenerator')

        # test CEP connection


class TesCep(unittest.TestCase):

    def test_define_receiver(self):
        receiver_test = '''<?xml version="1.0" encoding="UTF-8"?><eventReceiver name="httpReceiver001" statistics="enable" trace="enable" xmlns="http://wso2.org/carbon/eventreceiver"> <from eventAdapterType="http"> <property name="transports">all</property> <property name="basicAuthEnabled">true</property> </from> <mapping customMapping="disable" type="json"/> <to streamName="geosmart.test001" version="1.0.0"/> </eventReceiver>'''
        id_ = "001"
        stream_name = "geosmart.test001"
        version = "1.0.0"
        self.maxDiff = None
        receiver = cep.define_receiver(id_, stream_name, version)
        self.assertMultiLineEqual(receiver_test, receiver, 'define_receiver() failed to produce the right format')

    def test_map_stream_to_processor(self):
        valid_stream = '(meta_observation_id long, meta_result_time string, meta_symbol string, correlation_generator_id string, temperature double, x_coord double, y_coord double)'

        mapped_stream = cep.map_stream_to_processor(stream_def_test)
        self.assertMultiLineEqual(valid_stream, mapped_stream, 'mapped stream does not match valid stream sample')

    def test_define_stream(self):
        name = 'remote.test100'
        phenomenon = {'name': 'temperature', 'data type': 'DOUBLE'}
        stream = cep.define_stream(name, phenomenon, version='1.0.0', description='')
        # print(stream)

        self.maxDiff = None
        # self.assertEqual(stream_def_test, stream, 'define_streams() did not produced a valid definition')

    def test_define_execution_plan(self):
        input_streams = [stream_def_test]
        output_stream = copy.deepcopy(stream_def_test)
        condition = definition['properties']['attributive']['conditions'].items().__iter__()
        output_stream['name'] = 'Geosmart.output'
        query = 'from inputs [temperature > 20] select meta_observation_id, meta_result_time, meta_symbol, correlation_generator_id, temperature, x_coord, y_coord insert into output_1'
        execution_plan = cep.define_execution_plan('executionPlan001', input_streams, output_stream, condition.__next__())
        # print(execution_plan)


    def test_define_event_publisher(self):

        ui_pub = cep.define_event_publisher('uiPublisher', 'geosmart.test', '1.0.0', type_='ui')
        # TODO: add assertion
        http_pub = cep.define_event_publisher('httpPublisher', 'geosmart.test', '1.0.0', type_='http', target_url='https://host:port/endpoint')
        # print(http_pub)
        # todo: add assertion

    def test_cep_deploy_undeploy(self):
        stream_dir = conf["geosmart.sys"]["cep"]['home directory'] + conf["geosmart.sys"]["cep"]["stream subdir"] + '/geosmart.remote.test_1.0.0.json'

        cep_cof = conf["geosmart.sys"]["cep"]
        handler_cof = conf["geosmart.sys"]["handler"]

        j = json.dumps(stream_def_test)
        with tempfile.TemporaryFile('w+') as fo:
            fo.write(j)
            fo.seek(0) # set pointer at the beginning of the file
            upload_cep = cep.upload_to_cep(stream_dir, fo, cep_cof, handler_cof)
            self.assertTrue(upload_cep, 'Stream file was not created in the CEP server')

        remove_cep = cep.remove_from_cep(stream_dir, cep_cof, handler_cof)
        self.assertTrue(remove_cep, 'Stream file was not removed from the CEP server')

    def test_cep_query(self):
        test_query = 'from inputs [Temperature > -1000] select * insert into outputs'
        condition = definition['properties']['attributive']['conditions'].items().__iter__()
        query = cep.cep_query(condition.__next__(), 'inputs', 'outputs')
        self.assertMultiLineEqual(query, test_query, 'Query does not have the right format')

if __name__ == '__main__':
    unittest.main()

