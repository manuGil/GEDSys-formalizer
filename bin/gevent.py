"""
Project: Formalizer's implementation
Author: ManuelG
Created: 02-Jan-18 17:55
License: MIT
"""

from abc import ABCMeta, abstractmethod
import uuid
import datetime
import shapely.geos as geos
import shapely.wkt as wkt
import requests
from bin import cep
import time
import tempfile
import json
import threading
import logging

log = logging.getLogger('formalizer')
log.setLevel(logging.INFO)


class SensorApi(object):
    """
    Data source providing sensor data using the SensorThingAPI standard
    """

    def __init__(self, name, root_url):
        self.name = name
        self.url = root_url

    def test(self):
        """
        Prints response code of a get request. 200 == successful connection
        """
        response = requests.get(self.url)
        return response.status_code


class User(object):
    """
    Abstract class for user
    Attributes:
    """

    __metaclass__ = ABCMeta

    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.user_id = 'user-'+uuid.uuid4()

    def change_password(self, username, old_password, new_password):
        if self.username == username:
            if old_password == self.password:
                self.password = new_password
                print('You password was successfully change')
            else:
                print('Your old password and username do not match. Try again')
        else:
            print("Your user name  do not exist. Check the correct spelling")

    @abstractmethod
    def user_type(self):
        ''' Returns a string with the type of user'''
        pass


class Citizen(User):
    """
    Attributes
    """
    motivation = 'Transient'

    def user_type(self):
        return 'citizen'


class Businessman(User):
    """
    Attributes
    """
    motivation = 'Profitability'

    def user_type(self):
        return 'businessman'


class CityAdmin(User):
    """
    Attributes
    """
    motivation = 'City efficiency'

    def user_type(self):
        return 'city administrator'


def is_valid_time_interval(time_interval):
    """
    Validate format and validity of a time interval.
    :param time_interval: string describing a time interval as ISO 8601 (YYYY-MM-dd:hh:mm:ss)
    :return: validity of time interval
    """

    ind = time_interval.find('/')
    if ind != -1:
        start_time = datetime.datetime.strptime(time_interval[:ind], "%Y-%m-%dT%H:%M:%SZ")
        end_time = datetime.datetime.strptime(time_interval[ind + 1:], "%Y-%m-%dT%H:%M:%SZ")
        if end_time > start_time:
            return True
        else:
            print("Time interval is not valid")
            return False
    else:
        print("Time format is not valid, check event definition")
        return False


def is_valid_wkt_polygon(wkt_geometry):
    """
    Checks if a string contains a valid geometry express in Well Known Text
    :param wkt_geometry: well know text
    :return: True if geometry is valid
    """

    try:
        geometry = wkt.loads(wkt_geometry)
    except geos.WKTReadingError:
        return False
    else:
        if geometry.geom_type != 'Polygon':
            return False
    return True


def find_datastreams(sensor_api_root, extent, phenomenon, event_object=''):
    """
    Identifies datastreams in the Sensor API which match spatial and phenomenon filters
    :param sensor_api_root: root url to the sensor api
    :param extent: Spatial feature use to as bounding box in WKT (Well Known Text). A polygon
    :param phenomenon: name of observable property
    :param event_object: event object
    :return: list of datastreams  URIS
    """

    # example of valid request:
    '''http: // 130.89.217.201:8080/SensorThingsServer/v1.0/Datastreams/$ref?$top=100&
    $filter=geo.intersects(Things/Locations/location,geography
    'POLYGON((-4 42, -3.8 43.5, 1 44, 1 42.5, -4 42))') and 
    Datastreams/ObservedProperty/name eq 'Luminosity'
    '''

    page_size = 100  # max page size. Max page size depends on API settings
    if is_valid_wkt_polygon(extent):
        request_uri = sensor_api_root + "/Datastreams/$ref?$top=100" + \
              "&$filter=geo.intersects(Things/Locations/location,geography'" + \
              extent + "')" + \
              " and Datastreams/ObservedProperty/name eq '" + phenomenon + "'"
        print(request_uri)

        try:
            request = requests.get(url=request_uri)  #json object

        except requests.HTTPError:
            print('HTTP error for the request: ' + str(request_uri))
    else:
        print('Extent definition is not valid')
        return

    # make first request

    response_json = request.json()
    # collect data
    things_collector = response_json["value"] # collect things from all responses. A list

    make_request = True
    # retrieve data from all pages
    while make_request:
        if '@iot.nextLink' in response_json:
            request = requests.get(response_json['@iot.nextLink'])
            response_json = request.json()
            things_collector = things_collector + response_json['value']
        else:
            make_request = False

    # collect a list of ids
    ids = []
    for thing in things_collector:
        ids.append(thing['@iot.selfLink'])
    return ids


def get_xy_coord(location_json):
    """
    extract coordinates out of a location record in the Sensor API
    :param location_json: JSON object describing a location
    :return: location as [x, y]
    """
    # request = datastream_uri + '/Thing/Locations?$top=1'
    # response = requests.get(request)

    response = location_json['value'][0]['location']['coordinates']

    return response

def prepare_observations_request(sensor_api_root, extent, phenomenon, page_size=200):
    """
       Prepares a http request to retrieve latest observations from all Things (sensing devices) that intersect the extent and belong to phenomena
       :param sensor_api_root: root url to the API
       :param extent: WKT of a polygon
       :param phenomenon: Name of the phenomena, case sensitive
       :param page_size: page size for handling pagination. Max page size depends on API settings.
       Depending on the amount of data involved; big values will increase response  time,
       small values will increase number of individual requests
       :return: http request
    """

    if is_valid_wkt_polygon(extent):
        observations_request = sensor_api_root + "/Things?$top="+ str(page_size) + \
            "&$select=name,@iot.id&$expand=Datastreams($select=@iot.selflink," + \
            "unitOfMeasurement;$expand=Observations($orderby=phenomenonTime desc;$top=1))," + \
                      "Locations($select=location)&$filter=geo.intersects(Things/Locations/location," + \
                      "geography'" + extent + "')" + \
                      " and Datastreams/ObservedProperty/name eq '" + phenomenon + "'"

        # Construct test request, will retrieve only one item
        test_request = sensor_api_root + "/Things?$top=1" + \
            "&$select=name,@iot.id&$expand=Datastreams($select=@iot.selflink," + \
            "unitOfMeasurement;$expand=Observations($orderby=phenomenonTime desc;$top=1))," + \
                      "Locations($select=location)&$filter=geo.intersects(Things/Locations/location," + \
                      "geography'" + extent + "')" + \
                      " and Datastreams/ObservedProperty/name eq '" + phenomenon + "'"

        try:
            test = requests.get(url=test_request)  # json object

        except requests.HTTPError:
            print('HTTP error for the request: ' + str(observations_request))
    else:
        print('Extent definition is not valid')
        return
    return observations_request


def collect_observations(observations_request):
    """
    Retrieve latest observations from all Things (sensing devices) that intersect the extent and belong to phenomena
    :param observations_request: prepared observations request
    :return: A list  of things, their locations and the latest observation
    """

    try:
            request = requests.get(url=observations_request)  # json object
    except requests.HTTPError:
            print('HTTP error for the request: ' + str(observations_request))
    else:
        # make first request

        response_json = request.json()
        # collect data
        observations = response_json["value"]  # collect things from all responses. A list

        make_request = True
        # retrieve data from all pages
        while make_request:
            if '@iot.nextLink' in response_json:
                request = requests.get(response_json['@iot.nextLink'])
                response_json = request.json()
                observations = observations + response_json['value']
            else:
                make_request = False
        return observations



def test_remote_connection(url):
    """ Test if a remote HTTP connection is listening
    :param url: valid URL for the connection
    """
    r = requests.get(url)
    r.raise_for_status()
    return True


class ObservationsBuffer:
    """
    Buffers a list of observations from the SensorThingAPI

    Attributes:
        request: request definition to collect observations
        data: list of observations
        update_interval: time interval in seconds
        control = control the start and stop of auto_update
    Methods:
        update_data: Update list of observations in data attribute
        auto_update: continuously update the buffer given a time time interval
    """

    def __init__(self, request, update_interval):
        self.data = None
        self.request = request
        self.last_update = None
        self.update_interval = update_interval
        # self.control = 'stop'

        try:
            self.data = collect_observations(request)
        except Exception as e:
            print('Requesting data raised an exception: ', e)
        else:
            self.last_update = datetime.datetime.now().isoformat()

    def update_data(self):
        """
        Update buffer manually
        :return:
        """
        # if self.control == 'stopped':
        try:
            self.data = collect_observations(self.request)
        except Exception as e:
            print('Requesting data raised an exception: ', e)
        else:
            self.last_update = datetime.datetime.now().isoformat()
        # else:
        #     print('Auto update is running. This function call has no effect')
        #     return

    # def auto_update(self, update_interval, command):
    #     """
    #     Update buffer manually
    #     :param update_interval: time in seconds
    #     :param command: start/stop
    #     :return:
    #     """
    #
    #     if command == 'start' and self.control == 'started':
    #         print('Auto update was started already')
    #         return
    #     elif command == 'stop' and self.control == 'stopped':
    #         print('Auto update was stopped already')
    #         return
    #     else:
    #         self.control = command
    #
    #     while self.control == 'start':
    #             try:
    #                 self.data = collect_observations(self.request)
    #             except Exception as e:
    #                 print('Requesting data raised an exception: ', e)
    #             else:
    #                 self.last_update = datetime.datetime.now().isoformat()
    #                 time.sleep(update_interval)


class GEvent:
    """A geographic event, with the following properties:

    Spatial: detection extent, spatial granularity and topology
    Temporal: temporal window for the detection of an geographic event, time validity:
    Attributive: a list of condition statement. Ex. variable <comparison operator> value
    """
    def __init__(self, event_definition):
        gevent = event_definition # definition as json object (type: dict)
        properties = gevent["properties"]
        spatial_properties = properties["spatial"]
        temporal_properties = properties["temporal"]
        attrib_properties = properties["attributive"]
        self.name = gevent["name"]
        self.update_frequency = gevent["update frequency"]
        self.extent = spatial_properties['extent'] # list of polygons in WKT
        self.granularity = spatial_properties['granularity']
        self.extent_distance = self.granularity["distance"]
        self.extent_units = self.granularity["units"]
        self.topology = spatial_properties["topology"]
        self.time_type = temporal_properties["type"]
        self.time = temporal_properties["time"]
        self.validity = temporal_properties["validity"]
        self.conditions = attrib_properties["conditions"]
        self.id_ = uuid.uuid4().hex

    def phenomena_names(self):
        names = []
        for phenomenon in self.conditions.values():
            names.append(phenomenon[0])
        return names

    def phenomenon_json_type(self, phenomenon_name):
        """ Converts python datatypes into JSON (CEP specific) data types"""
        for name_value in self.conditions.values():
            try:
                phenomenon_name in name_value
            except ValueError:
                print('No name match, continue looking')
                continue
            else:
                python_type = type(name_value[1])
                if python_type == int or python_type == float:
                    return 'DOUBLE'
                elif python_type == str:
                    return 'STRING'
                else:
                    raise TypeError('Type conversion not defined!!')


class StreamGenerator:
    """Push a list of observations into a CEP receiver.
    Attributes:
        id: unique identifier
        cep_reciever: URL of a receiver in the processing engine
        datastream_uri: url of a datastream in the sensor API
        update_frequency: frequency in milliseconds to send request. Default 5 seconds.
        expiration: ISO formatted time at which the generator should expire.
    """
    stream_definition = {'name': 'geosmart.remote.test100', 'version': '1.0.0', 'nickName': 'streamTest', 'description': 'stream test', 'metaData': [{'name': 'observation_id', 'type': 'LONG'}, {'name': 'result_time', 'type': 'STRING'}, {'name': 'symbol', 'type': 'STRING'}], 'correlationData': [{'name': 'generator_id', 'type': 'STRING'}], 'payloadData': [{'name': 'Temperature', 'type': 'DOUBLE'}, {'name': 'x_coord', 'type': 'DOUBLE'}, {'name': 'y_coord', 'type': 'DOUBLE'}]}
        #  TODO:  remove dependency of the above stream definition, specially on the payloadData (phenomena name)

    def __init__(self, observations, expiration_, receiver_endpoint, update_frequency=5000):
        self.observations = observations
        # self.cep_url = cep_receiver
        self.update_frequency = update_frequency
        self.expiration = expiration_
        self._id = str(uuid.uuid4()) # id
        # self.gevent_id = gevent_id
        self.running = True
        self.receiver = receiver_endpoint

    def stream_to_cep(self):
        """
        :param receiver_endpoint: url to the receiver endpoint
        :return:
        """
        self.status = 'running'
        # start session at Sensor API
        sensor_api = requests.Session()
        # start session at CEP server
        cep_engine = requests.Session()
        if self.running and (datetime.datetime.now() < datetime.datetime.strptime(self.expiration, "%Y-%m-%dT%H:%M:%SZ")):
            # retrieve data
            # TODO: check for time stamp for avoiding sending redundant data
            latest_observation = sensor_api.get(self.datastream + '/Observations?$top=1&$expand=Datastream')
            latest_observation_json = latest_observation.json()['value'][0]
            location = sensor_api.get(self.datastream + '/Thing/Locations?$top=1')
            coords = get_xy_coord(location.json())
            # format data
            mapped_observation = cep.map_datatastream(self._id, latest_observation_json, coords, self.stream_definition)

            # push data to cep server
            log.info("datastream | " + self._id)
            cep_engine.post(self.receiver, json=mapped_observation, verify=False)
            print(self._id)
            # print('observation value', mapped_observation)
        return True


class EventHandler:
    """
    Process controller for the detection of an gevent
    Attributes:
    event: gevent object
    config: configuration details to access a CEP server.

    """

    def __init__(self, gevent, config_file):
        self.event = gevent
        self.event_id = gevent.id_
        self.cep_config = config_file['geosmart.sys']['cep']
        self.handler_conf = config_file['geosmart.sys']['handler']
        # self.username = config['Geosmart.sys']['cep']['username']
        # self.receiver_name = 'httpReciever.' + gevent.id_
        # self.stream_name = 'stream.' + gevent.id_  # will be more at least two
        # self.detection_plan_name = 'plan.' + gevent.id_  # may be more than one
        # self.publisher = 'publisher.' + gevent.id_  # may be more than one
        self.deployed_files = {"streams": [], "receivers": [], "plans": [], "publishers": []}
        self.status = False
        self.file_count = 0

    def deploy_cep_configuration(self, publisher_target):
        """ Create and deploy configuration files in the CEP server.
        Configuration files include definitions for streams, receivers, execution plans
        and publisher
        :param publisher_target: URL target to push event notifications
        """
        phenomena = self.event.phenomena_names() # list of names of phenomena to be detected
        streams_in = []
        receivers = []
        ind = 1
        # define receivers and associated event streams
        print('*** Preparing files for deployments...')
        for phenomenon in phenomena:
            stream_name = 'geosmart.stream.in.' + self.event_id + '_' + str(ind)
            version = '1.0.0'
            phenomenon = {"name": phenomenon , "data type": self.event.phenomenon_json_type(phenomenon)}
            s = cep.define_stream(stream_name, phenomenon, version, description='')
            receiver_id = self.event_id + str(ind)
            r = cep.define_receiver(receiver_id, stream_name, version)
            receivers.append(r)
            streams_in.append(s)
            ind += 1

        # define plans, output streams,
        ind = 1
        plans = [] # collect execution plans
        streams_out = [] # collect streams linked to executions plans
        conditions = self.event.conditions
        condition = conditions.items().__iter__() # will iter over operators (keys)
        for phenomenon in phenomena:
            stream_name = 'geosmart.stream.out.' + self.event_id + '_' + str(ind) # create output stream
            version = '1.0.0'
            plan_name = 'geosmart.plan.' + self.event_id + str(ind)
            phenomenon_ = {"name": phenomenon, "data type": self.event.phenomenon_json_type(phenomenon)}
            so = cep.define_stream(stream_name, phenomenon_, version, description='') # define output stream
            streams_out.append(so)
            # query_filter = cep.cep_query(condition.__next__())
            # print("stream out name ", so, '\n stream in name ', streams_in[ind-1])
            p = cep.define_execution_plan(plan_name, [streams_in[ind-1]], so, condition.__next__(), description='')  # define execution plan and linked with in and out streams
            plans.append(p)
            ind += 1

        # define publisher
        publisher_name = 'pub-' + self.event_id
        # stream_version = '1.0.0'
        # print("output stream: ", streams_out)
        publisher = cep.define_event_publisher(publisher_name, streams_out[0]['name'], streams_out[0]['version'], 'http', publisher_target)


        # deploy streams
        print('*** Deploying configuration files to CEP server...')
        stream_dir = self.cep_config['home directory'] + self.cep_config['stream subdir']
        for stream in (streams_in + streams_out):
            s = json.dumps(stream)
            # file_name = stream['name']
            with tempfile.TemporaryFile('w+') as fo:
                fo.write(s)
                fo.seek(0)
                fo_name = stream_dir + '/' + 'stream-' +self.event_id + '-' + str(self.file_count) + '.json'
                cep.upload_to_cep(fo_name, fo, self.cep_config, self.handler_conf)
                #todo: check for sucessful executions
                self.deployed_files["streams"].append(fo_name)
                self.file_count += 1
        self.status = True


        # deploy receivers
        receiver_dir = self.cep_config['home directory'] + self.cep_config['receiver subdir']
        for receiver in receivers:
            r = receiver
            with tempfile.TemporaryFile('w+', encoding='UTF-8') as fo:
                fo.write(r)
                fo.seek(0)
                fo_name = receiver_dir + '/' + 'receiver-' + self.event_id + '_' + str(self.file_count) + '.xml'
                cep.upload_to_cep(fo_name, fo, self.cep_config, self.handler_conf)
                self.deployed_files["receivers"].append(fo_name)
                self.file_count += 1
        self.status = True

        #  deployed plans
        plan_dir = self.cep_config['home directory'] + self.cep_config['plan subdir']
        for plan in plans:
            p = plan
            with tempfile.TemporaryFile('w+') as fo:
                fo.write(p)
                fo.seek(0)
                fo_name = plan_dir + '/' + 'plan-' + self.event_id + '-' + str(self.file_count) + '.siddhiql'
                cep.upload_to_cep(fo_name, fo, self.cep_config, self.handler_conf)
                self.deployed_files["plans"].append(fo_name)
                self.file_count += 1

        # deploy publisher:
        pub_dir = self.cep_config['home directory'] + self.cep_config['publisher subdir']
        with tempfile.TemporaryFile('w+') as pub:
            pub.write(publisher)
            pub.seek(0)
            pub_name = pub_dir + '/pub-' + self.event_id + '-' + str(self.file_count) + '.xml'
            cep.upload_to_cep(pub_name, pub, self.cep_config, self.handler_conf)
            self.deployed_files["publishers"].append(pub_name)
            self.file_count += 1

        self.status = True # change status on successful deployment
        print('*** Deployment is complete!')
        return self.status

    def undeploy_cep_configuration(self):
        """ Delete configurations files from CEP server"""

        print(str(self.file_count + 1), ' Files will be removed from CEP server')
        print('*** Execution in progress...')

        try:
            for stream in self.deployed_files['streams']:
                cep.remove_from_cep(stream, self.cep_config, self.handler_conf)

            for receiver in self.deployed_files['receivers']:
                cep.remove_from_cep(receiver, self.cep_config, self.handler_conf)

            for plan in self.deployed_files['plans']:
                cep.remove_from_cep(plan, self.cep_config, self.handler_conf)

            for publisher in self.deployed_files['publishers']:
                cep.remove_from_cep(publisher, self.cep_config, self.handler_conf)
        except:
            return self.status
        else:
            self.status = False # return status to initial state
            print('**Undeploy complete!')
        return self.status

    # def create_stream_generators(self, api_url):
    #     """Create generators using the StramGenerator class"""
    #
    #     # find datastreams for pheonemon X
    #     streams = find_datastreams(api_url, self.event.extent, self.event.phenomena_names()[0])

    #
    # def start_event_detection(self):
    #     # call generator
    #     pass
    #
    # def stop_event_detection(self):
    #     pass

        # stopt generator
        # remove deployed files

