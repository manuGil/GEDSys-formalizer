"""
Project: Formalizer. Provides mapping and formatting objects and functions for the WSO2 CEP engine
Author: ManuelG
Created: 02-Jan-18 18:50
License: MIT
"""

import paramiko
import traceback
import uuid

def get_event_stream_names(data_scheme):
    """
    get names of attributes from event stream definition.
    :param data_scheme: partial data scheme, a list of JSON objects
    :return: list of name:value pairs
    """

    attributes = {}
    if len(data_scheme) == 0:
        print('Data scheme has not attributes, check input data')
        return
    else:
        for attribute in data_scheme:
            name = attribute['name']
            attributes.update({name: ''})

    return attributes


def map_datatastream(generator_id, data_unit, location, event_stream_definition):
    """
    maps sensor data into CEP default JSON format
    :param generator_id: id of the generator
    :param data_unit: SensorAPI observation with datastream details
    :param location: observation location as [x, y]
    :param event_stream_definition: schema of event definition in processing engine as JSON
    :return: JSON object
    """

    # metadata = get_event_stream_names(event_stream_definition['metaData'])
    # correlation_data = get_event_stream_names(event_stream_definition['correlationData'])
    payload_data = get_event_stream_names(event_stream_definition['payloadData'])
    payload_names = payload_data.__iter__()

    # TODO: WARNING  overwriting generator id
    generator_id= uuid.uuid4().hex

    event_stream = {
        "event": {
            "metaData": {
                "observation_id": data_unit[0]["Observations"][0]['@iot.id'],
                "result_time": data_unit[0]["Observations"][0]['resultTime'],
                "symbol": data_unit[0]['unitOfMeasurement']['symbol']
            },
            "correlationData": {
                "event_id": generator_id
            },
            "payloadData": {
                next(payload_names): data_unit[0]["Observations"][0]['result'],
                "x_coord": location[0],
                "y_coord": location[1]
            }
        }
    }
    return event_stream


def define_stream(name, phenomenon, version, description=''):
    """
    Event stream definition in CEP format
    :param name: stream name, usually the same as the name of a Gevent
    :param phenomenon: name and data type (JSON data types) of a phenomenon associated with the stream.
    Ex. {"name": "temperature", "data type": "DOUBLE"}, or {"name": "wind_direction", "data type": "STING"}, etc.
    :param version: version of the stream. Ex. 1.0.0
    :param description: string describing the event. Optional
    :return: data stream definition in CEP format
    """

    cep_stream = {
                    "name": name,
                    "version": version,
                    "nickName": "",
                    "description": description,
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
                                  "name": "event_id",
                                  "type": "STRING"
                                }
                              ],
                    "payloadData": [
                                {
                                  "name": phenomenon["name"],
                                  "type": phenomenon["data type"]
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
    return cep_stream


def define_receiver(receiver_id, stream_name, stream_version):
    """
    Generates the definition of a HTTP reciever for the CEP engine
    :param receiver_id: unique id for the receiver, alphanumeric
    :param stream_name: name of the stream associated with the receiver
    :param stream_version: version fo the stream
    :return: string containing the definition of a httpReciever in XML
    """

    cep_receiver = '<?xml version="1.0" encoding="UTF-8"?><eventReceiver name="httpReceiver' + receiver_id + '" statistics="enable" trace="enable" xmlns="http://wso2.org/carbon/eventreceiver"> <from eventAdapterType="http"> <property name="transports">all</property> <property name="basicAuthEnabled">true</property> </from> <mapping customMapping="disable" type="json"/> <to streamName="' + stream_name + '" version="' +stream_version + '"/> </eventReceiver>'

    return cep_receiver


def map_stream_to_processor(stream_definition):
    """
    Maps an event stream definition into the event processor stream definition. Required for defining execution plans
    :param stream_definition: a event stream definition
    :return: string with mapped stream definition
    """

    # the below return a list of name, type pairs
    meta = stream_definition['metaData']
    correlation = stream_definition['correlationData']
    payload = stream_definition['payloadData']

    meta_items = ''
    for item in meta:
        meta_items = meta_items + 'meta_' + item['name'] + ' ' + item['type'].lower() + ', '

    corr_items = ''
    for item in correlation:
        corr_items = corr_items + 'correlation_' + item['name'] + ' ' + item['type'].lower() + ', '

    payload_items = ''
    for item in payload:
        payload_items = payload_items + item['name'] + ' ' + item['type'].lower() + ', '

    mapped_stream = '(' + meta_items + corr_items + payload_items
    mapped_stream = mapped_stream[:-2] + ')'

    return mapped_stream


def cep_query(event_condition, in_alias, out_alias):
    """
    Translate conditions in an event definition into a filter query
    :param event_condition: event conditions in event definition file,
    a tuple of the format: ('operator', ['phenomenon', value])
    :param in_alias: alias of the input stream
    :param out_alias: alias of the output stream
    :return: query (a string) in siddhiql
    """

    # create key iterator
    # i = event_condition.keys().__iter__()

    filter_ = ''
    operator = event_condition[0]
    filter_ = event_condition[1][0] + ' ' + operator + ' ' + str(event_condition[1][1])

    query = 'from ' + in_alias + ' [' + filter_ + '] select * ' \
        'insert into ' + out_alias

    return query


def define_execution_plan(name, input_streams, output_stream, event_condition, description=''):
    """
    Generates a text file describing a CEP execution plan. Execution plan define detection rules a  queries
    :param name: unique name for the execution plan. Alphanumeric, underscore (_) is allowed
    :param input_streams: a none empty list of stream definition
    :param output_stream: a single stream definition for collecting results from the event processor.
    :param event_condition: event conditions in event definition file,
    a tuple of the format: ('operator', ['phenomenon', value])
    :param description: unique description for the execution plan. Optional.
    :return: string defining a valid execution plan for the CEP engine
    """

    # check for different streams for inputs and outputs
    for stream in input_streams:
        try:
            (stream['name'] + ':' + stream['version']) != (output_stream["name"] + output_stream['version'])
        except:
            print(type(stream))
            print(stream['name'])
            raise  ValueError('Input and output streams must be different. Exception with: ', stream["name"], stream['version'])

    # map input streams
    idx = 1
    mapped_inputs = ''
    input_aliases = [] # collect aliases
    for stream in input_streams:
        alias = 'input_' + str(idx)
        stream_name = stream['name']+ ':' + stream['version']
        mapped_stream = map_stream_to_processor(stream)
        mapped_inputs = mapped_inputs + "@Import('" + stream_name + "') define stream " + alias + " " + mapped_stream + ";"
        input_aliases.append(alias)

    # mapping output stream
    output_alias = 'output_1'  # Common case is a single output stream
    output_stream_name = output_stream['name']+':'+ output_stream['version']
    mapped_output = map_stream_to_processor(output_stream)

    if description == '':
        plan_description = name
    else:
        plan_description = description

    query = cep_query(event_condition, input_aliases[0], output_alias)

    plan = """/* Enter a unique ExecutionPlan */
            @Plan:name('""" + name + """')

            /* Enter a unique description for ExecutionPlan */
            -- @Plan:description('""" + plan_description + """')

            /* define streams/tables and write queries here ... */

            /* mapping input stream(s) */
            """ + mapped_inputs + """
            
            /* mapping outputs into an existing stream */
            @Export('""" + output_stream_name + """') define stream """ + output_alias + " " + mapped_output + ";" + """

            /* query using Siddhiql */
            """ + query  # query definition needs to know the aliases of inputs and output streams.

    return plan


def define_event_publisher(name, stream_name, stream_version, type_='http', target_url=''):
    """
    Creates a defitinion of an event publisher. Event publisher are used to push events to event consumers
    :param name: unique name for the publisher
    :param stream_name: name of the event stream associated with the publisher
    :param stream_version: vesrion of the event stream associated with the publisher
    :param type_: type of the publisher. One of: 'http' or 'ui'.
    :param target_url: URL at which the events will be publish. If type is 'http' a URL must be declared
    :return: event publisher definition
    """

    valid_types = {'http': 'http', 'ui': 'ui'}
    try:
        type_ = valid_types[type_]
    except KeyError:
        raise ValueError('Publisher type is not defined. Publisher type is case sensitive. Not valid type: %s' % type_)

    else:

        if type_ == 'ui':
            publisher = '''<?xml version="1.0" encoding="UTF-8"?>
            <eventPublisher xmlns="http://wso2.org/carbon/eventpublisher" name="''' + name + '''" statistics="enable" trace="enable">
            <from streamName="''' + stream_name + '''" version="''' + stream_version + '''" />
            <mapping customMapping="disable" type="wso2event" />
            <to eventAdapterType="ui">
            </to>
            </eventPublisher>'''

        else:
            if target_url == '':
                raise ValueError('Parameter NOT optional for type="http". Provide a value for "target_url"')
            else:

                publisher = '''<?xml version="1.0" encoding="UTF-8"?><eventPublisher name="''' + name + '''" statistics="enable"
  trace="enable" xmlns="http://wso2.org/carbon/eventpublisher">
                <from streamName="''' + stream_name + '''" version="''' + stream_version + '''"/>
                <mapping customMapping="disable" type="json"/>
                <to eventAdapterType="http">
                    <property name="http.client.method">HttpPost</property>
                    <property name="http.url">''' + target_url + '''</property>
                    <property encrypted="true" name="http.password">kuv2MubUUveMyv6GeHrXr9il59ajJIqUI4eoYHcgGKf/BBFOWn96NTjJQI+wYbWjKW6r79S7L7ZzgYeWx7DlGbff5X3pBN2Gh9yV0BHP1E93QtFqR7uTWi141Tr7V7ZwScwNqJbiNoV+vyLbsqKJE7T3nP8Ih9Y6omygbcLcHzg=</property>
                    <property name="http.username">admin</property>
                </to>
                </eventPublisher>'''

    return publisher


def generate_data_requirements(event_object):
    pass


# def execute_in_cep(destination_dir, file_object, cep_conf, handler_conf, action=None):
#     """
#     Uploads or removes files from CEP hot directories, for http-receivers, event streams and execution plans
#     :param destination_dir: directory in the CEP server to which to transfer a file
#     :param cep_conf: parameters to connect to server where cep is running. Parameters are define in the config.json file
#     :param handler_conf: parameter of the event handler, as defines in the config.json file
#     :param file_object: open file (or file-like object) containing a definition of a configuration file
#     :param action: Upload or Remove
#     :return: True on successful execution
#     """
#     # log file
#     paramiko.util.log_to_file(handler_conf['logs'])
#
#     # host conf
#     port = cep_conf["port"]
#     hostname = cep_conf["hostname"]
#     username = cep_conf["username"]
#     passphrase = cep_conf["passphrase"]
#     key_file = cep_conf["private key"]
#
#     valid_actions = ["upload", "remove"]
#     try:
#         private_key = paramiko.RSAKey.from_private_key_file(key_file, passphrase)
#     except FileNotFoundError:
#         print('** file for primary key was not found in: %s' % key_file)
#     except ValueError:
#         print('** Passphrase is incorrect')
#
#     if action is None or (action not in valid_actions):
#         raise ValueError('** Action is not define. Use "upload" or "remove"')
#
#     else:
#         try:
#             t = paramiko.Transport((hostname, port))
#             t.connect(username=username, pkey=private_key)
#             sftp = paramiko.SFTPClient.from_transport(t)
#
#             # deployment: upload files
#             if action == 'upload':
#                 upload = sftp.putof(file_object,destination_dir)
#                 print(upload)
#             elif action == 'remove':
#                 remove = sftp.remove(destination_dir)
#
#
#         except:
#             pass
#
#         dirlist = sftp.listdir('.')
#         print("dirlist: %s" % dirlist)
#
#         t.close()
#
#     except Exception as e:
#     #     print('*** Caught exception: %s' % e.__class__)
#     #     traceback.print_exc()
#     #     try:
#     #         t.close()
#     #     except:
#     #         pass


def upload_to_cep(file_path, file_object, cep_conf, handler_conf):
    """
    Uploads a file from CEP hot directories, for http-receivers, event streams and execution plans
    :param file_path: path  of the file that will be created in the CEP server
    :param cep_conf: parameters to connect to server where cep is running. Parameters are define in the config.json file
    :param handler_conf: parameter of the event handler, as defines in the config.json file
    :param file_object: open file (or file-like object) containing a definition of a configuration file
    :return: True on successful execution
    """
    # log file
    paramiko.util.log_to_file(handler_conf['logs'])

    # host conf
    port = cep_conf["port"]
    hostname = cep_conf["hostname"]
    username = cep_conf["username"]
    passphrase = cep_conf["passphrase"]
    key_file = cep_conf["private key"]

    try:
        private_key = paramiko.RSAKey.from_private_key_file(key_file, passphrase)
    except FileNotFoundError:
        print('** file for primary key was not found in: %s' % key_file)
    except ValueError:
        print('** Passphrase is incorrect')
    else:
        try:
            t = paramiko.Transport((hostname, port))
            t.connect(username=username, pkey=private_key)
            sftp = paramiko.SFTPClient.from_transport(t)

            # deployment: upload files
            upload = sftp.putfo(file_object, file_path)
            t.close()
            return True

        except Exception as e:
            print('***Caught exception: %s' % e.__class__)
            print('here')
            traceback.print_exc()
            try:
                t.close()
                return False
            except:
                return False


def remove_from_cep(file_path, cep_conf, handler_conf):
    """
    Deletes a file from CEP hot directories, for http-receivers, event streams and execution plans
    :param file_path: path to the file to be removed
    :param cep_conf: parameters to connect to server where cep is running. Parameters are define in the config.json file
    :param handler_conf: parameter of the event handler, as defines in the config.json file
    :return: True on successful execution
    """
    # log file
    paramiko.util.log_to_file(handler_conf['logs'])

    # host conf
    port = cep_conf["port"]
    hostname = cep_conf["hostname"]
    username = cep_conf["username"]
    passphrase = cep_conf["passphrase"]
    key_file = cep_conf["private key"]

    try:
        private_key = paramiko.RSAKey.from_private_key_file(key_file, passphrase)
    except FileNotFoundError:
        print('** File for primary key was not found in: %s' % key_file)
    except ValueError:
        print('** Passphrase is incorrect')
    else:
        try:
            t = paramiko.Transport((hostname, port))
            t.connect(username=username, pkey=private_key)
            sftp = paramiko.SFTPClient.from_transport(t)

            # Delete file
            upload = sftp.remove(file_path)
            t.close()
            return True

        except Exception as e:
            print('***Caught exception: %s' % e.__class__)
            print('here')
            traceback.print_exc()
            try:
                t.close()
                return False
            except:
                return False

