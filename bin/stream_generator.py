'''
Generate data streams by reading event object and accessing a SensorThing API (OGC)
Inputs: event instance (python object)
'''


from bin import interpreter as interpreter


if __name__ == '__main__':

    file = '../tests/event_def_test.json'

    e = interpreter.GEvent(file)

    cep_reciever = 'http://130.89.217.201:9763/endpoints/httpReciever001'

    id_ = 123
    api_root = 'http://130.89.217.201:8080/SensorThingsServer/v1.0'
    cep_url = 'http://cep'

    api_root = 'http://130.89.217.201:8080/SensorThingsServer/v1.0'

    extent = 'POLYGON((-4 42, -3.8 43.5, 1 44, 1 42.5, -4 42))'

    fen = 'Luminosity'
    res = filter_datastreams(api_root, extent, fen)

    # print(len(res))
    # print(res)
    expiration = '2017-12-24T10:00:00Z'

    stream_def = {
  "streamId": "geosmart.test:1.0.0",
  "name": "geosmart.test",
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
      "type": "LONG"
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

    g = StreamGenerator(res[5], cep_reciever, expiration, stream_def, update_frequency=2000)
    g.start_streaming()

    #TODO: filter things by location, filter by property.








