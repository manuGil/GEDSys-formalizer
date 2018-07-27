# Event Formalizer

Implementation of the Event Formalizer for the Geographic Event Detection System.

## Prerequisites

1. <a href= "https://github.com/FraunhoferIOSB/FROST-Server">FROST-Server</a> v1.0, an implementation of the OGC SensThing API.
  2. <a href= "https://wso2.com/analytics/previous-releases">WSO2 Data Analytics Server</a> v3.1.0, a Complex Event Processing implementing the SiddhiQL language.

## Event Definition
Users define events by providing a name, a update frequency (in milliseconds), and a set of properties. Event definitions are formatted as JSON objects, with the following pattern:

```javascript
{
  "name": "hot day",
  "properties": {
    "spatial": {
      "extent": [
        "POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))"
      ],
      "granularity": {
        "distance": 100,
        "units": "m"
      },
      "topology": "single"
    },
    "temporal": {
      "type": "continuous",
      "time": "2016-11-24T10:00:00Z/2016-11-24T11:00:00Z",
      "validity": "2016-11-24T10:00:00Z/2016-11-24T11:00:00Z"
    },
    "attributive": {
      "conditions": {
        "<": [
          "temperature",
          25.0
        ]
      }
    },
    "update frequency": 60000
  }
}
```


**Spatial properties** define values to construct spatial filters. These are: *extent, granularity* and *topology*.

**Temporal properties** define values to apply temporal filter. For instance, a *time* for which an event is relevant, and a *validity* which defined the duration of an event int he system.

**Attributive properties** define a set of conditions (logic statements) to be applied to the phenomena being observed.

### Embedding conditions
An event definition are embedded using the <a href= "http://jsonlogic.com/">JsonLogic</a> format. For instance, for the 'hot day' event, the condition "temperature more than 25 degrees" is typed as:

``` json
{">": # operator as KEY
    ["temperature", 25.0] # operands (values to compare)
}
```

More complex rules are possible, including multiple conditions, logic operators (AND, OR), and negation. For example for an coposite event including Temperature and Luminisity.

```python
{
"conditions": {
        "and": [ # logic operator
          {">": [ # condition 1
            "Temperature",
            -1000
              ] 
          }, {
          ">":[  #condition 2
            "Luminosity",
            -1000
            ]
          }
        ]
      }
}
```

## Deploying files in the CEP Server (WSO2DAS-3.1.0)

The CEP server should be configured before an geo-event can start the detection process. for this two operations are essential. First, a data entry point needs to be created in the CEP server. This is achieved by creating a 'HTTP receirver' and an 'event stream'. Second, an 'execution plan', which contains detections rules, need to be created and associated with the 'event stream'.

In runtime, those operations are executed by deploying  files to the hot directories of the CEP Server.
For each geo-event, at list the following files should be deployed in the following order:

### Event Stream:

JSON file describing the data structure of a data stream.

### HTTP Receiver
XML file describing the details of a receiver and the association with one and only one event stream.

### Execution Plan
Custom CEP formal file (.siddhiql) describing the rules to be applied to an associated data stream. An execution plan needs to be associated with  existing event streams. An input event stream feed data to the execution plan in runtime, and output event stream outputs data produced by the CEP engine. In this implementation an execution plan must be associated with one *input* event stream and one *output* event stream.

Execution plans contain a query parameter which applies detection rules.
Queries must be written using the <a href=https://docs.wso2.com/display/DAS310/Siddhi+Query+Language>shiddhiql</a> language.  For now this implementation only support filtering. A query containing a filter has the follow structure:

``` sql
from <input stream name> [<filter condition>]
select <attribute name>, <attribute name>, ...
insert into <output stream name>
```

### Publisher
A publisher pushes data (filtered or not) through output connectors. In this implementation two types of publishers are available. UI publishers are meant to be used only withing WSO2CEP server and its built in dashboard. HTTP publishers can be use to push data to URL in a web service. Publisher definition files are XML files.


## Sensor API

SensorAPI implements the SensorThingAPI standard from OCG. We use the FROST-Server implementation.

### Querying Observations

The following query retrieves the latest observation and location of the things withing an area of interest `geo.intersect()`. Selection (`$select`) is applied for shortening the response output.

`json
http://130.89.217.201:8080/frost-server/v1.0/Things?$filter=geo.intersects(Locations/location,geography'POLYGON((-3.8469736283051370 43.4414847853464039, -3.8469736283051370 43.4863448420050389,  -3.7663235810882401 43.4863448420050389, -3.7663235810882401 43.4414847853464039, -3.8469736283051370 43.4414847853464039))') and Datastreams/ObservedProperty/name eq 'Luminosity'&$select=name,@iot.id&$expand=Datastreams($select=@iot.selflink,unitOfMeasurement;$filter=ObservedProperty/name eq 'Luminosity';$expand=Observations($orderby=phenomenonTime desc;$top=1)),Locations($select=location;$expand=HistoricalLocations($select=time;$orderby=time desc;$top=1))`

Output example:

`{
    "@iot.count": 557,
    "@iot.nextLink": "http://130.89.217.201:8080/frost-server/v1.0/Things?$top=2&$skip=2&$select=name,id&$filter=%28geo.intersects%28Locations%2Flocation%2Cgeography%27POLYGON%28%28-3.8469736283051370+43.4414847853464039%2C+-3.8469736283051370+43.4863448420050389%2C++-3.7663235810882401+43.4863448420050389%2C+-3.7663235810882401+43.4414847853464039%2C+-3.8469736283051370+43.4414847853464039%29%29%27%29+and+%28Datastreams%2FObservedProperty%2Fname+eq+%27Luminosity%27%29%29&$expand=Datastreams%28%24select%3DunitOfMeasurement%2C%2540iot.selfLink%3B%24filter%3D%28ObservedProperty%2Fname+eq+%27Luminosity%27%29%3B%24expand%3DObservations%28%24top%3D1%3B%24orderby%3DphenomenonTime+desc%29%29,Locations%28%24select%3Dlocation%3B%24expand%3DHistoricalLocations%28%24top%3D1%3B%24select%3Dtime%3B%24orderby%3Dtime+desc%29%29",
    "value": [
        {
            "name": "lightSensor535",
            "Locations": [
                {
                    "location": {
                        "type": "Point",
                        "coordinates": [
                            -3.80035,
                            43.46294
                        ]
                    },
                    "HistoricalLocations": [
                        {
                            "time": "2017-10-11T13:13:04.254Z"
                        }
                    ],
                    "HistoricalLocations@iot.count": 1
                }
            ],
            "Locations@iot.count": 1,
            "Datastreams": [
                {
                    "unitOfMeasurement": {
                        "name": "lux",
                        "symbol": "lx",
                        "definition": null
                    },
                    "Observations": [
                        {
                            "phenomenonTime": "2014-10-21T14:10:45.000Z",
                            "resultTime": "2014-10-21T14:10:45.000Z",
                            "result": 54230.22,
                            "@iot.id": 105,
                            "@iot.selfLink": "http://130.89.217.201:8080/frost-server/v1.0/Observations(105)"
                        }
                    ],
                    "Observations@iot.count": 1,
                    "@iot.selfLink": "http://130.89.217.201:8080/frost-server/v1.0/Datastreams(105)"
                }
            ],
            "Datastreams@iot.count": 1,
            "@iot.id": 53
        },
        {
            "name": "lightSensor506",
            "Locations": [
                {
                    "location": {
                        "type": "Point",
                        "coordinates": [
                            -3.80545,
                            43.46385
                        ]
                    },
                    "HistoricalLocations": [
                        {
                            "time": "2017-10-11T13:13:30.089Z"
                        }
                    ],
                    "HistoricalLocations@iot.count": 1
                }
            ],
            "Locations@iot.count": 1,
            "Datastreams": [
                {
                    "unitOfMeasurement": {
                        "name": "lux",
                        "symbol": "lx",
                        "definition": null
                    },
                    "Observations": [
                        {
                            "phenomenonTime": "2016-12-31T22:52:50.000Z",
                            "resultTime": "2016-12-31T22:52:50.000Z",
                            "result": 0.26,
                            "@iot.id": 3864994,
                            "@iot.selfLink": "http://130.89.217.201:8080/frost-server/v1.0/Observations(3864994)"
                        }
                    ],
                    "Observations@iot.count": 8678,
                    "Observations@iot.nextLink": "http://130.89.217.201:8080/frost-server/v1.0/Datastreams(365)/Observations?$top=1&$skip=1&$orderby=phenomenonTime+desc",
                    "@iot.selfLink": "http://130.89.217.201:8080/frost-server/v1.0/Datastreams(365)"
                }
            ],
            "Datastreams@iot.count": 1,
            "@iot.id": 183
        }
    ]
}`
