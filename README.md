# Event Formalizer

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

``` python
{">": # operator as KEY
    ["temperature", 25.0] # operands (values to compare)
}
```

More complex rules are possible, including multiple conditions, logic operators (AND, OR), and negation.

## Deploying files on the CEP Server

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

```
"POLYGON((-3.815358348846435 43.45897465327482,  -3.813354274749756 43.455766186244, -3.809586597442627 43.456975019613435, -3.8109898567199707 43.459990427792775,  -3.815358348846435 43.45897465327482))"

all area
POLYGON((-3.8469736283051370 43.4414847853464039, -3.8469736283051370 43.4863448420050389,  -3.7663235810882401 43.4863448420050389, -3.7663235810882401 43.4414847853464039, -3.8469736283051370 43.4414847853464039))
```