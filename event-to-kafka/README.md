<!--
{% comment %}
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
{% endcomment %}
-->
# Presto Event To Log(log history)
Plugin for Presto to save queries and metrics into files.

## Requirement
* Presto 0.224 or later

# Build
mvn clean assembly:assembly

# Usage

## copy
cd presto/plugin/\
mkdir event-to-kafka\
cp target jar and dependencies jars to event-to-kafka

## run
create a event-listener.properties file under /etc/presto/ .

eg.
/etc/presto/event-listener.properties
```bash
event-listener.name=event-to-kafka
event-listener.bootstrap.servers=xykfk01:9092,xykfk02:9092,xykfk03:9092,xykfk04:9092
event-listener.client.id=PrestoProducer
event-listener.kafka-topic=xdw_test
```

cd presto/bin\
./launcher restart
# presto-plugin
