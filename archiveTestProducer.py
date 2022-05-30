#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
from datetime import datetime, timezone, timedelta, date
import json
import urllib.request
import zlib

if __name__ == '__main__':
	config_file = "/home/esam2/.confluent/librdkafka.config"
	topic = "archiveTest"
	conf = ccloud_lib.read_ccloud_config(config_file)

	# Create Producer instance
	producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
	producer = Producer(producer_conf)

	# Create topic if needed
	ccloud_lib.create_topic(conf, topic)
	
	data = open('data.geojson', 'r').read()

	record_key = "key"
	record_value = data
	producer.produce(topic, key=record_key, value=record_value)
	producer.poll(0)
	producer.flush()

	print("sent data")