#!/usr/bin/env python3
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
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================
from confluent_kafka import Consumer
import json
import ccloud_lib
from datetime import datetime, timezone, timedelta, date
import time
import json
import psycopg2
import argparse
import re
import csv
import pandas as pd
import psycopg2.extras as extras
import pytz
from dateutil import parser
from google.cloud import storage

def upload_blob_from_memory(inputString):
	"""Uploads a string to the bucket."""
	storage_client = storage.Client()
	bucket = storage_client.bucket("ajckbaslhbaysuycascuy")
	blob = bucket.blob("storage-object")
	print("about to upload")
	compressed_data = str(zlib.compress(original_data, zlib.Z_BEST_COMPRESSION))
	blob.upload_from_string(compressed_data)
	print("uploaded")

if __name__ == '__main__':
	config_file =  "/home/esam2/.confluent/librdkafka.config"
	topic = "archiveTest"
	conf = ccloud_lib.read_ccloud_config(config_file)

	# Create Consumer instance
	# 'auto.offset.reset=earliest' to start reading from the beginning of the
	#   topic if no committed offsets exist



	consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
	consumer_conf['group.id'] = 'archive'
	consumer_conf['auto.offset.reset'] = 'earliest'
	consumer = Consumer(consumer_conf)

	# Subscribe to topic
	consumer.subscribe([topic])

	# Process messages
	try:
		while True:
			msg = consumer.poll(1.0)
			if msg is None:
				print("Waiting for message or event/error in poll()")
				continue
			elif msg.error():
				print('error: {}'.format(msg.error()))                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
			else:
				key = msg.key().decode("utf-8")
				message = msg.value()
				upload_blob_from_memory(message)
				
	except KeyboardInterrupt:
		pass
	finally:
		# Leave group and commit final offsets
		consumer.close()
