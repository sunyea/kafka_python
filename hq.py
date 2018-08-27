#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @File  : test_hq.py
# @Author: Liaop
# @Date  : 2018-08-20
# @Desc  : 取行情

from pykafka import KafkaClient

client = KafkaClient('192.168.100.70:9092,192.168.100.71:9092,192.168.100.72:9092')
topic = client.topics[b'market']
consumer = topic.get_simple_consumer()
for msg in consumer:
    print(msg.value.decode())
