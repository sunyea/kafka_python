#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @File  : KafkaServer.py
# @Author: Liaop
# @Date  : 2018-10-16
# @Desc  : Kafka服务端封装包

from pykafka import KafkaClient
from pykafka.topic import OffsetType
from pykafka.producer import CompressionType

class KafkaServer(object):
    '''
    针对快单手服务端，专用Python封装类
    '''

    def __init__(self, hosts, in_topic, out_topic, group_id, encoding='utf-8'):
        self.__hosts = hosts
        self.__encoding = encoding
        if not isinstance(in_topic, bytes):
            in_topic = in_topic.encode(self.__encoding)
        self.__in_topic = in_topic
        if not isinstance(out_topic, bytes):
            out_topic = out_topic.encode(self.__encoding)
        self.__out_topic = out_topic
        if not isinstance(group_id, bytes):
            group_id = group_id.encode(self.__encoding)
        self.__group_id = group_id

    def get_process(self, queue, name=None):
        '''
        接收请求进程
        :param queue: 消息队列
        :param name: 进程名
        :return:
        '''
        print('get {} process begin..'.format(name))
        _client = KafkaClient(hosts=self.__hosts)
        _consumer = (_client.topics[self.__in_topic]).get_balanced_consumer(consumer_group=self.__group_id,
                                                                            auto_offset_reset=OffsetType.LATEST,
                                                                            managed=True)
        try:
            while True:
                for _msg in _consumer:
                    if _msg is not None:
                        _value = _msg.value.decode(self.__encoding)
                        queue.put(_value)
                        print('[GETPROCESS {}] msg:{}'.format(name, _value))
                        _consumer.commit_offsets()
        finally:
            _consumer.stop()

    def send_process(self, queue, name=None):
        '''
        发送反馈进程
        :param queue: 消息队列
        :param name: 进程名
        :return:
        '''
        print('send {} process begin..'.format(name))
        _client = KafkaClient(hosts=self.__hosts)
        _producer = (_client.topics[self.__out_topic]).get_producer(linger_ms=0, compression=CompressionType.GZIP)
        try:
            while True:
                if not queue.empty():
                    _value = queue.get(True)
                    _message = self.handler(_value)
                    if not isinstance(_message, bytes):
                        _message = _message.encode(self.__encoding)
                    _producer.produce(_message)
                    print('[SENDPROCESS {}] msg:{}'.format(name, _message))
        finally:
            _producer.stop()

    def handler(self, message):
        return message