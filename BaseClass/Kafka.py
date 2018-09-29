#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @File  : Kafka.py
# @Author: Liaop
# @Date  : 2018-08-24
# @Desc  : Kafka操作的基础类，此类封装基本操作，主要用于设计服务端和前置端应用时使用

from pykafka import KafkaClient
from pykafka.common import CompressionType
import json

from BaseClass.Log import Loger


class Kafka(object):

    def __init__(self, hosts, loger=None, encoding='utf-8', max_buf=999950, max_retry=4, debug=False):
        '''
        初始化

        :param hosts: kafka主机地址，多个主机用逗号隔开
        :param loger: 日志记录对象
        :param encoding: 编码格式
        :param max_buf: 最大收发数据大小
        :param debug: 是否调试模式，如果是调试模式，cosumer将使用simple_consumer,否则使用balance_consumer
        '''
        if loger:
            self.loger = loger
        else:
            self.loger = Loger('Kafka', 'debug')
        self.__run = False
        if not hosts:
            self.loger.error('Kafka服务器地址为空.')
        self.__hosts = hosts
        self.__encoding = encoding
        self.__max_buf = max_buf
        self.__max_retry = max_retry
        self.__debug = debug
        self.__application = None
        self.__client = None
        self.__producer = None
        self.__consumer = None

    def __init_producer(self, out_topic):
        '''
        初始化Producer
        :param out_topic: 主题名
        :return: 正确初始化返回true
        '''
        if not out_topic:
            self.loger.error('主题名不能为空.')
            return False
        if not self.__hosts:
            self.loger.error('Kafka服务器地址为空，初始化失败.')
            return False
        try:
            if self.__client is None:
                self.__client = KafkaClient(hosts=self.__hosts)
            if not isinstance(out_topic, bytes):
                out_topic = out_topic.encode(self.__encoding)
            self.__producer = (self.__client.topics[out_topic]).get_producer(max_request_size=self.__max_buf,
                                                                             compression=CompressionType.GZIP,
                                                                             linger_ms=0)
            return True
        except Exception as e:
            self.loger.error('初始化producer异常：{}'.format(e))
            return False

    def __init_consumer(self, in_topic, consumer_group, consumer_timeout=0, balance=False):
        '''
        初始化Consumer
        :param in_topic: 主题名
        :param consumer_group: 消费者组名
        :param balance: 是否进行负载均衡
        :return: 正确初始化返回true
        '''
        if not in_topic:
            self.loger.error('主题名不能为空.')
            return False
        if not consumer_group:
            self.loger.error('消费者组名不能为空.')
            return False
        if not self.__hosts:
            self.loger.error('Kafka服务器地址为空，初始化失败.')
            return False
        try:
            if self.__client is None:
                self.__client = KafkaClient(hosts=self.__hosts)
            if not isinstance(in_topic, bytes):
                in_topic = in_topic.encode(self.__encoding)
            if not isinstance(consumer_group, bytes):
                consumer_group = consumer_group.encode(self.__encoding)
            if balance:
                self.__consumer = (self.__client.topics[in_topic]).get_balanced_consumer(consumer_group=consumer_group,
                                                                                         consumer_timeout_ms=consumer_timeout,
                                                                                         managed=True)
            else:
                self.__consumer = (self.__client.topics[in_topic]).get_simple_consumer(consumer_group=consumer_group,
                                                                                       consumer_timeout_ms=consumer_timeout)
            return True
        except Exception as e:
            self.loger.error('初始化consumer异常：{}'.format(e))
            return False

    def start(self, in_topic=None, out_topic=None, consumer_group=None, **kwargs):
        '''
        初始化kafka实例，设置相应参数

        :param in_topic: 输入主题
        :param out_topic: 输出主题
        :param consumer_group: 群组名
        :return: 如果成功返回True
        '''
        if self.__run:
            self.loger.error('Kafka实例已经启动中.')
            return False
        if in_topic == out_topic:
            self.loger.error('输入与输出主题不能一样')
            return False
        consumer_timeout = kwargs.get('consumer_timeout', 0)
        balance = kwargs.get('balance', False)
        if (in_topic is None) and (out_topic is not None):
            if self.__init_producer(out_topic):
                self.__run = True
                return True
            else:
                return False
        if (out_topic is None) and (in_topic is not None):
            if self.__init_consumer(in_topic, consumer_group, consumer_timeout=consumer_timeout, balance=balance):
                self.__run = True
                return True
            else:
                return False
        if not self.__init_consumer(in_topic, consumer_group, consumer_timeout=consumer_timeout, balance=balance):
            return False
        if not self.__init_producer(out_topic):
            return False
        self.__run = True
        self.loger.info("****START**** Kafka启动成功.")
        return True

    def stop(self):
        '''
        关闭producer和consumer，并销毁kafka实例

        :return: 如果成功返回True
        '''
        self.__run = False
        try:
            if self.__producer:
                self.__producer.stop()
                self.__producer = None
            if self.__consumer:
                self.__consumer.stop()
                self.__consumer = None
            if self.__client:
                self.__client = None
            self.loger.info('****STOP**** Kafka实例停止运行.')
            return True
        except Exception as e:
            self.loger.error('停止失败，原因：'.format(e))
            return False

    def __producer_send(self, message):
        '''
        信息推送
        :param message: 信息内容
        :return: 正确执行返回true
        '''
        if not self.__run:
            self.loger.error('Kafka实例未启动.')
            return False
        if len(message) > self.__max_buf:
            self.loger.error('消息大小为：{},已超过系统显示{}字节'.format(len(message), self.__max_buf))
            return False
        try:
            if not isinstance(message, bytes):
                message = message.encode(self.__encoding)
            self.__producer.produce(message)
            return True
        except Exception as e:
            self.loger.error('发送消息出错：{}'.format(e))
            return False

    def __consumer_get(self):
        '''
        获取信息
        :return: 返回获取到的信息，如果无信息或者超时返回None
        '''
        if not self.__run:
            self.loger.error('Kafka实例未启动.')
            return None
        try:
            for _msg in self.__consumer:
                if _msg and _msg.value:
                    return _msg.value.decode(self.__encoding)
            return None
        except Exception as e:
            self.loger.error('接收信息出错：{}'.format(e))
            return None

    def __get_session(self, message):
        '''
        从json消息串中解析出sessionid
        :param message: json消息串
        :return: 解析出的sessionid, 如果没有则返回None
        '''
        if not message:
            return None
        try:
            if isinstance(message, bytes):
                message = message.decode(self.__encoding)
            message = message.replace("'", '"').replace('":,', '":"",')
            _json_msg = json.loads(message)
            return _json_msg.get('sessionid', None)
        except Exception as e:
            return None

    def command(self, message):
        '''
        处理远程请求命令，在具体应用中重载该方法进行具体业务逻辑处理

        :param message: 远程请求消息，json字符串，格式：{'action':'command', 'sessionid':'****', 'data':{...}}
        :return: 业务处理后的反馈信息，json字符串，格式：{'code':0, 'err':'info', 'sessionid':'****', 'data':{...}}
        '''
        if isinstance(message, bytes):
            message = message.decode(self.__encoding)
        message = message.replace("'", '"').replace('":,', '":"",')
        _json_msg = json.loads(message)
        _action = _json_msg.get('action')
        _sessionid = _json_msg.get('sessionid')
        _data = _json_msg.get('data')
        if _action is None:
            _rt = {'code': -1, 'err': '没有明确的指令', 'sessionid': _sessionid, 'data': _data}
        elif _action == 'resp':
            _rt = {'code': 0, 'err': '{} 执行成功'.format(_action), 'sessionid': _sessionid, 'data': _data}
        else:
            _rt = {'code': -2, 'err': '未知指令 {}'.format(_action), 'sessionid': _sessionid, 'data': _data}
        return json.dumps(_rt)

    def waitForAction(self):
        '''
        等待处理远程请求命令
        :return:
        '''
        try:
            while self.__run:
                message = self.__consumer_get()
                if message is None:
                    continue
                resp_msg = self.command(message)
                if not self.__producer_send(resp_msg):
                    raise Exception('发送失败')
                self.__consumer.commit_offsets()
        except Exception as e:
            raise e

    def requestAndResponse(self, message):
        '''
        发送远程请求指令，并等待指令结果
        :param message: 发送给远程的指令，json字符串，格式：{'action':'command', 'sessionid':'****', 'data':{...}}
        :return: 指令结果，json字符串，格式：{'code':0, 'err':'info', 'sessionid':'****', 'data':{...}}
        '''
        try:
            sessionid = self.__get_session(message)
            if sessionid is None:
                self.loger.error('待发送的信息没有sessionid,发送失败')
                return json.dumps({'code': -1, 'err': '没有session值', 'sessionid': None, 'data': None})
            if not self.__producer_send(message):
                return json.dumps({'code': -2, 'err': '发送远程请求失败', 'sessionid': sessionid, 'data': None})
            for i_retry in range(self.__max_retry):
                while self.__run:
                    resp_msg = self.__consumer_get()
                    if resp_msg is None:
                        break
                    resp_sessionid = self.__get_session(resp_msg)
                    if sessionid == resp_sessionid:
                        resp_msg = resp_msg.replace("'", '"').replace('":,', '":"",')
                        # js_msg = json.loads(resp_msg)
                        # return json.dumps(js_msg)
                        return resp_msg
                self.loger.error('获取超时，尝试第{}次重新获取.'.format(i_retry+1))
            return json.dumps({'code': -3, 'err': '获取回复超时', 'sessionid': sessionid, 'data': None})
        except Exception as e:
            self.loger.error('发生异常：{}'.format(e))
            return json.dumps({'code': -7, 'err': '其他异常：{}'.format(e), 'sessionid': None, 'data': None})

    def send_always(self, msg):
        '''
        可持续发送信息到主题
        :param msg: 发送的信息内容
        :return:
        '''
        self.__producer_send(msg)

    def get_always(self):
        '''
        可持续从主题接收信息
        :return: 接收的内容，None表示超时没接收到
        '''
        return self.__consumer_get()

    def send_msg(self, topic, msg):
        '''
        向指定主题发送一条信息

        :param topic: 主题名
        :param msg: 信息内容
        :return: 发送成功则返回true
        '''
        if not self.start(out_topic=topic):
            return False
        if not self.__producer_send(msg):
            return False
        self.stop()
        return True

    def get_msg(self, topic, group):
        '''
        从指定主题获取一条信息

        :param topic: 主题
        :param group: 群组
        :return: 获取的信息
        '''
        if not self.start(in_topic=topic, consumer_group=group, consumer_timeout=6000):
            return None
        rt = self.__consumer_get()
        self.__consumer.commit_offsets()
        self.stop()
        return rt

    def commitOffsets(self):
        '''
        提交offset
        :return:
        '''
        self.__consumer.commit_offsets()

    def get_market(self, topic, code):
        '''
        根据合约号获取最新的合约行情
        :param code:
        :return:
        '''

        # 向前追溯的历史数据数量
        _count = 500
        _client = KafkaClient(self.__hosts)
        if not isinstance(topic, bytes):
            topic = topic.encode(self.__encoding)
        _topic = _client.topics[topic]
        _consumer = _topic.get_simple_consumer(consumer_group=b'gp.python.market',
                                               consumer_timeout_ms=100)
        _offset = _topic.latest_available_offsets()[0][0][0]
        _partition = _topic.partitions[0]
        _consumer.start()
        _consumer.reset_offsets([(_partition, _offset-_count)])
        msgs = list()
        for i in range(_count):
            _msg = _consumer.consume()
            if _msg:
                _value = _msg.value.decode(self.__encoding)
                if _value and (code in _value):
                    msgs.append(_value)

        _consumer.stop()
        _consumer = None
        _topic = None
        _client = None
        if msgs:
            return msgs[-1]
        else:
            return None
