#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @File  : server2.py
# @Author: Liaop
# @Date  : 2018-10-16
# @Desc  : 消息队列封装包测试

from multiprocessing import Process, Queue
import json

from BaseClass.KafkaServer import KafkaServer

hosts = '192.168.100.70:9092,192.168.100.71:9092,192.168.100.72:9092'
in_topic = 'tp.js'
out_topic = 'tp.js.response'
group_id = 'serverGroup'

class CHandler(KafkaServer):
    # 重载处理函数
    def handler(self, message):
        try:
            if isinstance(message, bytes):
                message = message.decode()
            message = message.replace("'", '"').replace('":,', '":"",')
            _json_msg = json.loads(message)
            _action = _json_msg.get('action')
            _sessionid = _json_msg.get('sessionid')
            _data = _json_msg.get('data')
            if _action is None:
                _rt = {'code': -1, 'err': '[ServerKafka] 没有明确的指令', 'sessionid': _sessionid, 'data': _data}
            elif _action == 'resp':
                _rt = {'code': 0, 'err': '[ServerKafka] {} 执行成功'.format(_action), 'sessionid': _sessionid,
                       'data': _data}
            else:
                _rt = {'code': -2, 'err': '[ServerKafka] 未知指令 {}'.format(_action), 'sessionid': _sessionid,
                       'data': _data}
            return json.dumps(_rt)
        except Exception as e:
            _rt = {'code': -3, 'err': '[ServerKafka] 未知异常', 'sessionid': None, 'data': None}
            return json.dumps(_rt)




if __name__ == '__main__':
    my_que = Queue()
    kafka = CHandler(hosts=hosts, in_topic=in_topic, out_topic=out_topic, group_id=group_id)
    # 接收和发送的进程数
    i_get = 2
    i_send = 2
    p_get = list()
    p_send = list()
    for i in range(i_get):
        p_get.append(Process(target=kafka.get_process, args=(my_que, i)))
    for j in range(i_send):
        p_send.append(Process(target=kafka.send_process, args=(my_que, j)))

    print('begin..')
    for i in range(i_get):
        p_get[i].start()
    for j in range(i_send):
        p_send[j].start()

    for i in range(i_get):
        p_get[i].join()
    for j in range(i_send):
        p_send[j].join()
    print('end..')