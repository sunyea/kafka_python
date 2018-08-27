#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @File  : app.py
# @Author: Liaop
# @Date  : 2018-08-17
# @Desc  : 通用服务，接收resp指令，并将该指令的内容原样返回

from BaseClass.Daemon import Daemon
from BaseClass.Log import Loger
from BaseClass.Kafka import Kafka
import json, sys

hosts = '192.168.100.70:9092,192.168.100.71:9092,192.168.100.72:9092'
in_topic = 'tp.test.common'
out_topic = 'tp.test.common.response'
group_id = 'gp.test.common'


class ServerKafka(Kafka):
    '''
    重载command方法来实现处理指令的业务逻辑

    '''
    def command(self, message):
        try:
            self.loger.debug('收到信息：{}'.format(message))
            if isinstance(message, bytes):
                message = message.decode(self.__encoding)
            message = message.replace("'", '"').replace('":,', '":"",')
            _json_msg = json.loads(message)
            _action = _json_msg.get('action')
            _sessionid = _json_msg.get('sessionid')
            _data = _json_msg.get('data')
            if _action is None:
                _rt = {'code': -1, 'err': '[ServerKafka] 没有明确的指令', 'sessionid': _sessionid, 'data': _data}
            elif _action == 'resp':
                _rt = {'code': 0, 'err': '[ServerKafka] {} 执行成功'.format(_action), 'sessionid': _sessionid, 'data': _data}
            else:
                _rt = {'code': -2, 'err': '[ServerKafka] 未知指令 {}'.format(_action), 'sessionid': _sessionid, 'data': _data}
            self.loger.debug('反馈信息：{}'.format(json.dumps(_rt)))
            return json.dumps(_rt)
        except Exception as e:
            _rt = {'code': -3, 'err': '[ServerKafka] 未知异常 {}'.format(e), 'sessionid': None, 'data': None}
            return json.dumps(_rt)


class CommonServer(Daemon):
    '''
    重载_run函数来实现服务成为守护进程

    '''
    def __init__(self, pidfile, loger=None):
        super(CommonServer, self).__init__(pidfile, loger)
        self.__kafka = ServerKafka(hosts, loger, debug=True)

    def _run(self):
        run = self.__kafka.start(in_topic=in_topic, out_topic=out_topic, consumer_group=group_id,
                                 consumer_timeout=-1, balance=False)
        while run:
            try:
                self.__kafka.waitForAction()
            except Exception as e:
                self.loger.error('Kafka出现错误，需要准备重启，原因: {}'.format(e))
            finally:
                self.__kafka.stop()
                run = self.__kafka.start(in_topic=in_topic, out_topic=out_topic, consumer_group=group_id,
                                         consumer_timeout=-1, balance=False)

    def stop(self):
        self.__kafka.stop()
        super(CommonServer, self).stop()


if __name__ == '__main__':
    # ====== Linux 下正式运行，使用该设置 =======
    # log = Loger('CommonServer', 'debug', '/tmp/daemon/log.txt')
    # daemon = CommonServer('/tmp/daemon/RUNNING.pid', loger=log)

    # ====== Windows 下调试使用该设置 =======
    log = Loger('CommonServer', 'debug')
    daemon = CommonServer('RUNNING.pid', loger=log)

    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print('unknown command')
            sys.exit(2)
        sys.exit(0)
    else:
        print('Usage: {} start|stop|restart'.format(sys.argv[0]))
        sys.exit(2)