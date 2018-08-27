#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @File  : Daemon.py
# @Author: Liaop
# @Date  : 2018-08-17
# @Desc  : Linux系统守护进程基础类，此类只能用于Linux操作系统

import sys, os, time, atexit, platform
from signal import SIGTERM
from BaseClass.Log import Loger


class Daemon(object):
    def __init__(self, pidfile, loger=None):
        '''
        类初始化

        :param pidfile: 记录Pid的文件，可以用来判断进程是否启动
        :param loger: 日志记录对象
        '''
        self.platform = platform.system().lower()
        self.__pidfile = pidfile
        if loger:
            self.loger = loger
        else:
            self.loger = Loger('Daemon', 'debug')

    def _daemonize(self):
        '''
        让程序成为守护进程

        :return:
        '''
        try:
            pid = os.fork()     # 第一次fork，生成子进程，脱离父进程
            if pid > 0:
                sys.exit(0)     # 退出父进程
        except OSError as e:
            self.loger.error('Fork #1 failed:{} ({})'.format(e.errno, e.strerror))
            sys.exit(1)

        os.chdir('/')       # 修改工作目录
        os.setsid()         # 设置新的会话链接
        os.umask(0)         # 重新设置文件创建权限

        try:
            pid = os.fork()     # 第二次fork，禁止进程打开终端
            if pid > 0:
                sys.exit(0)
        except OSError as e:
            self.loger.error('Fork #2 failed:{} ({})'.format(e.errno, e.strerror))
            sys.exit(1)

        # 注册退出函数，根据文件pid判断是否存在进程
        atexit.register(self._delpid)
        pid = str(os.getpid())
        open(self.__pidfile, 'w+').write('{}\n'.format(pid))

    def _delpid(self):
        os.remove(self.__pidfile)

    def start(self):
        '''
        启动服务
        通过检查pid是否存在来判断进程是否启动
        如果启动则报错，没有启动则启动进程

        :return:
        '''
        try:
            pf = open(self.__pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            self.loger.warning('pidfile {} already exist. Daemon already running!'.format(self.__pidfile))
            sys.exit(1)
        # 启动
        if self.platform != 'windows':
            self._daemonize()
        self._run()

    def stop(self):
        '''
        停止服务
        通过从pid文件中获取pid

        :return:
        '''
        try:
            pf = open(self.__pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if not pid:
            self.loger.warning('pidfile {} does not exist. Daemon not running!'.format(self.__pidfile))
            exit(1)

        # 杀进程
        try:
            while True:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
        except OSError as e:
            err = str(e)
            if err.find('No such process') > 0:
                if os.path.exists(self.__pidfile):
                    os.remove(self.__pidfile)
                else:
                    self.loger.error('{}'.format(err))
                    sys.exit(1)

    def restart(self):
        self.stop()
        self.start()

    def _run(self):
        '''
        运行的服务
        用户进行重载，加入自己的业务逻辑

        :return:
        '''
        while True:
            self.loger.debug('{}:hello world!'.format(time.ctime()))
            time.sleep(2)

if __name__ == '__main__':
    pass