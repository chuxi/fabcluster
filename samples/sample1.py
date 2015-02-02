#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

"""

__author__ = 'king'

import os
from fabric.api import *

env.user = 'hadoop'
env.password = 'hadoopexit'

env.roledefs = {
    'g1': ['10.214.208.11', '10.214.208.12'],
    'g2': ['10.214.208.13', '10.214.208.14']
}

env.keywords = ['jdk', 'hadoop', 'zookeeper', 'hbase', 'kafka', 'spark']

env.fnames = dict(zip(sorted(env.keywords), sorted(os.listdir('./tars'))))


def test0():
    print(env.fnames)


def test1():
    run('echo hello')
    print('this is test 1')

@roles('g1')
def pg1():

    with settings(warn_only=True):
        result = run("systemctl list-units | grep 'ntpd' | awk '{{print $4}}'")
        if (result == 'running'):
            print("running")
        else:
            print("not running")


@roles('g2')
def pg2():
    test1()
    print(env.host_string)


def deploy():
    # pg1()
    execute(pg1)
    # execute(pg2)
