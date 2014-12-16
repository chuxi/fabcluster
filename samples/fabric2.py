#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

"""

__author__ = 'king'

from fabric.api import *

env.user = 'vlis'
env.password = '*******'

env.roledefs = {
    'g1': ['10.214.208.11', '10.214.208.12'],
    'g2': ['10.214.208.13', '10.214.208.14']
}

def test1():
    run('echo hello')
    print('this is test 1')

@roles('g1')
def pg1():
    test1()
    print(env.host_string)

@roles('g2')
def pg2():
    test1()
    print(env.host_string)


def deploy():
    pg1()
    # execute(pg1)
    # execute(pg2)
