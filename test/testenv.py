__author__ = 'king'




import os
from fabric.colors import *
from fabric.api import *
from fabric.context_managers import *
from fabric.contrib.console import confirm

env.user = 'vlis'
env.password = 'zjuvlis'
# env.hostnames = {'10.214.208.11': 'node1',
# '10.214.208.12': 'node2',
#                  '10.214.208.13': 'node3',
#                  '10.214.208.14': 'node4',
#                  }
clusters = ['10.214.208.11', '10.214.208.12', '10.214.208.13', '10.214.208.14']

# single node
# clusters = ['10.214.20.118']

# clusters = ['10.214.20.116']

env.hostnames = dict([h, 'node%d' % (i + 1)] for i, h in enumerate(clusters))


env.files = os.listdir('../tars')
env.keywords = [filter(str.isalpha, i.split('.')[0]) for i in env.files]

env.fnames = dict(zip(sorted(env.keywords), sorted(env.files)))


env.roledefs = {
    'clusters': clusters,
    'hadoop_master': clusters[:1],
    'hadoop_smaster': clusters[1:2],
    'hadoop_slaves': clusters,
    'zookeeper': clusters,
    'hbase': clusters,
    'hbase_master': clusters[0:1],
    'hbase_slaves': clusters[1:],
    'kafka': clusters,
    'spark': clusters,
    'spark_master': clusters[:1],
    'spark_slaves': clusters
}

# single node
# env.roledefs = {
#     'cluster': clusters,
#     'hadoop_master': clusters[0],
#     'hadoop_smaster': clusters[0],
#     'hadoop_slaves': clusters,
#     'zookeeper': clusters,
#     'hbase': clusters,
#     'hbase_master': clusters[0],
#     'hbase_slaves': clusters,
#     'kafka': clusters,
#     'spark': clusters,
#     'spark_master': clusters[0],
#     'spark_slaves': clusters
# }

baseDir = '/home/hadoop'
optDir = '/opt/vlis'
tmpDir = '/tmp'

newgroup = 'hadoop'
newuser = 'hadoop'
newpasswd = 'hadoop'

# methods to be tested, just for coding test

@task
@roles('clusters')
def configProfile(envname, key):
    with settings(warn_only=True):
        result = run("nl /etc/profile | grep '%s' | awk '{print $1}'" % ('export ' + envname))
    if result == '':
        sudo("sed -i '$i export %s=/usr/local/%s' /etc/profile" % (envname, key))
        sudo("sed -i '$i export PATH=$%s/bin:$PATH' /etc/profile" % envname)
        sudo("source /etc/profile")
    else:
        # num = int(result)
        # sudo("sed -i '%dc export %s=/usr/local/%s' /etc/profile" % (num, envname, key))
        print yellow("the env value %s is already exist!" % envname)


def setXMLPropVal(fname, prop, value):
    prop = '    <name>%s</name>' % prop
    value = '    <value>%s</value>' % value
    with settings(warn_only=True):
        result = run("nl %s | grep '%s' | awk '{print $1}'" % (fname, prop))
    if result == '':
        sudo("sed -i '$i <property>' %s" % fname)
        sudo("sed -i '$i %s' %s" % (prop, fname))
        sudo("sed -i '$i %s' %s" % (value, fname))
        sudo("sed -i '$i </property>' %s" % fname)
    else:
        num = int(result) + 1
        sudo("sed -i '%dc %s' %s" % (num, value, fname))


def setProperty(fname, prop, value):
    with settings(warn_only=True):
        result = run("nl %s | grep '%s' | awk '{print $1}'" % (fname, prop))
    if result == '':
        sudo("sed -i '$i %s' %s" % (prop + value, fname))
    else:
        num = int(result)
        sudo("sed -i '%dc %s' %s" % (num, prop + value, fname))

if __name__ == "__main__":
    execute(configProfile, "JAVA_HOME", "jdk")