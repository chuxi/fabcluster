#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    auto ssh
"""

__author__ = 'king'

import os
from fabric.colors import *
from fabric.api import *
from fabric.context_managers import *
from fabric.contrib.console import confirm

env.user = 'vlis'
env.password = 'zjuvlis'
# env.hostnames = {'10.214.208.11': 'node1',
#                  '10.214.208.12': 'node2',
#                  '10.214.208.13': 'node3',
#                  '10.214.208.14': 'node4',
#                  }
clusters = ['10.214.208.11', '10.214.208.12', '10.214.208.13', '10.214.208.14']

env.hostnames = dict([h, 'node%d' % (i+1)] for i, h in enumerate(clusters))

env.keywords = ['jdk', 'hadoop', 'zookeeper', 'hbase', 'kafka', 'spark']
# 获取需要安装的tar包名称
env.fnames = dict(zip(sorted(env.keywords), sorted(os.listdir('./tars'))))

env.roledefs = {
    'cluster': clusters,
    'hadoop_master': clusters[:1],
    'hadoop_smaster': clusters[1:2],
    'hadoop_slaves': clusters,
    'zookeeper': clusters[1:],
    'hbase': clusters,
    'hbase_master': clusters[:1],
    'hbase_slaves': clusters,
    'kafka': clusters[2:],
    'spark': clusters,
    'spark_master': clusters[2:3],
    'spark_slaves': clusters
}



baseDir = '/home/hadoop'
optDir = '/opt/vlis'
tmpDir = '/tmp'

newgroup = 'hadoop'
newuser = 'hadoop'
newpasswd = 'hadoop'


@runs_once
def prelocal():
    '''
    在/tmp目录下准备一些配置需要的临时文件
    :return: None
    '''
    # 准备hosts文件
    local('rm -rf /tmp/hosts')
    with lcd('/tmp'):
        local('touch hosts')
        local("echo '127.0.0.1 localhost' > hosts")
        for host in env.hostnames:
            local("echo '%s %s' >> hosts" % (host, env.hostnames[host]))

    # 准备ssh的id_rsa，id_rsa.pub和authorized_keys文件
    local('rm -rf /tmp/ssh')
    local('mkdir -p /tmp/ssh')
    with lcd('/tmp/ssh'):
        local('rm -rf *')
        local('ssh-keygen -t rsa -N "" -f id_rsa')
        local('cat id_rsa.pub > authorized_keys')
        local('echo "StrictHostKeyChecking no" > config')




def setHosts():
    with settings(warn_only=True):
        result = put("/tmp/hosts", "/tmp/hosts")
    if result.failed and not confirm("put file failed, Continue[Y/N]"):
        abort("Aborting file put task - %s!" % setHosts.__name__)
    sudo('mv /tmp/hosts /etc/')


def setHostnames():
    # modify hostname
    sudo("hostname %s" % env.hostnames[env.host_string])
    with cd('/etc'):
        sudo("echo %s > hostname" % env.hostnames[env.host_string])


def setSSHs():
    sshpath = '/home/%s/.ssh' % newuser
    sudo('rm -rf %s' % sshpath)
    sudo('mkdir -p %s' % sshpath)
    with settings(warn_only=True):
        sudo('rm -rf /tmp/ssh')
        run('mkdir -p /tmp/ssh')
        result = put("/tmp/ssh/*", '/tmp/ssh')
    if result.failed and not confirm("put ssh file failed, Continue[Y/N]"):
        abort("Aborting file put ssh task - %s!" % setSSHs.__name__)
    sudo('cp -rf /tmp/ssh/* %s' % sshpath)
    with cd(sshpath):
        sudo('chmod 600 id_rsa')
        sudo('chmod 600 authorized_keys')
        sudo('chmod 600 config')
    sudo('chmod 755 %s' % sshpath)
    sudo('chown -R %s:%s %s' % (newuser, newgroup, sshpath))


def disableFirewall():
    print yellow('start to stop %s firewall...' % env.host_string)

    result = run('python -c "import platform; os, ver, a = platform.dist(); print os, ver"')
    myos = result.split(' ')[0].lower()
    myver = result.split(' ')[1]
    with settings(warn_only=True):
        if myos == 'centos':
            sudo('setenforce 0')
            sudo('sed -i "s#SELINUX=.*#SELINUX=disabled#" /etc/selinux/config')
            if myver.startswith('7'):
                sudo('systemctl stop firewalld.service')
                sudo('systemctl disable firewalld.service')
            elif myver.startswith('6'):
                sudo('service iptables stop')
                sudo('chkconfig iptables off')
            else:
                abort("Sorry! The operating system is not supported now!")
        elif myos == 'ubuntu':
            sudo('ufw disable')
        else:
            abort("Sorry! The operating system is not supported now!")

    print green("%s firewall is disabled successfully!" % env.host_string)

# add a user and group - hadoop
def addUser():
    print yellow('start to add user hadoop %s ...' % env.host_string)

    with hide('stdout'), settings(warn_only=True):
        if run('cat /etc/passwd').find(newuser) != -1:
            sudo('rm -rf /home/%s' % newuser)
            sudo('userdel -f %s' % newuser)
        if run('cat /etc/group').find(newgroup) != -1:
            sudo('groupdel %s' % newgroup)

    sudo('groupadd %s' % newgroup)
    sudo('useradd -g %s -m -r %s' % (newgroup, newuser))
    sudo('echo %s:%s | chpasswd' % (newuser, newpasswd))
    sudo('sed -i \'s/.*%s.*//g\' /etc/sudoers' % newuser)
    sudo('sed -i \'$a hadoop  ALL=(ALL)       ALL\' /etc/sudoers')
    print green('add user hadoop %s successfully!' % env.host_string)


# add NTP
def setNTP():
    print yellow('start to install NTP service.')
    result = run('python -c "import platform; os, ver, a = platform.dist(); print os, ver"')
    myos = result.split(' ')[0].lower()
    myver = result.split(' ')[1]
    with settings(warn_only=True):
        if myos == 'centos':
            sudo('yum -y install ntp')
            if myver.startswith('7'):
                sudo('systemctl start ntpd.service')
                sudo('systemctl enable ntpd.service')
            elif myver.startswith('6'):
                sudo('service ntpd start')
                sudo('chkconfig ntpd on')
            else:
                abort("Sorry! The operating system is not supported now!")
        elif myos == 'ubuntu':
            sudo('apt-get install ntp')
            sudo('service ntp restart')
        else:
            abort("Sorry! The operating system is not supported now!")

    print green('install NTP service successfully!')

def mkDirs():
    '''
        添加需要预先设置的目录
    '''
    with settings(warn_only=True):
        sudo('[ ! -d %s ] && mkdir -p %s && chown -R %s:%s %s' % (optDir, optDir, newuser, newgroup, optDir))


def puttar(fname):
    print yellow('start to put %s' % fname)
    with cd("/tmp"):
        if run('ls').find(fname) == -1:
            with settings(warn_only=True):
                result = put("./tars/%s" % fname, "/tmp/%s" % fname)
            if result.failed and not confirm("put tar file failed, Continue[Y/N]"):
                abort("Aborting file put tar task!")
        else:
            print yellow('%s already exists! ' % fname)

def checkmd5(fname):
    with settings(warn_only=True):
        lmd5=local("md5sum ./tars/" + fname, capture=True).split(' ')[0]
        rmd5=run("md5sum /tmp/" + fname).split(' ')[0]
    if lmd5 == rmd5:
        print green("Successfully put " + fname + " !")
    else:
        print red("failed to put " + fname + " !")

def untarfile(key, fname):
    with cd("/tmp"):
        dirname = run("tar tf " + fname + " | head -n 1 | awk -F / '{{print $1}}'")
        with cd(optDir):
            sudo("rm -rf " + dirname)
        with settings(warn_only=True):
            sudo("tar -xzf %s -C %s" % (fname, optDir))
        with cd(optDir):
            sudo('chown -R %s:%s %s' % (newuser, newgroup, dirname))
    with cd("/usr/local"):
        sudo("rm -rf " + key)
        sudo("ln -s %s/%s %s" % (optDir, dirname, key))


def processTar(key, fname):
    puttar(fname)
    checkmd5(fname)
    untarfile(key, fname)


@roles('cluster')
def installJDK():
    processTar('jdk', env.fnames['jdk'])
    configProfile('JAVA_HOME', 'jdk')


def configProfile(envname, key):
    sudo('sed -i \'s/.*%s.*//g\' /etc/profile' % envname)
    sudo('sed -i \'$a export %s=/usr/local/%s\' /etc/profile' % (envname, key))
    sudo('sed -i \'$a export PATH=$%s/bin:$PATH\' /etc/profile' % envname)

def setXMLPropVal(fname, prop, value):
    prop = '<name>' + prop + '</name>'
    value = '<value>%s</value>' % value
    with settings(warn_only=True):
        result = run('nl %s | grep \'%s\'' % (fname, prop))
    if result == '':
        sudo('sed -i \'$i <property>\' %s' % fname)
        sudo('sed -i \'$i %s\' %s' % (prop, fname))
        sudo('sed -i \'$i %s\' %s' % (value, fname))
        sudo('sed -i \'$i </property>\' %s' % fname)
    else:
        num = int(run('grep -n \'%s\' %s' % (prop, fname)).split(':')[0]) + 1
        sudo('sed -i \'%dc %s\' %s' % (num, value, fname))

def setProperty(fname, prop, value):
    sudo('sed -i \'s/.*%s.*//g\' %s' % (prop, fname))
    sudo('sed -i \'$a %s\' %s' % (prop + value, fname))






@roles('cluster')
def installHadoop():
    key = 'hadoop'
    processTar(key, env.fnames[key])
    configProfile('HADOOP_HOME', 'hadoop')
    configHadoop()

@roles('cluster')
def configHadoop():
    configDir = '/usr/local/hadoop/etc/hadoop'

    coresite = configDir + '/core-site.xml'
    hdfssite = configDir + '/hdfs-site.xml'
    mapredsite = configDir + '/mapred-site.xml'
    yarnsite = configDir + '/yarn-site.xml'

    # hadoop-env.sh
    setProperty(configDir + '/hadoop-env.sh', 'export JAVA_HOME=', r'\/usr\/local\/jdk')

    # core-site.xml
    setXMLPropVal(coresite, 'fs.defaultFS', 'hdfs://%s:9000' % env.roledefs['hadoop_master'][0])
    setXMLPropVal(coresite, 'io.file.buffer.size', '131072')

    # hdfs-site.xml
    setXMLPropVal(hdfssite, 'dfs.namenode.name.dir', 'file:/home/%s/dfs/name' % newuser)
    setXMLPropVal(hdfssite, 'dfs.datanode.data.dir', 'file:/home/%s/dfs/data' % newuser)
    setXMLPropVal(hdfssite, 'dfs.namenode.secondary.http-address', '%s:50090' % env.roledefs['hadoop_smaster'][0])
    setXMLPropVal(hdfssite, 'dfs.replication', '3')
    setXMLPropVal(hdfssite, 'dfs.permissions.enabled', 'false')

    # yarn-site.xml
    setXMLPropVal(yarnsite, 'yarn.resourcemanager.hostname', env.roledefs['hadoop_master'][0])
    setXMLPropVal(yarnsite, 'yarn.nodemanager.aux-services', 'mapreduce_shuffle')

    # mapred-site.xml
    with settings(user = newuser, password = newpasswd, warn_only=True), cd(configDir):
        run('[ ! -e mapred-site.xml ] && cp mapred-site.xml.template mapred-site.xml')
    setXMLPropVal(mapredsite, 'mapreduce.framework.name', 'yarn')


    # slaves
    with cd(configDir):
        sudo('rm slaves')
        sudo('touch slaves')
        sudo('chown -R %s:%s %s' % (newuser, newgroup, 'slaves'))
        for k in env.roledefs['hadoop_slaves']:
            sudo('echo %s >> slaves' % k)


@roles('zookeeper')
def installZookeeper():
    k = 'zookeeper'
    processTar(k, env.fnames[k])
    configProfile('ZOOKEEPER_HOME', k)
    configZookeeper()

@roles('zookeeper')
def configZookeeper():
    configDir = '/usr/local/zookeeper/conf'
    dataDir = '/home/%s/zookeeper' % newuser

    with settings(user = newuser, password = newpasswd):
        with cd(configDir):
            sudo('cp zoo_sample.cfg zoo.cfg')
            setProperty('zoo.cfg', 'dataDir=', dataDir)
            count = 0
            for h in env.roledefs['zookeeper']:
                count = count + 1
                setProperty('zoo.cfg', 'server.%d=' % count, '%s:2888:3888' % h)

        myhost = env.host_string
        with settings(warn_only = True):
            rs = run('grep \'%s\' %s' % (myhost + ':2888:3888', configDir + '/zoo.cfg'))
        num = int(rs.split('=')[0].split('.')[1])

        run('rm -rf %s' % dataDir)
        run('mkdir -p %s' % dataDir)
        with cd(dataDir):
            run('touch %s' % 'myid')
            run('echo %d > myid' % num)

@roles('hbase')
def installHBase():
    k = 'hbase'
    processTar(k, env.fnames[k])
    configProfile('HBASE_HOME', k)
    configHBase()

@roles('hbase')
def configHBase():
    configDir = '/usr/local/hbase/conf'

    # hbase-env.sh
    setProperty(configDir + '/hbase-env.sh', 'export JAVA_HOME=', r'\/usr\/local\/jdk')
    setProperty(configDir + '/hbase-env.sh', 'export HBASE_MANAGES_ZK=', 'false')

    # hbase-site.xml
    setXMLPropVal(configDir + '/hbase-site.xml', 'hbase.rootdir', 'hdfs://%s:9000/hbase' % env.roledefs['hadoop_master'][0])
    setXMLPropVal(configDir + '/hbase-site.xml', 'hbase.cluster.distributed', 'true')
    setXMLPropVal(configDir + '/hbase-site.xml', 'hbase.zookeeper.quorum', '%s' % (','.join(x for x in env.roledefs['zookeeper'])))
    setXMLPropVal(configDir + '/hbase-site.xml', 'hbase.zookeeper.property.dataDir', '/home/%s/zookeeper' % newuser)

    # regionservers
    with cd(configDir):
        with settings(user = newuser, password = newpasswd):
            run('rm -rf regionservers')
            run('touch regionservers')
            for k in env.roledefs['hbase_slaves']:
                run('echo %s >> regionservers' % k)


@roles('kafka')
def installKafka():
    k = 'kafka'
    processTar(k, env.fnames[k])
    configProfile('KAFKA_HOME', k)
    configKafka()

@roles('kafka')
def configKafka():
    configDir = '/usr/local/kafka/config'

    # server.properties
    setProperty(configDir + '/server.properties', 'broker.id=', env.hostnames[env.host_string][4:])
    setProperty(configDir + '/server.properties', 'log.dirs=', '/home/%s/kafka' % newuser)
    setProperty(configDir + '/server.properties', 'zookeeper.connect=', (','.join(x + ':2181' for x in env.roledefs['zookeeper'])))


@roles('spark')
def installSpark():
    k = 'spark'
    processTar(k, env.fnames[k])
    configProfile('SPARK_HOME', k)
    configSpark()

@roles('spark')
def configSpark():
    configDir = '/usr/local/spark/conf'

    # slaves
    with cd(configDir):
        with settings(user = newuser, password = newpasswd):
            run('rm -rf slaves')
            run('touch slaves')
            for k in env.roledefs['spark_slaves']:
                run('echo %s >> slaves' % k)
            run('cp spark-env.sh.template spark-env.sh')
            run('cp spark-defaults.conf.template spark-defaults.conf')

    # spark-env.sh
    setProperty(configDir + '/spark-env.sh', 'JAVA_HOME=', '\/usr\/local\/jdk')
    setProperty(configDir + '/spark-env.sh', 'HADOOP_CONF_DIR=', '\/usr\/local\/hadoop\/etc\/hadoop')
    setProperty(configDir + '/spark-env.sh', 'SPARK_LOCAL_DIRS=', '/home/%s/spark' % newuser)

    # spark-defaults.conf
    setProperty(configDir + '/spark-defaults.conf', 'spark.master', ' spark://' + env.roledefs['spark_master'][0] + ':7077')
    setProperty(configDir + '/spark-defaults.conf', 'spark.eventLog.enabled', ' true')
    setProperty(configDir + '/spark-defaults.conf', 'spark.eventLog.dir', ' hdfs:///spark-event-log')




@roles('hadoop_master')
def startHadoop():
    reformat = prompt('would you like to reformat the namenode? (Y / N): ', default='N')
    if reformat == 'Y':
        with settings(user = newuser, password = newpasswd):
            run('hdfs namenode -format')
    elif reformat == 'N':
        print green('No namenode format.')
    else:
        print yellow('You just typed something wrong! No namenode format. ')

    if not checkHadoop():
        st = prompt('1. start dfs; \n2. start yarn; \n3. start all; \n Choose one in (1, 2, 3): ', default='1')

        if st == '1':
            with settings(user = newuser, password = newpasswd):
                run('/usr/local/hadoop/sbin/start-dfs.sh')
        elif st == '2':
            with settings(user = newuser, password = newpasswd):
                run('/usr/local/hadoop/sbin/start-yarn.sh')
        elif st == '3':
            with settings(user = newuser, password = newpasswd):
                run('/usr/local/hadoop/sbin/start-all.sh')
        run('sleep 3')


@roles('hadoop_master')
def stopHadoop():
    with hide('stdout'), settings(user = newuser, password = newpasswd, warn_only = True):
        run('/usr/local/hadoop/sbin/stop-all.sh')
        rs = run('jps | grep -P \'NameNode|DataNode\'')
        if rs != '':
            abort('can not stop hadoop')
    print green('hadoop stopped!')


@roles('hadoop_master')
def checkHadoop():
    with hide('stdout'), settings(user = newuser, password = newpasswd, warn_only = True):
        rs = run('jps | grep -P \'NameNode|DataNode\'')
        if rs != '':
            return True
        else:
            return False

@roles('cluster')
def cleanHadoop():
    with settings(user = newuser, password = newpasswd):
        run('rm -rf /home/%s/dfs' % newuser)


@roles('zookeeper')
def startZookeeper():
    with settings(user = newuser, password = newpasswd):
        run('zkServer.sh start')

@roles('zookeeper')
def stopZookeeper():
    with settings(user = newuser, password = newpasswd):
        run('zkServer.sh stop')

@roles('zookeeper')
def cleanZookeeper():
    with settings(user = newuser, password = newpasswd):
        run('rm -rf /home/%s/zookeeper/version-2' % newuser)

@roles('hbase_master')
def startHBase():
    with settings(user = newuser, password = newpasswd):
        run('start-hbase.sh; sleep 3')

@roles('hbase_master')
def stopHBase():
    with settings(user = newuser, password = newpasswd, warn_only = True):
        run('stop-hbase.sh')

@roles('kafka')
def startKafka():
    with settings(user = newuser, password = newpasswd):
        run('kafka-server-start.sh -daemon /usr/local/kafka/config/server.properties; sleep 1')


@roles('kafka')
def stopKafka():
    with settings(user = newuser, password = newpasswd, warn_only = True):
        run('ps ax | grep -i \'kafka\.Kafka\' | grep java | grep -v grep | awk \'{print $1}\' | xargs kill -9')

@roles('spark_master')
def startSpark():
    with settings(user = newuser, password = newpasswd):
        run('/usr/local/spark/sbin/start-all.sh; sleep 3')

@roles('spark_master')
def stopSpark():
    with settings(user = newuser, password = newpasswd):
        run('/usr/local/spark/sbin/stop-all.sh')


@roles('cluster')
def preset():
    prelocal()
    setHosts()
    setHostnames()
    addUser()
    setSSHs()
    disableFirewall()
    setNTP()
    mkDirs()


def basedeploy():
    execute(preset)
    execute(installJDK)


def deploy(op=None):
    if op == None:
        execute(installHadoop)
        execute(installZookeeper)
        execute(installHBase)
        execute(installKafka)
        execute(installSpark)
    elif op == 'hadoop':
        execute(installHadoop)
    elif op == 'hbase':
        execute(installHadoop)
        execute(installZookeeper)
        execute(installHBase)
    elif op == 'kafka':
        execute(installKafka)
    elif op == 'spark':
        execute(installSpark)
    else:
        pass


def configuration(op=None):
    if op == None:
        execute(configHadoop)
        execute(configZookeeper)
        execute(configHBase)
        execute(configKafka)
        execute(configSpark)
    elif op == 'hadoop':
        execute(configHadoop)
    elif op == 'hbase':
        execute(configHBase)
    elif op == 'kafka':
        execute(configKafka)
    elif op == 'spark':
        execute(configSpark)
    pass

@task
def starts(op=None):
    if op == None:
        execute(startHadoop)
        execute(startZookeeper)
        execute(startHBase)
        execute(startKafka)
        execute(startSpark)
    elif op == 'hadoop':
        execute(startHadoop)
    elif op == 'hbase':
        execute(startZookeeper)
        execute(startHBase)
    elif op == 'kafka':
        execute(startKafka)
    elif op == 'spark':
        execute(startSpark)
    else:
        pass

@task
def installs(op=None):
    execute(preset)
    basedeploy()
    deploy(op)
    starts(op)

@task
def configs(op=None):
    configuration(op)


@task
def stops(op=None):
    if op == None:
        execute(stopSpark)
        execute(stopKafka)
        execute(stopHBase)
        execute(stopHadoop)
        execute(stopZookeeper)
    elif op == 'spark':
        execute(stopSpark)
    elif op == 'kafka':
        execute(stopKafka)
    elif op == 'hbase':
        execute(stopHBase)
    elif op == 'hadoop':
        execute(stopHadoop)
    elif op == 'zookeeper':
        execute(stopZookeeper)
    else:
        pass


@task
def cleans(op=None):
    if op == None:
        execute(cleanHadoop)
        execute(cleanZookeeper)
    elif op == 'hadoop':
        execute(cleanHadoop)
    elif op == 'zookeeper':
        execute(cleanZookeeper)
    else:
        pass

@task
@roles('cluster')
def status():
    with settings(user = newuser, password = newpasswd):
        run('jps')