# Pre-requisites

* install hadoop-libhdfs
* set environment variable CLASSPATH depending on your libhdfs installed path

# rsyslog v8

add Centos repository for rsyslog

```
cd /etc/yum.repo.d
wget http://rpms.adiscon.com/v8-stable/rsyslog.repo
sudo yum update
```

then install rsyslog-v8

```
sudo yum list rsyslog
sudo yum install rsyslog
```

# Building and installing sendhdfs

```
make
mv sendhdfs /usr/local/bin
```

# Rsyslog configuration

/etc/rsyslog.conf configuration directive:

At the start:

```
$ModLoad omprog
```

After the very last line:

```
$ActionOMProgBinary /usr/local/bin/sendhdfs
*.* :omprog:;cee_enhanced
```

Check configuration

```
rsyslogd -f /etc/rsyslog.conf -N1
```
