# Pre-requisites

* install hadoop-libhdfs
* set environment variable CLASSPATH depending on your libhdfs installed path，
	more details:http://www.rsyslog.com/doc/omhdfs.html

# Rsyslog v8

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

# Sendhdfs configuration
two ways to configure sendhdfs:

```
obtain parameters from command lines, you can get more details by typing option "-h", namely help documentation
```
```
configure your set by modifying sendhdfs.conf
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
