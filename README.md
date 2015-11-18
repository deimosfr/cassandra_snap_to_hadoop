# Cassandra Snapshot to Hadoop utility 

[![Build Status](https://travis-ci.org/deimosfr/cassandra_snap_to_hadoop.svg?branch=master)](https://travis-ci.org/deimosfr/cassandra_snap_to_hadoop)

This tool is made to perform Cassandra Snapshot and store them on a Hadoop cluster.

NOTE: this is currently in development

# Install

## CentOS

First of all you need to get the EPEL repository to be able to install pip then:
```
yum install epel-release
```

Then, you will need the following packages to get it work:
```
yum install python python-pip
```

### Libraries build dependencies

If you need to build python libraries, you'll need the following packages:
```
yum install gcc krb5-devel python-devel libcurl-devel
```

## Python lib

You can use your package manager to install python libraries dependancies or yu can use pip:
```
pip install -r requirements.txt
```

# Usage

## Kerberos ticket

A keytab usage is required in order to perform a Kerberos authentication against a Hadoop Cluster.
To begin, be sure you've got your krb5.conf updated with your Kerberos server information.
Then create a Kerberos client ticket:
```
$ ktutil
ktutil:  addent -password -p <username>@<DOMAIN> -k 1 -e aes256-cts
Password for <username>@<DOMAIN>: [enter your password]
ktutil:  wkt username.keytab
ktutil:  quit 
```

You will need 'krb5-workstation' package on Centos to get kinit command.

To finish, request a Kerberos ticket:
```
kinit <username|principal>@<DOMAIN> -k -t username.keytab
```

# Notes

You may encounter issues when you'll want to connect to Kerberos.
Following the opened issue https://github.com/requests/requests-kerberos/issues/54, you may need to
patch the kerberos_.py library as described here:

```
@@ -149,6 +149,7 @@
         response.content
         response.raw.release_conn()
 
+        self.deregister(response)
         _r = response.connection.send(response.request, **kwargs)
         _r.history.append(response)
```

# Build sources and RPM

To build dependencies and make an RPM, there is an existing Dockerfile at the root directory of the project.
You only need to run it to build everything:

```
docker build -t cass_snap .
CONTAINER=$(docker run -d -v "`pwd`:/mnt" cass_snap bash -c "cp -Rfv /root/rpmbuild /mnt/")
docker attach $CONTAINER
docker rm -f $CONTAINER
```
You now have a folder "rpmbuild" with the compiled sources and the RPM.
