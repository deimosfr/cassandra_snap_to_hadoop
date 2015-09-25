FROM centos:6
MAINTAINER Pierre Mavro <p.mavro@criteo.com> <pierre@mavro.fr>

LABEL Description="This image is used to build a Python virtualenv and a RPM"

# Install rpm prequesites
RUN yum -y install rpmdevtools
RUN cd /root && rpmdev-setuptree

# Copy sources to containers
ADD . /root/rpmbuild/SOURCES
WORKDIR /root/rpmbuild/SOURCES

# Install dev dependencies
RUN yum -y install epel-release
RUN yum -y install python python-pip gcc krb5-devel python-devel libcurl-devel python-virtualenv libyaml-devel

# Build virtualenv
RUN virtualenv .
RUN source bin/activate && pip install -r requirements.txt && deactivate

# Build RPM from virtualenv
WORKDIR /root
RUN mv rpmbuild/SOURCES/build_rpm.spec rpmbuild/SPECS/build_rpm.spec
RUN rpmbuild -ba rpmbuild/SPECS/build_rpm.spec
