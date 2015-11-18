FROM centos:6
MAINTAINER Pierre Mavro <p.mavro@criteo.com> <pierre@mavro.fr>

LABEL Description="This image is used to build a Python virtualenv and a RPM"

# Install dev dependencies
RUN yum -y install epel-release
RUN yum -y install python python-pip gcc krb5-devel python-devel libcurl-devel python-virtualenv libyaml-devel tar git

# Install rpm prequesites
RUN yum -y install rpmdevtools

# Copy sources to containers
RUN mkdir -p /root/cass_snap
ADD . /root/cass_snap
WORKDIR /root/cass_snap

# Build virtualenv
RUN virtualenv python26
RUN source python26/bin/activate && pip install -r requirements.txt && deactivate

# Apply patch
# https://github.com/requests/requests-kerberos/issues/54
RUN cp patchs/kerberos_.py python26/lib/python2.6/site-packages/requests_kerberos/

# Build RPM from virtualenv
WORKDIR /root
RUN rpmdev-setuptree
RUN mv cass_snap/cassnap2hadoop_build_rpm.spec rpmbuild/SPECS/
RUN rm -Rf cass_snap/{Dockerfile,LICENSE,patchs,*.keytab,requirements.txt,test,setup.py,.git*,rpmbuild}

RUN awk -F"'" '/^__version__/{print $2}' cass_snap/cassnap_manage.py > /root/version
RUN mv cass_snap cassnap2hadoop-$(cat version)
RUN tar -czf /root/cassnap2hadoop-$(cat version).tgz cassnap2hadoop-$(cat version)
RUN mv *.tgz rpmbuild/SOURCES/
RUN rpmbuild -ba rpmbuild/SPECS/cassnap2hadoop_build_rpm.spec
