Name:           cassnap2hadoop
Version:        0.1
Release:        1%{?dist}
Summary:        Cassandra snapshot to Hadoop tool

Group:          Backup Client
License:        GPL
URL:            https://github.com/deimosfr/cassandra_snap_to_hadoop

BuildRequires:  epel-release python python-pip gcc krb5-devel python-devel libcurl-devel python-virtualenv libyaml-devel
Requires:       /usr/bin/python

%description
This tool is able to make and send Cassandra snapshots to a Hadoop Cluster.
It is also able to make incremental snapshot, restore and list available
snapshots.

%build
rm -Rf %{_builddir}
mkdir -p %{_builddir}/%{_datadir}/%{name} %{_builddir}/%{_bindir}
cp -Rf %{_sourcedir}/* %{_builddir}/%{_datadir}/%{name}
cp %{_sourcedir}/scripts/cassnap_manage %{_builddir}/%{_bindir}/cassnap_manage
chmod 755 %{_builddir}/%{_bindir}/cassnap_manage
rm -Rf %{_builddir}/%{_datadir}/%{name}/.git

%install
cp -Rf %{_builddir}/* %{buildroot}

%clean
rm -Rf %{_builddir}

%files
%defattr(-,root,root,-)
%{_datadir}/%{name}
%{_bindir}/cassnap_manage

%doc

%changelog
* Thu Oct 1 2015 Pierre Mavro <p.mavro@criteo.com> 0.1
- First release
