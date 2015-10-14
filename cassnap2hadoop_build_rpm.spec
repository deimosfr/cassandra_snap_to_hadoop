Name:           cassnap2hadoop
Version:        0.1
Release:        1%{?dist}
Summary:        Cassandra snapshot to Hadoop tool

Group:          Backup Client
License:        GPL
URL:            https://github.com/deimosfr/cassandra_snap_to_hadoop

BuildRequires:  python python-pip gcc krb5-devel python-devel libcurl-devel python-virtualenv libyaml-devel
Requires:       /usr/bin/python
Source0:  %{name}-%{version}.tgz

%description
This tool is able to make and send Cassandra snapshots to a Hadoop Cluster.
It is also able to make incremental snapshot, restore and list available
snapshots.


%prep
%setup

%build

%install
mkdir -p %{buildroot}/%{_datadir}/%{name}
mkdir -p %{buildroot}/usr/bin
cp -Rf %{_builddir}/%{name}-%{version}/* %{buildroot}/%{_datadir}/%{name}
cp -Rf %{_builddir}/%{name}-%{version}/scripts/* %{buildroot}/usr/bin
rm -rf %{buildroot}/%{_datadir}/%{name}/scripts

%clean
rm -Rf %{_builddir}

%files
%defattr(-,root,root,-)
%{_datadir}
%attr(755, root, root) /usr/bin

%doc

%changelog
* Thu Oct 1 2015 Pierre Mavro <p.mavro@criteo.com> 0.1
- First release
