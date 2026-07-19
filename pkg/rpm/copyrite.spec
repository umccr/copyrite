%{!?version: %global version 0.7.0}

Name:           copyrite
Version:        %{version}
Release:        1%{?dist}
Summary:        CLI tool for efficient checksum and copy operations across object stores

License:        MIT
URL:            https://github.com/umccr/copyrite

%global debug_package %{nil}

%description
A CLI tool for efficient checksum and copy operations across object stores.

%build
cargo build --release

%install
install -D target/release/%{name} %{buildroot}%{_bindir}/%{name}

%files
%{_bindir}/%{name}

%changelog
* Mon Jul 21 2026 Marko Malenic <mmalenic1@gmail.com> - 0.7.0-1
- fix: pairing clients with their source/destination location properly by @mmalenic in https://github.com/umccr/copyrite/pull/97

* Wed Jul 16 2026 Marko Malenic <mmalenic1@gmail.com> - 0.6.0-1
- fix: option to disable request checksums by @andrewpatto in https://github.com/umccr/copyrite/pull/94

* Fri Jul 03 2026 Marko Malenic <mmalenic1@gmail.com> - 0.5.1-1
- feat: improve error print output by @mmalenic in https://github.com/umccr/copyrite/pull/87

* Thu Jun 04 2026 Marko Malenic <mmalenic1@gmail.com> - 0.5.0-1
- fix: in-memory bytes by @mmalenic in https://github.com/umccr/copyrite/pull/79
- fix: max part size copy by @mmalenic in https://github.com/umccr/copyrite/pull/82

* Tue May 12 2026 Marko Malenic <mmalenic1@gmail.com> - 0.4.0-1
- ci: check if assets are found on release by @mmalenic in https://github.com/umccr/copyrite/pull/73
- ci: docker.yml needs to use the same tag logic as release-bins.yml by @mmalenic in https://github.com/umccr/copyrite/pull/74
- feat: stalled stream override by @mmalenic in https://github.com/umccr/copyrite/pull/75

* Fri Apr 10 2026 Marko Malenic <mmalenic1@gmail.com> - 0.3.1-1
- Release 0.3.1

* Fri Apr 10 2026 Marko Malenic <mmalenic1@gmail.com> - 0.3.0-1
- Release 0.3.0

* Wed Oct 22 2025 Marko Malenic <mmalenic1@gmail.com> - 0.1.0-1
- Initial package
