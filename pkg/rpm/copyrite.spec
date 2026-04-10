Name:           copyrite
Version:        0.3.2
Release:        1%{?dist}
Summary:        CLI tool for efficient checksum and copy operations across object stores

License:        MIT
URL:            https://github.com/mmalenic/copyrite

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
* Fri Apr 10 2026 Marko Malenic <mmalenic1@gmail.com> - 0.3.1-1
- Release 0.3.1

* Fri Apr 10 2026 Marko Malenic <mmalenic1@gmail.com> - 0.3.0-1
- Release 0.3.0

* Wed Oct 22 2025 Marko Malenic <mmalenic1@gmail.com> - 0.1.0-1
- Initial package
