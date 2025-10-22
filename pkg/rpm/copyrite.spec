Name:           copyrite
Version:        0.1.0
Release:        1%{?dist}
Summary:        CLI tool for efficient checksum and copy operations across object stores

License:        MIT
URL:            https://github.com/mmalenic/copyrite
Source0:        %{name}-%{version}.tar.gz

BuildRequires:  rustc
BuildRequires:  cargo

%description
A CLI tool for efficient checksum and copy operations across object stores.

%prep
%autosetup

%build
cargo build --release

%install
install -D target/release/%{name} %{buildroot}%{_bindir}/%{name}

%files
%{_bindir}/%{name}

%changelog
* Wed Oct 22 2025 Marko Malenic <mmalenic1@gmail.com> - 0.1.0-1
- Initial package
