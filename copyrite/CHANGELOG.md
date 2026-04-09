# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/umccr/copyrite/releases/tag/v0.1.0) - 2026-04-09

### Added

- add source and destination options for the compatibility section
- change logic of source/destination prefixes for copies and allow un-prefixed options that apply both
- split out compatibility options to separate struct
- add rust version
- bump dependencies and fix new compiler errors
- add credential overrides and environment variables for source and destination
- add account-id option for account based endpoints
- add aws-secrets provider type

### Fixed

- the server-side copy should use the destination client for the `CopyObject` calls
- fall back to part number instead of part sizes for ceph

### Other

- fix typo
- fmt and clippy
- add integration test support for S3 compatible endpoints with a secret
- return error if client not set and always set from top-level code
- wire up client wrapper to all commands
- move the S3 client creation to a wrapper that owns the compatibility settings
- remove account_id option and clarify doc
- add space
- [**breaking**] prefix env options with `COPYRITE_` and apply line-wrapping, and differentiate long and short help clearer.
- add unit tests for merging and deserializing
- hide credentials to long-help only and clarify secret structure
- use clap-specific requires_if derive to validate required args
- add more detailed architecture docs
- update readme description and package description
- fix mocks from upgraded version
- update dependencies
- redo workspace change
