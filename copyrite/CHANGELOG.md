# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.9.2](https://github.com/umccr/copyrite/compare/v0.9.1...v0.9.2) - 2026-08-13

### Added

- warn when an explicit part size is ignored
- add full object and composite checksum tracking
- abort unfinished multipart copies
- carry system metadata through download-upload copies

### Fixed

- use default client-side checksum calculation for multipart uploads
- match checksum algorithms exhaustively
- correct the S3 maximum object size
- record errors not captured on best-effort paths
- require concurrency to be at least 1
- skip SDK response checksum validation on ranged reads
- no additional checksum should be supplied when it's configured off
- paginate object parts
- send per-part checksums for precalculated algorithms
- URL-encode copy source and tagging headers

### Other

- Merge pull request #120 from umccr/fix/stream-and-checksums
- assert multipart uploads store a full object checksum
- document memory use of precalculated part buffering
- add S3-compatible endpoint and Ceph docs

## [0.9.1](https://github.com/umccr/copyrite/compare/v0.9.0...v0.9.1) - 2026-08-11

### Fixed

- do not send pre-calculated checksum on create multipart

### Other

- Merge pull request #118 from umccr/fix/stream-and-checksums

## [0.9.0](https://github.com/umccr/copyrite/compare/v0.8.0...v0.9.0) - 2026-08-11

### Added

- expand cases where server-side copies can be used automatically
- add no-precalculated-checksum compatibility option

### Fixed

- send precalculated values for checksums the SDK cannot compute
- lazy source reads
- fixed bug with tag mode suppress

### Other

- fmt and removed some tests

## [0.8.0](https://github.com/umccr/copyrite/compare/copyrite-v0.7.0...copyrite-v0.8.0) - 2026-07-23

### Added

- [**breaking**] introduce copy success reason to better track why a copy was successful or skipped
- add md5 sum support and also fix encryption-based etag support
- add xxhash variants, md5 and sha512

### Fixed

- add check to ensure that copying when source == destination doesn't fail
- add new checksums to tests and properly run md5 through parts

### Other

- add checksum tests for md5, xxhash, sha512

## [0.7.0](https://github.com/umccr/copyrite/compare/v0.6.0...v0.7.0) - 2026-07-19

### Fixed

- pairing clients with their source/destination location properly

## [0.6.0](https://github.com/umccr/copyrite/compare/v0.5.1...v0.6.0) - 2026-07-16

### Fixed

- Introduced an option that disables request checksums (which had been enabled automatically in the AWS SDK)

## [0.5.1](https://github.com/umccr/copyrite/compare/v0.5.0...v0.5.1) - 2026-07-03

### Added

- improve error print output

## [0.5.0](https://github.com/umccr/copyrite/compare/v0.4.0...v0.5.0) - 2026-06-03

### Added

- add max object size setting
- add re-open read for aws based operations
- add re-open read for file based operations
- add streaming to multipart as well
- add with_reopen to copy when using best effort copying
- add re-open handle to allow re-fetching the source when required

### Fixed

- max object size should be 50 TiB not 5 TiB
- append on existing data for file copy
- correctly run upload tasks concurrently, single-part should be inclusive
- guard against invalid single-part copy attempts, and change < to <= in multipart detection
- apply reopen logic to avoid holding memory while using put object

### Other

- fmt
- add S3 tests for max size and single part check
- add file based tests for append/truncate
- avoid unnecessary clone on object info
- add test cases targeting retries and re-open
- add bytes dependency
- make the CopyContent re-open function required

## [0.4.0](https://github.com/umccr/copyrite/compare/v0.3.2...v0.4.0) - 2026-05-12

### Added

- add overrides for other S3 operations
- add wrapper calls to s3 client with overrides
- add SSP override options

### Other

- use wrapper functions when calling S3

## [0.3.2](https://github.com/umccr/copyrite/compare/v0.3.1...v0.3.2) - 2026-04-10

### Fixed

- rename variable

## [0.3.1](https://github.com/umccr/copyrite/compare/v0.3.0...v0.3.1) - 2026-04-10

### Other

- release v0.3.0

## [0.3.0](https://github.com/umccr/copyrite/releases/tag/v0.3.0) - 2026-04-10

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

- start from 0.3.0, update versions and permissions
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
