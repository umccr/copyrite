# Requirements Document

## Introduction

This document specifies the requirements for adding SFTP as a third provider backend to copyrite, a Rust CLI tool for efficient checksum and copy operations across object stores. The SFTP provider enables users to generate checksums, verify file integrity, and copy files to/from remote SFTP servers using the same interface as existing File and S3 backends.

## Glossary

- **Provider**: An enum representing a storage backend (File, S3, or Sftp) that copyrite can read from or write to.
- **ObjectCopy**: A trait defining write/transfer operations including download, upload, multipart copy, and state initialization.
- **ObjectSums**: A trait defining checksum-related read operations including sums file read/write, file size retrieval, and streaming file content.
- **SftpClient**: A wrapper around an async SFTP session providing file operations (read, write, stat) on a remote server.
- **SftpEndpoint**: A struct identifying a unique SFTP connection by user, host, and port.
- **SftpAuth**: An enum representing authentication methods for SFTP connections (PrivateKey, Password).
- **SumsFile**: A JSON file storing previously computed checksums for a given file.
- **CopyContent**: A stream of bytes with a reopen factory for retry support during upload operations.
- **MultiPartOptions**: Parameters controlling byte-range transfers for large file copies.
- **ObjectCopyBuilder**: A builder that dispatches to the correct ObjectCopy implementation based on Provider type.
- **ObjectSumsBuilder**: A builder that dispatches to the correct ObjectSums implementation based on Provider type.
- **Feature_Flag**: A cargo feature gate (`sftp`) that conditionally compiles SFTP support.

## Requirements

### Requirement 1: SFTP URL Parsing

**User Story:** As a user, I want to specify SFTP locations using a URL format, so that I can reference remote files consistently with other provider URLs.

#### Acceptance Criteria

1. WHEN a URL string starts with `sftp://`, THE Provider parser SHALL parse it into a Provider::Sftp variant with user, host, port, and path fields.
2. WHEN the SFTP URL omits the username component, THE Provider parser SHALL default the user field to the current operating system username.
3. WHEN the SFTP URL omits the port component, THE Provider parser SHALL default the port field to 22.
4. WHEN the SFTP URL contains an explicit port, THE Provider parser SHALL parse and store the port as a u16 value.
5. IF the SFTP URL has an empty host component, THEN THE Provider parser SHALL return a ParseError.
6. IF the SFTP URL has an empty or missing path component, THEN THE Provider parser SHALL return a ParseError.
7. THE Provider parser SHALL produce a path field that is always absolute (starts with `/`).
8. WHEN a Provider::Sftp value is formatted to a string and re-parsed, THE Provider parser SHALL produce an equivalent Provider::Sftp value (round-trip property).

### Requirement 2: SFTP Authentication

**User Story:** As a user, I want to authenticate to SFTP servers using a private key or password, so that I can connect securely using my preferred credentials.

#### Acceptance Criteria

1. WHEN `--sftp-key` is provided, THE SftpClient SHALL authenticate using the specified private key file.
2. WHEN `--sftp-key` and `--sftp-passphrase` are both provided, THE SftpClient SHALL decrypt the private key using the passphrase before authenticating.
3. WHEN `--sftp-password` is provided and `--sftp-key` is not provided, THE SftpClient SHALL authenticate using username/password authentication.
4. IF private key loading fails due to a non-existent file path or incorrect passphrase, THEN THE SftpClient SHALL return a CopyError indicating the cause of the key loading failure.
5. IF password authentication is rejected by the server, THEN THE SftpClient SHALL return a CopyError indicating authentication was rejected.
6. IF both `--sftp-key` and `--sftp-password` are provided, THEN THE SftpClient SHALL use private key authentication and ignore the password flag.
7. IF neither `--sftp-key` nor `--sftp-password` is provided, THEN THE SftpClient SHALL return an error indicating that an SFTP authentication method must be specified via `--sftp-key` or `--sftp-password`.

### Requirement 3: SFTP Connection Management

**User Story:** As a user, I want copyrite to manage SFTP connections efficiently, so that operations complete without unnecessary reconnection overhead.

#### Acceptance Criteria

1. WHEN SftpClient::connect is called with a valid SftpEndpoint and SftpAuth credentials, THE SftpClient SHALL establish an authenticated SSH session and open the SFTP subsystem within 30 seconds.
2. THE SftpClient SHALL hold the underlying SFTP session behind an Arc<Mutex<_>> for safe concurrent access.
3. IF the remote host is unreachable or DNS resolution fails, THEN THE SftpClient SHALL return a CopyError containing "SSH connection failed" and the underlying OS or network error.
4. IF the SSH handshake fails, THEN THE SftpClient SHALL return a CopyError with the underlying failure reason.
5. IF the SSH session drops during a read or write operation, THEN THE SftpClient SHALL propagate the IO error to the caller without automatic reconnection.
6. IF the TCP connection to the remote host is not established within 30 seconds, THEN THE SftpClient SHALL return a CopyError indicating a connection timeout.
7. WHEN all operations using the SftpClient are complete, THE SftpClient SHALL close the underlying SSH session on drop.

### Requirement 4: SFTP ObjectCopy Implementation

**User Story:** As a user, I want to copy files to and from SFTP servers, so that I can transfer data between SFTP and other storage backends.

#### Acceptance Criteria

1. WHEN download is called without multipart options, THE SftpCopyFile SHALL read the entire remote source file and return it as a CopyContent stream.
2. WHEN download is called with multipart options specifying a byte range, THE SftpCopyFile SHALL seek to the MultiPartOptions.start position and read exactly (end - start) bytes from the remote file.
3. THE SftpCopyFile download SHALL provide a reopen factory that, when invoked, re-reads the same byte range from the remote source and returns byte-identical content to the original read.
4. WHEN upload is called without multipart options, THE SftpCopyFile SHALL truncate the destination file and write the entire CopyContent stream, returning a CopyResult with bytes_transferred equal to the number of bytes written.
5. WHEN upload is called with multipart options where part_number is 1, THE SftpCopyFile SHALL truncate the destination file and write data starting at the specified offset.
6. WHEN upload is called with multipart options where part_number is greater than 1, THE SftpCopyFile SHALL open the destination file in append mode and write data at the specified offset without truncating existing content.
7. WHEN upload is called with multipart options where part_number is None, THE SftpCopyFile SHALL treat it as the completion step and write zero bytes, returning a CopyResult with bytes_transferred equal to 0.
8. WHEN copy is called with multipart options where part_number is Some, THE SftpCopyFile SHALL return a default empty CopyResult without performing any transfer.
9. WHEN copy is called without multipart options or with multipart options where part_number is None, THE SftpCopyFile SHALL perform a download-then-upload sequence to transfer the file content.
10. THE SftpCopyFile SHALL report max_part_size as u64::MAX, max_parts as u64::MAX, and min_part_size as u64::MIN.
11. WHEN initialize_state is called, THE SftpCopyFile SHALL stat the remote source file and return a CopyState with the file size as reported by the stat result.

### Requirement 5: SFTP ObjectSums Implementation

**User Story:** As a user, I want to generate and verify checksums for files on SFTP servers, so that I can ensure data integrity of remote files.

#### Acceptance Criteria

1. WHEN sums_file is called and a `.sums` file exists on the remote, THE SftpSumsFile SHALL read and parse the remote sums file into a SumsFile object.
2. WHEN sums_file is called and no `.sums` file exists on the remote, THE SftpSumsFile SHALL return None without error.
3. IF sums_file is called and the remote `.sums` file contains malformed JSON, THEN THE SftpSumsFile SHALL return an error indicating the sums file could not be parsed.
4. WHEN reader is called, THE SftpSumsFile SHALL return an AsyncRead stream of the target file content from the remote server.
5. IF reader is called and the target file does not exist on the remote, THEN THE SftpSumsFile SHALL return an IOError.
6. WHEN file_size is called and the remote file exists, THE SftpSumsFile SHALL return the file size in bytes as reported by stat.
7. WHEN file_size is called and the remote file does not exist, THE SftpSumsFile SHALL return None without error.
8. WHEN write_sums_file is called, THE SftpSumsFile SHALL serialize the SumsFile to JSON and write it to the remote `.sums` path, truncating any existing content.
9. THE SftpSumsFile location method SHALL format the path as an SFTP URL in the form `sftp://[user@]host[:port]/path` using the configured SftpEndpoint.

### Requirement 6: Builder Dispatch Integration

**User Story:** As a developer, I want the existing builder dispatch to route SFTP providers to the correct implementation, so that SFTP integrates seamlessly with the existing architecture.

#### Acceptance Criteria

1. WHEN ObjectCopyBuilder receives a Provider::Sftp source or destination, THE ObjectCopyBuilder SHALL construct an SftpCopyFile using the SftpClient configured on the builder, applying the source and destination paths extracted from the Provider::Sftp variant.
2. WHEN ObjectSumsBuilder receives a URL with the `sftp://` scheme, THE ObjectSumsBuilder SHALL parse the URL into a Provider::Sftp variant and construct an SftpSumsFile using the SftpClient configured on the builder.
3. IF an SFTP provider is encountered but no SftpClient was configured on the builder, THEN THE ObjectCopyBuilder SHALL return a CopyError indicating that an SFTP client is required.
4. IF an SFTP provider is encountered but no SftpClient was configured on the builder, THEN THE ObjectSumsBuilder SHALL return a ParseError indicating that an SFTP client is required.
5. WHEN ObjectCopyBuilder receives a mixed-provider pair where one side is Provider::Sftp and the other is Provider::S3 or Provider::File, THE ObjectCopyBuilder SHALL construct the appropriate implementation for each side independently, using SftpCopyFile for the SFTP side and the existing S3 or File implementation for the other side.
6. THE ObjectCopyBuilder SHALL handle all Provider variants (File, S3, Sftp) via exhaustive matching without any panic or unreachable paths for recognized variants.
7. THE ObjectSumsBuilder SHALL handle all Provider variants (File, S3, Sftp) via exhaustive matching without any panic or unreachable paths for recognized variants.

### Requirement 7: CLI Integration

**User Story:** As a user, I want to pass SFTP authentication options via CLI flags or environment variables, so that I can configure SFTP access without modifying code.

#### Acceptance Criteria

1. THE CLI SHALL accept a global `--sftp-key` flag (or `COPYRITE_SFTP_KEY` env var) to specify an SSH private key file path, where the CLI flag takes precedence over the env var when both are provided.
2. THE CLI SHALL accept a global `--sftp-passphrase` flag (or `COPYRITE_SFTP_PASSPHRASE` env var) to specify an SSH key passphrase, where the CLI flag takes precedence over the env var when both are provided.
3. THE CLI SHALL accept a global `--sftp-password` flag (or `COPYRITE_SFTP_PASSWORD` env var) to specify an SSH password, where the CLI flag takes precedence over the env var when both are provided.
4. WHEN SFTP URLs are provided as source or destination arguments to `generate`, `copy`, or `check` subcommands, THE CLI SHALL construct an SftpClient using the provided SFTP authentication flags and pass it to the ObjectCopyBuilder or ObjectSumsBuilder for that operation.
5. IF `--sftp-key` is provided and the specified file path does not exist or is unreadable, THEN THE CLI SHALL return an error indicating the private key file could not be accessed.
6. IF SFTP URLs are provided but no authentication method can be determined (no key and no password configured), THEN THE CLI SHALL return an error indicating that an SFTP authentication method must be specified via `--sftp-key` or `--sftp-password`.

### Requirement 8: Feature Gating

**User Story:** As a developer, I want SFTP support behind a cargo feature flag, so that users who do not need SFTP can avoid the additional SSH dependency overhead.

#### Acceptance Criteria

1. THE Cargo.toml SHALL define an `sftp` feature that enables the russh, russh-sftp, russh-keys, and whoami dependencies as optional.
2. THE `sftp` feature SHALL NOT be included in the default feature set.
3. IF the `sftp` feature is not enabled, THEN THE crate SHALL compile successfully without any SFTP-related modules, Provider::Sftp variant, CLI flags (`--sftp-key`, `--sftp-passphrase`, `--sftp-password`), or SFTP dependencies.
4. IF the `sftp` feature is enabled, THEN THE Provider enum SHALL include the Sftp variant and the ObjectCopy and ObjectSums trait implementations for SFTP SHALL be available.
5. IF the `sftp` feature is not enabled and a user provides a URL with the `sftp://` scheme, THEN THE Provider parser SHALL return a ParseError indicating that SFTP support requires enabling the `sftp` feature.

### Requirement 9: Error Handling

**User Story:** As a user, I want clear and descriptive error messages when SFTP operations fail, so that I can diagnose and resolve connection or transfer issues.

#### Acceptance Criteria

1. IF DNS resolution or TCP connection fails, THEN THE SftpClient SHALL return a CopyError containing "SSH connection failed" and the underlying error details.
2. IF a remote file is not found during stat or read, THEN THE SftpClient SHALL return an IOError with ErrorKind::NotFound and include the remote file path in the error message.
3. IF a remote permission error occurs during a file operation (stat, read, or write), THEN THE SftpClient SHALL return an IOError with ErrorKind::PermissionDenied and include the remote file path in the error message.
4. IF the SFTP subsystem request fails, THEN THE SftpClient SHALL return a CopyError containing "SFTP session init failed" and the underlying error details.
5. WHEN a multipart download fails mid-transfer, THE CopyContent reopen factory SHALL return a new CopyContent that re-reads from the same byte range of the source file, starting a fresh SFTP read from the original offset.
6. IF a remote permission error occurs during SSH authentication, THEN THE SftpClient SHALL return a CopyError indicating that authentication was rejected by the server.
