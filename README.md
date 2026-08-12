# copyrite

A CLI tool for efficient checksum and copy operations across object stores.

copyrite can calculate checksums on files in object stores and copy them to other locations.

The aims of copyrite are to:
* Calculate multiple checksums in parallel as efficiently as possible.
  * Multiple checksums are supported which can be computed by reading through the data once.
* Copy files across object stores with verifiable integrity.
  * Files are copied concurrently, and verified with part sizes that are as optimal as possible.
* Avoid doing unnecessary work.
  * If checksums exists natively in object stores, they are not computed again.
  * Checksums are saved and propagated through a [`.sums` file][sums] for future operations on the same file.
* Have a wide range of configurable options for copying tags, metadata, or part sizes.
* Output detailed statistics on operations that can be consumed by other processes.

## Usage

Run the help command:

```
copyrite -h
```

Generate checksums for an input file:

```
copyrite generate --checksum md5,sha1,sha256 <INPUT_FILE>
```

AWS style etags are supported, with either a `-<part_size>` suffix or `-<part_number>` suffix.
For example, `-8` represents splitting the checksum into 8 parts, where as `-8mib` represents
splitting the checksum into 8mib chunks.

```
copyrite generate --checksum md5-aws-8,md5-aws-8mib <INPUT_FILE>
```

To see if files are identical, use the check command:

```
copyrite check <INPUT_FILE_1> <INPUT_FILE_2>
```

Objects on S3 are also supported by using the `s3://bucket/key` syntax:

```
copyrite generate --checksum md5-aws-8,md5-aws-8mib s3://bucket/key
copyrite check s3://bucket/key1 s3://bucket/key2
```

Copy files, this supports S3 and local files for source and destination:

```sh
# Server-side copy in S3.
copyrite copy s3://bucket/key1 s3://bucket/key2
# Local to local
copyrite copy local_file1 local_file2

# S3 to local
copyrite copy s3://bucket/key1 local_file
# Local to S3
copyrite copy local_file s3://bucket/key1
```

The credentials used for the source and destination side are customizable through different providers and environment
variables. See the "Credentials" section in the long help:

```sh
copyrite --help
```

## S3-compatible endpoints

copyrite targets the official S3 API. S3-compatible implementations vary in how much of the
checksum API they support, so a set of compatibility options is available to turn off the parts
an endpoint does not implement. `--s3-compatible` enables all of them at once, and each can also
be set individually, or per-side on `copy` with a `--source-`/`--destination-` prefix. See the
long help (`copyrite --help`) for the full list.

| Option                        | Turns off                                                          |
|-------------------------------|--------------------------------------------------------------------|
| `--force-path-style`          | Virtual-hosted-style addressing.                                   |
| `--no-get-object-attributes`  | `GetObjectAttributes`, falling back to per-part `HeadObject`.       |
| `--no-checksum-mode`          | `ChecksumMode::Enabled` on `HeadObject`, so only `ETag`s are used.  |
| `--no-request-checksum`       | SDK-computed upload checksums, including trailers.                  |
| `--no-precalculated-checksum` | Precalculated `x-amz-checksum-*` values on uploads.                 |

### Ceph RADOS Gateway

Checksum support in RGW changed substantially across releases, so the flags needed depend on the
version. All of these still require `--force-path-style`.

| Release                  | Checksum behaviour                                                     | Recommended flags            |
|--------------------------|------------------------------------------------------------------------|------------------------------|
| Tentacle 20.2.0+         | Full support, including `GetObjectAttributes` and checksum types.      | `--force-path-style`         |
| Squid 19.2.x, Reef 18.2.5+ | Uploads with checksums succeed, but the values are discarded.        | `--s3-compatible`            |
| Reef ≤ 18.2.4, Quincy    | Checksum trailers are rejected with `XAmzContentSHA256Mismatch`.        | `--s3-compatible`            |

Tentacle supports `crc32`, `crc32c`, `xxh3`, `sha1`, `sha256`, `sha512`, `blake3` and
`crc64nvme`, but **not** `md5`, `xxh64` or `xxh128`. Requesting an unsupported algorithm from a
sums file results in an upload without that checksum rather than a hard failure.

On Squid and Reef 18.2.5+, checksum values sent on upload are neither verified, stored, nor
returned, so `x-amz-checksum-*` gives no end-to-end integrity guarantee. copyrite still verifies
these copies, but does so by comparing `ETag`s, which for a multipart upload requires the part
sizes to match on both sides.

Other version-specific hazards worth knowing about:

* Object tags on multipart objects cannot be read back before Squid 19.2.4, so `--tag-mode copy`
  is unreliable on earlier releases. Use `--tag-mode best-effort` or `--tag-mode suppress` there.
* Copying an object onto itself can lose data before Squid 19.2.3 and Reef 18.2.8. copyrite
  refuses same-location copies, so `s3://x/y → s3://x/y` is not affected.
* `CopyObject` on server-side encrypted objects needs Tentacle 20.2.3.
* `ETag`s are returned unquoted before Reef 18.2.8.
* `x-amz-mp-object-size` is ignored by all RGW releases.
* An `AbortIncompleteMultipartUpload` lifecycle rule is recommended on the destination bucket, so
  that parts left behind by an interrupted copy are cleaned up.

## Memory use

Copies stream, so memory use does not scale with the object size, with one exception. Multipart
uploads of `md5`, `sha512`, `xxh64`, `xxh3` and `xxh128` cannot have their checksum computed by
the AWS SDK while streaming, so each part is buffered in memory to compute its checksum before
being sent. Peak usage for those copies is roughly `--concurrency` multiplied by the part size.
Lower either option if a copy runs out of memory, or pick a checksum the SDK can stream
(`crc32`, `crc32c`, `crc64nvme`, `sha1`, `sha256`), which uses constant memory.

## Design

This tool aims to be as efficient and performant as possible when calculating checksums. This means that it only
reads the data once, and simultaneously calculates desired sets of checksums as it reads through the data. On S3, it always
uses metadata fields like ETags and additional checksums to obtain data without reading the file if it is able to.

This tool requires generating `.sums` files to allow checking it. This means that a `generate` command should always be
performed before a `check`. To avoid specifying checksums, use `--missing` on the `generate` command to generate only
the needed checksums to perform a `check`.

## Tests

Run unit tests using:

```sh
cargo test --all-features
```

Run bench marks using:

```sh
cargo bench --all-features
```

Integration tests are ignored by default. They perform operations on an S3 bucket directly, and need to have a
`COPYRITE_TEST_BUCKET_URI` environment set, to a bucket and prefix that files can be written to. Run the tests
using:

```sh
COPYRITE_TEST_BUCKET_URI="s3://bucket/prefix" cargo test --all-features -- --ignored
```

The endpoint URL can also be set for S3-compatible endpoint tests:

```sh
COPYRITE_TEST_BUCKET_URI="s3://bucket/prefix" COPYRITE_TEST_ENDPOINT_URL="https://storage.googleapis.com" cargo test --all-features -- --ignored
```

This can also source credentials from a secret, and set compatibility options. The full test environment variables are:

| Variable                      | Description                                                                                                  |
|-------------------------------|--------------------------------------------------------------------------------------------------------------|
| `COPYRITE_TEST_BUCKET_URI`    | The S3 bucket and prefix to use.                                                                             |
| `COPYRITE_TEST_ENDPOINT_URL`  | The S3 endpoint URL.                                                                                         |
| `COPYRITE_TEST_SECRET`        | The AWS Secrets Manager secret name or ARN, this will also set the credential provider type to `aws-secret`. |
| `COPYRITE_TEST_REGION`        | The AWS region.                                                                                              |
| `COPYRITE_TEST_S3_COMPATIBLE` | Set to `true` to enable S3-compatibility.                                                                    |

[sums]: docs/ARCHITECTURE.md#the-sums-file
