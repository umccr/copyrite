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
copyrite --help
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

[sums]: docs/ARCHITECTURE.md#the-sums-file
