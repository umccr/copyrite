# cloud-checksum
A CLI tool for computing checksums across multiple cloud object stores

## Usage

Run the help command:

```
cargo run -p cloud-checksum -- --help
```

Generate checksums for an input file:

```
cargo run -p cloud-checksum -- generate --checksum md5,sha1,sha256 <INPUT_FILE>
```

AWS style etags are supported, with either a `-<part_size>` suffix or `-<part_number>` suffix.
For example, `-8` represents splitting the checksum into 8 parts, where as `-8mib` represents
splitting the checksum into 8mib chunks.

```
cargo run -p cloud-checksum -- generate --checksum md5-aws-8,md5-aws-8mib <INPUT_FILE>
```

To see if files are identical, use the check command:

```
cargo run -p cloud-checksum -- check <INPUT_FILE> <INPUT_FILE>
```

Objects on S3 are also supported by using the `s3://bucket/key` syntax:

```
cargo run -p cloud-checksum -- generate --checksum md5-aws-8,md5-aws-8mib s3://bucket/key
cargo run -p cloud-checksum -- check s3://bucket/key1 s3://bucket/key2
```

Copy files, this supports S3 and local files for source and destination:

```sh
# Server-side copy in S3.
cargo run -p cloud-checksum -- copy s3://bucket/key1 s3://bucket/key2
# Local to local
cargo run -p cloud-checksum -- copy local_file1 local_file2

# S3 to local
cargo run -p cloud-checksum -- copy s3://bucket/key1 local_file
# Local to S3
cargo run -p cloud-checksum -- copy local_file s3://bucket/key1
```

## Design

This tool aims to be as efficient and performant as possible when calculating checksums. This means that it only
reads the data once, and simultaneously calculates all the checksums as it reads through the data. On S3, it always
uses metadata fields like ETags and additional checksums to obtain data without reading the file if it is able to.

This tool requires generating `.sums` files to allow checking it. This means that a `generate` command should always be
performed before a `check`. To avoid specifying checksums, use `--missing` on the `generate` command to generate only
the needed checksums to perform a `check`.
