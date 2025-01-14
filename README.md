# checksum-cloud
A CLI tool for computing checksums across multiple cloud object stores

## Usage

Run the help command:

```
cargo run -p checksum-cloud -- --help
```

Generate checksums for an input file:
```
cargo run -p checksum-cloud -- --checksum md5,sha1,sha256 <INPUT_FILE>
```