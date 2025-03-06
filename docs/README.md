## `generate` Invokes (WIP)

`cc generate <local-path>`

Compute a default set of checksums for the local file
and output a single line to stdout of them. What is
the default set??

`cc generate --checksum md5,sha1,sha256 <local-path>`

`--checksum` is flag to control the set of checksums that are generated
beyond/instead of the default set.

`cc generate --partial-timeout <seconds> <local-path>`

Spend <seconds> generating checksums for the given file
and then output a (invalid) sums output that is a partial
representation of the checksums as being calculated.



## `copy` Invokes (WIP)

`cc copy <s3://src> <s3://dest>`

Server side clone of object so that destination object is
identical to source (ETag and checksums).

`cc copy <s3://src> <local-path>`

Copy object locally - preferrably using concurrency to
download at speed (not something that is normally done in most s3 tools).

`cc copy <local-path> <s3://dest>`

Multi-part copy object up to cloud. Use concurrency for the
parts (standard feature of most s3 tools). How to control
the part sizes? If there is an accompanying sums
file or passed in sums data - should choose
a part size that matches checksums.

`cc copy --tag-mode suppress|best-effort|copy <s3:src> <s3:dest>`

Default to ??. Control copying of tags on the source object
to the dest object.
suppress - don't copy them no matter what
best-effort - copy them but don't abort if you can't
copy - copy them and if they can't due to permisisons, fail



## Reading

Random resources from the internet regarding checksums

- https://cloud.google.com/blog/products/storage-data-transfer/new-file-checksum-feature-lets-you-validate-data-transfers-between-hdfs-and-cloud-storage
- https://developers.cloudflare.com/r2/objects/multipart-objects
- https://galdin.dev/blog/md5-has-checks-on-azure-blob-storage-files
- https://stackoverflow.com/questions/66364156/how-to-upload-multiple-chunks-of-a-single-video-file-in-azure-blob-storage-using
- https://stackoverflow.com/questions/42229153/how-to-check-azure-storage-blob-file-uploaded-correctly
- https://cloud.google.com/storage/docs/data-validation
- https://users.ece.cmu.edu/~koopman/crc/index.html
- https://en.wikipedia.org/wiki/Rolling_hash


## Information to be placed elsewhere

| Tool                   | Threshold Config Name                                                                                                                                              | Threshold | Part Size Config Name       | Part Size              |
|------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|-----------------------------|------------------------|
| AWS S3 CLI v1          | `multipart_threshold` (in s3 config file)                                                                                                                          | 8MiB      | `multipart_chunksize`                            | 8MiB                   |
| AWS S3 CLI v2          | `multipart_threshold` (in s3 config file)                                                                                                                          | 8MiB      | `multipart_chunksize`                            | 8MiB                   |
| AWS S3 Console Upload* | Not configurable                                                                                                                                                   | 16MiB     | Not configurable            | 16MiB
| `rclone`               | `--s3-upload-cutoff`                                                                                                                                               | 200MiB    | `--s3-chunk-size`           | 5MiB                   |
| `s3p`                  | Uses aws-cli under the hood for large files so maybe irrelevant for this table??                                                                                   |           |                             |
| `s3cmd`                | "Multipart uploads are automatically used when a file to upload is larger than 15MB." (no setting)                                                                 | 15 MiB    | `--multipart-chunk-size-mb` | 15 (confirm MiB or MB) |
| `s3kor`                | "To cater for large objects and to limit memory usage this is done utilizing multi parts of 5MB in size and will attempt to limit in memory storage." (no setting) |           |                             | 5 MiB                  |
| AWS Thaw from Glacier  |                                                                                                                                                                    |           |                             |                        |


* Tested manually in Firefox. The maximum file uploadable via the Console is 160 GiB (as stated in the UI) - which makes sense
as a non-configurable part size of 16 MiB * 10,000 parts = 160 GiB
