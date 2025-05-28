# Architecture 
The components of `copyrite` are intended to be as efficient as possible, making use of parallelism for copying operations
and for generate multiple checksums simultaneously.

The project is split into three main components, generating checksums, verifying identical files, and copying files.
When copying a file, `copyrite` acts like a sync operation, where it first checks to see if a file exists, and only copies
the file if does not exist or has mismatched checksums.

## Generating checksums

To do this, it has to be able to compute a variety of checksums that are supported across different object stores. It
supports MD5, SHA varieties, CRC varieties and AWS ETags (with plans for more in the future). It is able to compute
multiple checksums in parallel with one read pass over the data. This is useful when copying and verifying objects 
across stores that have different native checksums. `copyrite` is able to select the best checksum depending on the
kind of copy, and it also avoids unnecessarily computing checksums if they are already available natively. 

### The `.sums` file

One way that `copyrite` avoids doing unnecessary work is by saving results in a JSON file with a `.sums` prefix. When
verifying checksums, this file can be used to avoid recomputing checksums that have already been computed. The structure
of the file shows various checksums that can be computed:

```json
{
  "version": "1",
  "size": 10485760,
  "md5-aws-5242880b": "ec1e29805585d04a93eb8cf464b68c43-5242880b",
  "crc64nvme": "c8cfc4c0cc45c6c1",
  "md5": "617808065bb1a8be2755f9be0c0ac769"
}
```

Checksums are listed as fields in the JSON object. Any new checksums that are computed are merged with the existing file
unless configured otherwise.

## Verifying objects

The CLI also verifies object integrity and shows the matching checksum which proves that the files are identical. The aim
of the tool is to squeeze as much information as possible from native sources like AWS S3 HeadObject about various checksums.

Files can be verified for equality, or for comparability. A compatibility check shows if the files could be checked for 
equality with the currently available checksums without computing any additional checksums. This is useful to know which
checksums need to be computed to confirm that a set of files are identical. A minimal set of missing checksums can be
computed using the CLI flags.

## Copying objects 

When copying objects, `copyrite` will determine the best possible settings to copy with to avoid unnecessary computation
of additional checksums. It supports arbitrary file sizes (as supported by the object store) and intelligently determines
the best settings to use when copying multiple parts.

For example, it will automatically match the part sizes of source files and duplicate them to the destination, to avoid
re-calculating ETags. It will also utilize server-side copying if possible, for example by using `CopyObject` instead
of downloading the object and uploading it again. 

Once the file is copied, `copyrite` will verify that the destination is the same as the source. It will also output detailed
statistics in JSON which include the time spent, checksums generated, files checked, matching checksums, any API errors and
the reason why a copy check was successful. There is also a more user-friendly `ui` mode if computer-readable outputs are
not required.
