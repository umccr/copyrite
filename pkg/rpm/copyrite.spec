%{!?version: %global version 0.7.0}

Name:           copyrite
Version:        %{version}
Release:        1%{?dist}
Summary:        CLI tool for efficient checksum and copy operations across object stores

License:        MIT
URL:            https://github.com/umccr/copyrite

%global debug_package %{nil}

%description
A CLI tool for efficient checksum and copy operations across object stores.

%build
cargo build --release

%install
install -D target/release/%{name} %{buildroot}%{_bindir}/%{name}

%files
%{_bindir}/%{name}

%changelog
* Wed Jul 22 2026 Marko Malenic <mmalenic1@gmail.com> - 0.7.0-1
- Add new checksums to tests and properly run md5 through parts (@mmalenic)
- Add md5 sum support and also fix encryption-based etag support (@mmalenic)
- Add checksum tests for md5, xxhash, sha512 (@mmalenic)
- Add xxhash variants, md5 and sha512 (@mmalenic)
- Add pkg changelogs (@mmalenic)
- Pairing clients with their source/destination location properly (@mmalenic)
- Update CHANGELOG to remove fixed entries (@andrewpatto)
- Fix clippy (@andrewpatto)
- Fix fmt (@andrewpatto)
- Introduced an option that disables request checksums (which had been enabled automatically in the AWS SDK) (@andrewpatto)
- Add release for 0.5.1, and use glibc 2.34 for greater compatibility (@mmalenic)
- Improve error print output (@mmalenic)
- Fmt (@mmalenic)
- Max object size should be 50 TiB not 5 TiB (@mmalenic)
- Add S3 tests for max size and single part check (@mmalenic)
- Add file based tests for append/truncate (@mmalenic)
- Avoid unnecessary clone on object info (@mmalenic)
- Append on existing data for file copy (@mmalenic)
- Correctly run upload tasks concurrently, single-part should be inclusive (@mmalenic)
- Add max object size setting (@mmalenic)
- Guard against invalid single-part copy attempts, and change < to <= in multipart detection (@mmalenic)
- Add test cases targeting retries and re-open (@mmalenic)
- Add bytes dependency (@mmalenic)
- Add re-open read for aws based operations (@mmalenic)
- Add re-open read for file based operations (@mmalenic)
- Make the CopyContent re-open function required (@mmalenic)
- Add streaming to multipart as well (@mmalenic)
- Apply reopen logic to avoid holding memory while using put object (@mmalenic)
- Add with_reopen to copy when using best effort copying (@mmalenic)
- Add re-open handle to allow re-fetching the source when required (@mmalenic)

* Tue May 12 2026 Marko Malenic <mmalenic1@gmail.com> - 0.4.0-1
- Format pkg release (@mmalenic)
- Add overrides for other S3 operations (@mmalenic)
- Use wrapper functions when calling S3 (@mmalenic)
- Add wrapper calls to s3 client with overrides (@mmalenic)
- Add SSP override options (@mmalenic)
- Docker.yml needs to use the same tag logic as release-bins.yml (@mmalenic)
- Check if assets are found on the release before attempting the workflow, as release-plz completes before and after publishing (@mmalenic)

* Fri Apr 10 2026 Marko Malenic <mmalenic1@gmail.com> - 0.3.2-1
- Rename variable (@mmalenic)
- Adjust workflow to trigger on workflow_run (@mmalenic)

* Fri Apr 10 2026 Marko Malenic <mmalenic1@gmail.com> - 0.3.1-1
- Also update changelog for pkg files (@mmalenic)
- Use types published and avoid creating a separate release (@mmalenic)
- Use trusted publishing (@mmalenic)

* Sun May 11 2025 Marko Malenic <mmalenic1@gmail.com> - 0.2.5-1
- Add option to avoid `GetObjectAttributes` (@mmalenic)
- Fix determining copy mode settings (@mmalenic)
- Fix mocked AWS calls (@mmalenic)
- Allow setting clients per object for check/generate (@mmalenic)
- Make GetObjectAttributes failure recoverable and return in api errors (@mmalenic)
- Further improve AWS error message context (@mmalenic)
- Add more error context (@mmalenic)
- Implement source/destination credentials, endpoint urls, profiles and regions (@mmalenic)
- Trying client creation (@andrewpatto)
- Added some extra ignores (@andrewpatto)

* Fri May 02 2025 Marko Malenic <mmalenic1@gmail.com> - 0.2.4-1
- Format errors as json (@mmalenic)

* Fri May 02 2025 Marko Malenic <mmalenic1@gmail.com> - 0.2.3-1
- Don't write sums by default, add `--write-sums-file` option and `--missing` option to check (@mmalenic)

* Wed Apr 30 2025 Marko Malenic <mmalenic1@gmail.com> - 0.2.2-1
- Add sums mismatch option for stats (@mmalenic)
- Add descriptions of fields generated in stats (@mmalenic)
- Add skipped option to copy to avoid copying unnecessarily (@mmalenic)
- Add copy command stats (@mmalenic)
- Add generate stats output (@mmalenic)
- Adjust reason field in compared values (@mmalenic)
- Check command updated stats (@mmalenic)
- Adjust default part size when left unspecified for generate (@mmalenic)
- Implement check command stats (@mmalenic)
- Create structs for output stats (@mmalenic)
- Move free-standing cli functions to structs (@mmalenic)

* Thu Apr 17 2025 Marko Malenic <mmalenic1@gmail.com> - 0.2.1-1
- Parsing part size off multipart sum manually and adding crc64nvme to all calls (@mmalenic)
- Add nvme crc64 (@mmalenic)

* Thu Apr 17 2025 Marko Malenic <mmalenic1@gmail.com> - 0.2.0-1
- Macro in part size position to avoid repetition (@mmalenic)
- Use array with constant byte sizes (@mmalenic)
- Change multipart threshold (@mmalenic)
- Is_copy and is_best_effort to avoid confusion (@mmalenic)
- Initialize s3 client only once (@mmalenic)
- Remove mutexes and add integration tests (@mmalenic)
- Copy with check and generate output (@mmalenic)
- Add additional checksums to copied files (@mmalenic)
- Add check/generate to copy command to confirm copy (@mmalenic)
- Add concurrency to copies (@mmalenic)
- Implement multipart copies in loop (@mmalenic)
- Add tests for multipart/single part settings detection (@mmalenic)
- Add preferred checksum part size detection (@mmalenic)
- Add multipart uploads (@mmalenic)
- Add copy_object in parts functionality (@mmalenic)
- Add ordering of ideal part sizes (@mmalenic)

* Tue Apr 15 2025 Marko Malenic <mmalenic1@gmail.com> - 0.1.0-1
- 'tar: none' has the side effect of 'upload-rust-binary-action/v1/main.sh: line 493: assets[@]: unbound variable'... we'll have to live with tarballs per platform for now (@brainstorm)
- Rename release-bins workflow accordingly (instead of tests) (@brainstorm)
- Target is arch target, not bin target (@brainstorm)
- Re-enable tests workflow pre-pr-merge (@brainstorm)
- Removing cargo-bloat since it failed unexpectedly, should be moved to a separate 'audit' workflow anyway (@brainstorm)
- Separate workflows by concern, remove release-plz one (for now, until a name for the project is decided?) (@brainstorm)
- No changelog yet (@brainstorm)
- Skip tests for now, do any of the taiki-e actions 'cargo build'? (@brainstorm)
- Bump mozilla actions sccache (see https://github.blog/changelog/2025-03-20-notification-of-upcoming-breaking-changes-in-github-actions/#decommissioned-cache-service-brownouts) (@brainstorm)
- No release-plz for Andrew :) (@brainstorm)
- First cut at releasing binaries (and crate) as suggested in https://github.com/umccr/cloud-checksum/issues/29 by @andrewpatto (@brainstorm)
- Retry logic for access denied errors on tagging (@mmalenic)
- Add separate option for tag mode vs metadata mode (@mmalenic)
- Add option to force download-upload mode (@mmalenic)
- Copy operations and tagging (@mmalenic)
- Add metadata copy mode (@mmalenic)
- Remove ordering of ctx changes (@mmalenic)
- Implement single-part copy (@mmalenic)
- Tidy trait seperation using copy and sums (@mmalenic)
- Move url parsing to provider (@mmalenic)
- Add single-part copies (@mmalenic)
- Decide on the "best" ordering for aws and standard checksums (@mmalenic)
- Separate reader/write in IO (@mmalenic)
- Copy command options (@mmalenic)
- Simplify .sums file to remove unnecessary part checksums (@mmalenic)
- Re-work canonical form of aws etag to include byte size (@mmalenic)
- Allow specifying different part sizes for aws checksums (@mmalenic)
- Aws decode additional checksums and use generate for tests (@mmalenic)
- Simplify version string to a single number (@mmalenic)
- Use head object for calculating parts for etags (@mmalenic)
- Re-work part checksums so that the part size is nested inside a part checksum to support arbitrary part sizes (@mmalenic)
- Add method to add AWS checksums and consider the full/composite checksum type in the SDK (@mmalenic)
- Add checksums from object attributes (@mmalenic)
- Fetch existing sums files from S3 (@mmalenic)
- Add s3 url parsing logic (@mmalenic)
- Remove cloud module and add to reader instead (@mmalenic)
- Construct sums ctx using function and fix aws endianness (@mmalenic)
- Enable generate and check logic for metadata-only sums (@mmalenic)
- Merge metadata checksums with existing sums on S3 side (@mmalenic)
- Link up S3 sums logic with the rest of the code (@mmalenic)
- Add into async read for the S3 reader (@mmalenic)
- Avoid using serde_with for now (@mmalenic)
- Skip empty optionals (@mmalenic)
- Random thoughts (@andrewpatto)
- Use single-line non-pretty json formatting (@mmalenic)
- Aws etag syntax and logic (@mmalenic)
- Crc32 and crc32-le should hash differently (@mmalenic)
- Allow commands to work with empty or missing sums files (@mmalenic)
- Add check output struct (@mmalenic)
- Part number should be the canonical form for aws-style checksums (@mmalenic)
- Add equality and comparability tests (@mmalenic)
- Add generate missing option (@mmalenic)
- Add generate missing arg and allow specifying multiple input files for generate (@mmalenic)
- Add update from check option (@mmalenic)
- Add set-based check command (@mmalenic)
- Implement check command from .sums files (@mmalenic)
- Add part-number style etag parsing (@mmalenic)
- Separate regular and AWS checksums (@mmalenic)
- Add tests for aws checksums (@mmalenic)
- Link up CLI options with aws etag checksummer algorithm (@mmalenic)
- Add part checksummer (@mmalenic)
- Add checksum algorithm with part size struct (@mmalenic)
- Add tests for verify, overwrite, and no-verify modes (@mmalenic)
- Add verify flag, use "-" instead of "--stdin", add version and remove name from output file, overwrite when merging (@mmalenic)
- Use kebab-case because that's what checksum names will be based on (@mmalenic)
- Add ability to append to existing checksums file and option to force overwrite (@mmalenic)
- Rework input to use position arg as default and pass --stdin for stdin (@mmalenic)
- Add output file and write checksums instead of printing (@mmalenic)
- Add single-part aws-etag CLI parsing and calculation (@mmalenic)
- Add definition for output file (@mmalenic)
- Add -le and -be extension to enum variants on CLI (@mmalenic)
- Add crc32c checksum (@mmalenic)
- Add crc32 checksum (@mmalenic)
- Generate test file with mutex guard so tests can run in parallel (@mmalenic)
- Add test action (@mmalenic)
- Use multiple mpsc channels to fix slow receiver (@mmalenic)
- Add some unit tests and update test file generator (@mmalenic)
- Add test file generating module (@mmalenic)
- Add chunk size configuration to reader (@mmalenic)
- Use async_channel, as it seems to perform better and have a simpler unbounded implementation (@mmalenic)
- Refine benchmarks and avoid lagging receiver (@mmalenic)
- Add benchmarks for channel reader (@mmalenic)
- Create task executor, confine channels to channel reader, and perform checksum task in checksum enum (@mmalenic)
- Add reader structs and refine task types (@mmalenic)
- Refine check command arguments (@mmalenic)
- Add some subcommands and rename package (@mmalenic)
- Add explaining comments (@mmalenic)
- Add basic outline of concurrent checksums (@mmalenic)
- Add basic skeleton with argument parsing (@mmalenic)
- Initial commit (@andrewpatto)


