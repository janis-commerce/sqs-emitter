# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### [1.2.0] - 2026-10-27
### Added
- **Logging** Failed events of `publishEvents()` are now logged by default, without the message content. Set `SQS_EMITTER_DISABLE_FAILED_LOG=true` to disable it (https://janiscommerce.atlassian.net/browse/JPS-471)

### Fixed
- Functions publishing on every invocation no longer run out of connections (`EMFILE` errors) (https://janiscommerce.atlassian.net/browse/JPS-471)
- `publishEvent()` now rejects with an `S3_ERROR` when a large payload can't be uploaded, and the 256KB limit is measured in bytes, so content with accented characters no longer exceeds it (https://janiscommerce.atlassian.net/browse/JPS-471)

### [1.1.0] - 2025-06-09
### Added
- **AWS X-Ray** support

### [1.0.0] - 2025-06-01
### Added
- SQS emitter for publishing events to AWS SQS queues with support for message attributes and batch processing
