# Contributing to Aero Arc Agent

Thank you for contributing to Aero Arc. Keep pull requests focused, add tests
for behavior changes, and discuss substantial architectural changes in an issue
before implementation.

## Development

Create a branch from `master`, then run the project checks before opening a
pull request:

```sh
go test ./...
```

External pull request workflows require maintainer approval before they run.
Do not include secrets or credentials in code, tests, logs, or workflow files.

## Developer Certificate of Origin

Every commit must include a `Signed-off-by` trailer certifying the
[Developer Certificate of Origin](https://developercertificate.org/).

Create signed-off commits with:

```sh
git commit -s
```

The name and email in the trailer must identify the contributor. Pull requests
cannot merge until every commit passes the repository's DCO check.

## Pull Requests

Explain what changed, why it changed, and how it was tested. Address review
feedback constructively and ensure all required checks pass before merge.
