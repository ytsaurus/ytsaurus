# Base artifacts for ytsaurus images

This folder contains package fragments that are meant to be included from ytsaurus and query-tracker packages.
Package fragments are split into files: {section}-{group}[-{variant}].json

Section is package json sections like "build" or "data".

Each group contains common parts like:

### common
- Dockerfile
- certificates

### server
- ytserver-all
- ytserver-all credits
- init scripts

### python
- python binaries

Variant reflects target architecture or combination of any other options.
