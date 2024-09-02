GO_TEST_FOR(vendor/github.com/fsnotify/fsnotify)

LICENSE(BSD-3-Clause)

# FIXME: It doesn't look like a good solution, but the tests execute 'mv' command.

ENV(PATH=/bin:/usr/bin)

SIZE(MEDIUM)

END()
