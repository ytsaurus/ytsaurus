# Generated by devtools/yamaker from nixpkgs 24.05.

LIBRARY()

LICENSE("(BSD-2-Clause OR GPL-2.0-or-later)")

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(4.33)

ORIGINAL_SOURCE(http://dist.schmorp.de/libev/Attic/libev-4.33.tar.gz)

ADDINCL(
    contrib/libs/libev
)

NO_COMPILER_WARNINGS()

NO_RUNTIME()

CFLAGS(
    -DHAVE_CONFIG_H
)

SRCS(
    ev.c
)

END()
