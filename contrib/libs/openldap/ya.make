# Generated by devtools/yamaker from nixpkgs 24.05.

LIBRARY()

LICENSE(
    BSD-3-Clause AND
    BSD-4-Clause-UC AND
    Bsd-Original-Uc-1986 AND
    Custom-openldap AND
    MIT AND
    OLDAP-2.8 AND
    Unicode-Mappings
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(2.6.10)

ORIGINAL_SOURCE(https://gitlab.com/api/v4/projects/openldap%2Fopenldap/repository/archive.tar.gz?sha=OPENLDAP_REL_ENG_2_6_10)

PEERDIR(
    contrib/libs/openldap/libraries/liblber
    contrib/libs/openssl
    contrib/libs/sasl
)

ADDINCL(
    GLOBAL contrib/libs/openldap/include
    contrib/libs/sasl/include
)

NO_COMPILER_WARNINGS()

NO_RUNTIME()

CFLAGS(
    -DLDAPI_SOCK=\"/run/openldap/ldapi\"
    -DLDAP_LIBRARY
)

SRCS(
    libraries/libldap/abandon.c
    libraries/libldap/account_usability.c
    libraries/libldap/add.c
    libraries/libldap/addentry.c
    libraries/libldap/assertion.c
    libraries/libldap/avl.c
    libraries/libldap/bind.c
    libraries/libldap/cancel.c
    libraries/libldap/charray.c
    libraries/libldap/compare.c
    libraries/libldap/controls.c
    libraries/libldap/cyrus.c
    libraries/libldap/dds.c
    libraries/libldap/delete.c
    libraries/libldap/deref.c
    libraries/libldap/dnssrv.c
    libraries/libldap/error.c
    libraries/libldap/extended.c
    libraries/libldap/fetch.c
    libraries/libldap/filter.c
    libraries/libldap/free.c
    libraries/libldap/getattr.c
    libraries/libldap/getdn.c
    libraries/libldap/getentry.c
    libraries/libldap/getvalues.c
    libraries/libldap/init.c
    libraries/libldap/lbase64.c
    libraries/libldap/ldap_sync.c
    libraries/libldap/ldif.c
    libraries/libldap/ldifutil.c
    libraries/libldap/messages.c
    libraries/libldap/modify.c
    libraries/libldap/modrdn.c
    libraries/libldap/msctrl.c
    libraries/libldap/open.c
    libraries/libldap/options.c
    libraries/libldap/os-ip.c
    libraries/libldap/os-local.c
    libraries/libldap/pagectrl.c
    libraries/libldap/passwd.c
    libraries/libldap/ppolicy.c
    libraries/libldap/print.c
    libraries/libldap/psearchctrl.c
    libraries/libldap/rdwr.c
    libraries/libldap/references.c
    libraries/libldap/request.c
    libraries/libldap/result.c
    libraries/libldap/rq.c
    libraries/libldap/sasl.c
    libraries/libldap/sbind.c
    libraries/libldap/schema.c
    libraries/libldap/search.c
    libraries/libldap/sort.c
    libraries/libldap/sortctrl.c
    libraries/libldap/stctrl.c
    libraries/libldap/string.c
    libraries/libldap/tavl.c
    libraries/libldap/thr_debug.c
    libraries/libldap/thr_nt.c
    libraries/libldap/thr_posix.c
    libraries/libldap/thr_pth.c
    libraries/libldap/thr_thr.c
    libraries/libldap/threads.c
    libraries/libldap/tls2.c
    libraries/libldap/tls_g.c
    libraries/libldap/tls_o.c
    libraries/libldap/tpool.c
    libraries/libldap/turn.c
    libraries/libldap/txn.c
    libraries/libldap/unbind.c
    libraries/libldap/url.c
    libraries/libldap/utf-8-conv.c
    libraries/libldap/utf-8.c
    libraries/libldap/util-int.c
    libraries/libldap/vc.c
    libraries/libldap/version.c
    libraries/libldap/vlvctrl.c
    libraries/libldap/whoami.c
)

END()

RECURSE(
    libraries
)
