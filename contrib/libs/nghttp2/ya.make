# Generated by devtools/yamaker from nixpkgs 24.05.

LIBRARY()

LICENSE(
    FSFAP AND
    MIT
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(1.66.0)

ORIGINAL_SOURCE(https://github.com/nghttp2/nghttp2/archive/v1.66.0.tar.gz)

ADDINCL(
    GLOBAL contrib/libs/nghttp2/lib/includes
    contrib/libs/nghttp2
    contrib/libs/nghttp2/lib
)

NO_COMPILER_WARNINGS()

NO_RUNTIME()

CFLAGS(
    -DBUILDING_NGHTTP2
    -DHAVE_CONFIG_H
)

SRCS(
    lib/nghttp2_alpn.c
    lib/nghttp2_buf.c
    lib/nghttp2_callbacks.c
    lib/nghttp2_debug.c
    lib/nghttp2_extpri.c
    lib/nghttp2_frame.c
    lib/nghttp2_hd.c
    lib/nghttp2_hd_huffman.c
    lib/nghttp2_hd_huffman_data.c
    lib/nghttp2_helper.c
    lib/nghttp2_http.c
    lib/nghttp2_map.c
    lib/nghttp2_mem.c
    lib/nghttp2_option.c
    lib/nghttp2_outbound_item.c
    lib/nghttp2_pq.c
    lib/nghttp2_priority_spec.c
    lib/nghttp2_queue.c
    lib/nghttp2_ratelim.c
    lib/nghttp2_rcbuf.c
    lib/nghttp2_session.c
    lib/nghttp2_stream.c
    lib/nghttp2_submit.c
    lib/nghttp2_time.c
    lib/nghttp2_version.c
    lib/sfparse.c
)

END()
