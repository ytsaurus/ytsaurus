# This file was generated by the YaTool build system (https://github.com/yandex/yatool),
# from a source YaTool build configuration provided in ya.make files.
#
# If the repository supports both CMake and ya build configurations, please modify both of them.
#
# If only CMake build configuration is supported then modify only CMake files and note that only
# simple modifications are allowed like adding source-files to targets or adding simple properties
# like target_include_directories. These modifications will be ported to original ya.make files
# by maintainers. Any complex modifications which can't be easily ported back to the ya build
# system may be rejected.
#
# Please refer to the build instructions in the repository for more information about manual
# changes in this file.

find_package(linux-headers-generic REQUIRED)

add_library(contrib-libs-curl)


target_compile_options(contrib-libs-curl PUBLIC
  -DCURL_STATICLIB
)

target_compile_options(contrib-libs-curl PRIVATE
  -DBUILDING_LIBCURL
  -DHAVE_CONFIG_H
  -DARCADIA_CURL_DNS_RESOLVER_ARES
  $<IF:$<CXX_COMPILER_ID:MSVC>,,-Wno-everything>
)

target_include_directories(contrib-libs-curl PUBLIC
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/include
)

target_include_directories(contrib-libs-curl PRIVATE
  ${PROJECT_SOURCE_DIR}/contrib/libs/c-ares/include
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib
  ${PROJECT_SOURCE_DIR}/contrib/libs/libssh2/include
  ${PROJECT_SOURCE_DIR}/contrib/libs/ngtcp2/crypto/includes
  ${PROJECT_SOURCE_DIR}/contrib/libs/zstd/include
)

target_link_libraries(contrib-libs-curl PUBLIC
  linux-headers-generic::linux-headers-generic
  brotli-c-dec
  brotli-c-enc
  contrib-libs-libc_compat
  contrib-libs-libssh2
  contrib-libs-nghttp2
  contrib-libs-nghttp3
  contrib-libs-ngtcp2
  contrib-libs-openssl
  contrib-libs-zlib
  contrib-libs-zstd
  contrib-libs-c-ares
)

target_sources(contrib-libs-curl PRIVATE
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/altsvc.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/amigaos.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/asyn-ares.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/asyn-thread.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/base64.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/bufq.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/bufref.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/c-hyper.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/cf-h1-proxy.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/cf-h2-proxy.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/cf-haproxy.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/cf-https-connect.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/cf-socket.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/cfilters.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/conncache.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/connect.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/content_encoding.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/cookie.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/curl_addrinfo.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/curl_des.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/curl_endian.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/curl_fnmatch.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/curl_get_line.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/curl_gethostname.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/curl_gssapi.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/curl_memrchr.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/curl_multibyte.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/curl_ntlm_core.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/curl_ntlm_wb.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/curl_path.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/curl_range.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/curl_rtmp.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/curl_sasl.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/curl_sspi.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/curl_threads.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/curl_trc.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/dict.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/doh.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/dynbuf.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/dynhds.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/easy.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/easygetopt.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/easyoptions.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/escape.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/file.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/fileinfo.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/fopen.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/formdata.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/ftp.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/ftplistparser.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/getenv.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/getinfo.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/gopher.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/hash.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/headers.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/hmac.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/hostasyn.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/hostip.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/hostip4.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/hostip6.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/hostsyn.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/hsts.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/http.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/http1.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/http2.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/http_aws_sigv4.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/http_chunks.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/http_digest.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/http_negotiate.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/http_ntlm.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/http_proxy.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/idn.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/if2ip.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/imap.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/inet_ntop.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/inet_pton.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/krb5.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/ldap.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/llist.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/macos.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/md4.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/md5.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/memdebug.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/mime.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/mprintf.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/mqtt.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/multi.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/netrc.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/nonblock.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/noproxy.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/openldap.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/parsedate.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/pingpong.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/pop3.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/progress.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/psl.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/rand.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/rename.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/rtsp.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/select.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/sendf.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/setopt.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/sha256.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/share.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/slist.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/smb.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/smtp.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/socketpair.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/socks.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/socks_gssapi.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/socks_sspi.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/speedcheck.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/splay.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/strcase.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/strdup.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/strerror.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/strtok.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/strtoofft.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/system_win32.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/telnet.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/tftp.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/timediff.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/timeval.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/transfer.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/url.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/urlapi.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vauth/cleartext.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vauth/cram.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vauth/digest.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vauth/digest_sspi.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vauth/gsasl.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vauth/krb5_gssapi.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vauth/krb5_sspi.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vauth/ntlm.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vauth/ntlm_sspi.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vauth/oauth2.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vauth/spnego_gssapi.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vauth/spnego_sspi.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vauth/vauth.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/version.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/version_win32.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vquic/curl_msh3.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vquic/curl_ngtcp2.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vquic/curl_quiche.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vquic/vquic.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vssh/libssh.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vssh/libssh2.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vssh/wolfssh.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vtls/bearssl.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vtls/hostcheck.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vtls/keylog.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vtls/mbedtls_threadlock.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vtls/openssl.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vtls/rustls.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vtls/sectransp.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vtls/vtls.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vtls/wolfssl.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/vtls/x509asn1.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/warnless.c
  ${PROJECT_SOURCE_DIR}/contrib/libs/curl/lib/ws.c
)

