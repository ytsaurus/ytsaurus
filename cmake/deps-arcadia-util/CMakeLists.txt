set( STRING_BASE ${CMAKE_SOURCE_DIR}/util/string )
set( STRING_SRCS
  ${STRING_BASE}/ascii.cpp
  ${STRING_BASE}/base64.cpp
  ${STRING_BASE}/cast.cc
 #${STRING_BASE}/cgiparam.cpp (uses charset/)
 #${STRING_BASE}/dtoa.cpp (included in cast.cc)
 #${STRING_BASE}/dtoa_impl.cpp (included in cast.cc)
 #${STRING_BASE}/encodexml.cpp (uses charset/)
  ${STRING_BASE}/escape.cpp
 #${STRING_BASE}/htmlentity.cpp (uses charset/)
  ${STRING_BASE}/http.cpp
  ${STRING_BASE}/kmp.cpp
 #${STRING_BASE}/pcdata.cpp (uses charset/)
  ${STRING_BASE}/printf.cpp
 #${STRING_BASE}/quote.cpp (uses charset/)
  ${STRING_BASE}/split.cpp
  ${STRING_BASE}/split_iterator.cpp
  ${STRING_BASE}/strip.cpp
  ${STRING_BASE}/type.cpp
  ${STRING_BASE}/url.cpp
  ${STRING_BASE}/util.cpp
  ${STRING_BASE}/vector.cpp
)

set( GENERIC_BASE ${CMAKE_SOURCE_DIR}/util/generic )
set( GENERIC_SRCS
  ${GENERIC_BASE}/buffer.cpp
  ${GENERIC_BASE}/chartraits.cpp
  ${GENERIC_BASE}/guid.cpp
  ${GENERIC_BASE}/ptr.cpp
  ${GENERIC_BASE}/singleton.cpp
  ${GENERIC_BASE}/strbuf.cpp
  ${GENERIC_BASE}/strfcpy.cpp
  ${GENERIC_BASE}/stroka.cpp
  ${GENERIC_BASE}/stroka_io.cpp
  ${GENERIC_BASE}/stroka_stdio.cpp
  ${GENERIC_BASE}/yexception.cpp
  ${GENERIC_BASE}/ymath.cpp
)

set( DIGEST_BASE ${CMAKE_SOURCE_DIR}/util/digest )
set( DIGEST_SRCS
  ${DIGEST_BASE}/city.cpp
  ${DIGEST_BASE}/crc.cpp
  ${DIGEST_BASE}/md5.cpp
  ${DIGEST_BASE}/murmur.cpp
)

set( SYSTEM_BASE ${CMAKE_SOURCE_DIR}/util/system )
set( SYSTEM_SRCS
  ${SYSTEM_BASE}/atexit.cpp
  ${SYSTEM_BASE}/backtrace.cpp
  ${SYSTEM_BASE}/compat.cpp
  ${SYSTEM_BASE}/condvar.cpp
  ${SYSTEM_BASE}/context.cpp
  ${SYSTEM_BASE}/daemon.cpp
  ${SYSTEM_BASE}/datetime.cpp
  ${SYSTEM_BASE}/demangle.cpp
  ${SYSTEM_BASE}/dynlib.cpp
  ${SYSTEM_BASE}/err.cpp
  ${SYSTEM_BASE}/error.cpp
  ${SYSTEM_BASE}/event.cpp
  ${SYSTEM_BASE}/execpath.cpp
  ${SYSTEM_BASE}/fasttime.cpp
  ${SYSTEM_BASE}/file.cpp
  ${SYSTEM_BASE}/filemap.cpp
  ${SYSTEM_BASE}/flock.cpp
  ${SYSTEM_BASE}/freeBSD_mktemp.cpp
  ${SYSTEM_BASE}/fs.cpp
  ${SYSTEM_BASE}/fstat.cpp
  ${SYSTEM_BASE}/hostname.cpp
  ${SYSTEM_BASE}/hp_timer.cpp
  ${SYSTEM_BASE}/info.cpp
  ${SYSTEM_BASE}/mktemp.cpp
  ${SYSTEM_BASE}/mlock.cpp
  ${SYSTEM_BASE}/mutex.cpp
  ${SYSTEM_BASE}/oldfile.cpp
  ${SYSTEM_BASE}/pipe.cpp
  ${SYSTEM_BASE}/progname.cpp
  ${SYSTEM_BASE}/protect.cpp
  ${SYSTEM_BASE}/rusage.cpp
  ${SYSTEM_BASE}/rwlock.cpp
  ${SYSTEM_BASE}/sem.cpp
  ${SYSTEM_BASE}/shellcommand.cpp
  ${SYSTEM_BASE}/shmat.cpp
  ${SYSTEM_BASE}/spinlock.cpp
  ${SYSTEM_BASE}/strlcpy.c
  ${SYSTEM_BASE}/sysstat.cpp
  ${SYSTEM_BASE}/thread.cpp
  ${SYSTEM_BASE}/tls.cpp
  ${SYSTEM_BASE}/user.cpp
  ${SYSTEM_BASE}/yassert.cpp
  ${SYSTEM_BASE}/yield.cpp
)

if (WIN32)
  set( SYSTEM_SRCS ${SYSTEM_SRCS}
    ${SYSTEM_BASE}/fs_win.cpp
  )
endif()

set( MEMORY_BASE ${CMAKE_SOURCE_DIR}/util/memory )
set( MEMORY_SRCS
  ${MEMORY_BASE}/alloc.cpp
  ${MEMORY_BASE}/blob.cpp
  ${MEMORY_BASE}/mmapalloc.cpp
  ${MEMORY_BASE}/pool.cpp
  ${MEMORY_BASE}/profile.cpp
  ${MEMORY_BASE}/tempbuf.cpp
)

set( STREAM_BASE ${CMAKE_SOURCE_DIR}/util/stream )
set( STREAM_SRCS
  ${STREAM_BASE}/buffer.cpp
  ${STREAM_BASE}/buffered.cpp
 #${STREAM_BASE}/bzip2.cpp (uses contrib/libbz2)
  ${STREAM_BASE}/chunk.cpp
  ${STREAM_BASE}/debug.cpp
  ${STREAM_BASE}/factory.cpp
  ${STREAM_BASE}/file.cpp
  ${STREAM_BASE}/format.cpp
  ${STREAM_BASE}/helpers.cpp
  ${STREAM_BASE}/http.cpp
  ${STREAM_BASE}/input.cpp
  ${STREAM_BASE}/lz.cpp
 #${STREAM_BASE}/lzma.cpp (uses contrib/lzmasdk)
  ${STREAM_BASE}/lzop.cpp
  ${STREAM_BASE}/mem.cpp
  ${STREAM_BASE}/multi.cpp
  ${STREAM_BASE}/null.cpp
  ${STREAM_BASE}/output.cpp
  ${STREAM_BASE}/pipe.cpp
  ${STREAM_BASE}/printf.cpp
  ${STREAM_BASE}/stack_file.cpp
  ${STREAM_BASE}/str.cpp
  ${STREAM_BASE}/tee.cpp
  ${STREAM_BASE}/zerocopy.cpp
  ${STREAM_BASE}/zlib.cpp
)

set( CONFIG_BASE ${CMAKE_SOURCE_DIR}/util/config )
set( CONFIG_SRCS
  ${CONFIG_BASE}/last_getopt.cpp
  ${CONFIG_BASE}/opt2.cpp
  ${CONFIG_BASE}/opt.cpp
  ${CONFIG_BASE}/posix_getopt.cpp
  ${CONFIG_BASE}/ygetopt.cpp
)

set( NETWORK_BASE ${CMAKE_SOURCE_DIR}/util/network )
set( NETWORK_SRCS
  ${NETWORK_BASE}/address.cpp
  ${NETWORK_BASE}/endpoint.cpp
  ${NETWORK_BASE}/hostip.cpp
  ${NETWORK_BASE}/init.cpp
  ${NETWORK_BASE}/interface.cpp
  ${NETWORK_BASE}/pair.cpp
  ${NETWORK_BASE}/poller.cpp
  ${NETWORK_BASE}/socket.cpp
)

set( FOLDER_BASE ${CMAKE_SOURCE_DIR}/util/folder )
set( FOLDER_SRCS
  ${FOLDER_BASE}/dirut.cpp
  ${FOLDER_BASE}/filelist.cpp
  ${FOLDER_BASE}/fts.cpp
  ${FOLDER_BASE}/iterator.cpp
  ${FOLDER_BASE}/path.cpp
  ${FOLDER_BASE}/tempdir.cpp
)

if(WIN32)
  set( FOLDER_SRCS ${FOLDER_SRCS}
    ${FOLDER_BASE}/dirent_win.cpp
    ${FOLDER_BASE}/lstat_win.cpp
  )
endif()

set( DATETIME_BASE ${CMAKE_SOURCE_DIR}/util/datetime )
set( DATETIME_SRCS
  ${DATETIME_BASE}/base.cpp
  ${DATETIME_BASE}/cputimer.cpp
  ${DATETIME_BASE}/parser.cpp
  ${DATETIME_BASE}/strptime.cpp
  ${DATETIME_BASE}/systime.cpp
)

if (WIN32)
  set( DATETIME_SRCS ${DATETIME_SRCS}
    ${DATETIME_BASE}/strptime.cpp
  )
endif()

set( THREAD_BASE ${CMAKE_SOURCE_DIR}/util/thread )
set( THREAD_SRCS
  ${THREAD_BASE}/pool.cpp
  ${THREAD_BASE}/queue.cpp
  ${THREAD_BASE}/tasks.cpp
)

set( GREEN_BASE ${CMAKE_SOURCE_DIR}/util/green )
set( GREEN_SRCS
  ${GREEN_BASE}/impl.cpp
  ${GREEN_BASE}/listen.cpp
  ${GREEN_BASE}/poller.cpp
  ${GREEN_BASE}/sockpool.cpp
  ${GREEN_BASE}/stack.cpp
)

set( RANDOM_BASE ${CMAKE_SOURCE_DIR}/util/random )
set( RANDOM_SRCS
  ${RANDOM_BASE}/entropy.cpp
  ${RANDOM_BASE}/mersenne.cpp
  ${RANDOM_BASE}/randcpp.cpp
  ${RANDOM_BASE}/random.cpp
  ${RANDOM_BASE}/rc4.cpp
)

set( DRAFT_BASE ${CMAKE_SOURCE_DIR}/util/draft )
set( DRAFT_SRCS
 #${DRAFT_BASE}/autozero.cpp
 #${DRAFT_BASE}/backtrace_exception.cpp
 #${DRAFT_BASE}/bitutils.cpp
 #${DRAFT_BASE}/datafile.cpp
 #${DRAFT_BASE}/date.cpp
 #${DRAFT_BASE}/datetime.cpp
 #${DRAFT_BASE}/delim_file_iter.cpp
 #${DRAFT_BASE}/env.cpp
 #${DRAFT_BASE}/file_io.cpp
 #${DRAFT_BASE}/file_poller.cpp
 #${DRAFT_BASE}/file_utils.cpp
 #${DRAFT_BASE}/free_thread_queue.cpp
 #${DRAFT_BASE}/modchooser.cpp
  ${DRAFT_BASE}/netloss.cpp
 #${DRAFT_BASE}/parse_utils.cpp
 #${DRAFT_BASE}/prob_distr.cpp
 #${DRAFT_BASE}/prog_options.cpp
)

set( ROOT_BASE ${CMAKE_SOURCE_DIR}/util )
set( ROOT_SRCS
  ${ROOT_BASE}/ffb.cpp
  ${ROOT_BASE}/fileptr.cpp
  ${ROOT_BASE}/httpcodes.cpp
  ${ROOT_BASE}/httpdate.cpp
  ${ROOT_BASE}/mbitmap.cpp
  ${ROOT_BASE}/str_hash.cpp
  ${ROOT_BASE}/ysafeptr.cpp
  ${ROOT_BASE}/ysaveload.cpp
)

add_library( ytext-arcadia-util STATIC
  ${STRING_SRCS}
  ${GENERIC_SRCS}
  ${DIGEST_SRCS}
  ${SYSTEM_SRCS}
  ${MEMORY_SRCS}
  ${STREAM_SRCS}
  ${CONFIG_SRCS}
  ${NETWORK_SRCS}
  ${FOLDER_SRCS}
  ${DATETIME_SRCS}
  ${THREAD_SRCS}
  ${GREEN_SRCS}
  ${RANDOM_SRCS}
  ${DRAFT_SRCS}
  ${ROOT_SRCS}
)

target_link_libraries ( ytext-arcadia-util
  ytext-snappy
  ytext-lz4
  ytext-fastlz
  ytext-minilzo
  ytext-zlib
  ytext-quicklz
  ytext-zstd
  ytext-brotli
)

if (CMAKE_COMPILER_IS_GNUCXX)
  target_link_libraries( ytext-arcadia-util -ldl )
  if (NOT APPLE)
    target_link_libraries( ytext-arcadia-util -lrt )
  endif()
endif()

include_directories( ${CMAKE_SOURCE_DIR} )

if (CMAKE_COMPILER_IS_GNUCXX)
  set_target_properties( ytext-arcadia-util PROPERTIES COMPILE_FLAGS "-fPIC" )
endif()

if (APPLE)
  set_target_properties( ytext-arcadia-util PROPERTIES COMPILE_DEFINITIONS "_REENTRANT;_XOPEN_SOURCE" )
endif()
