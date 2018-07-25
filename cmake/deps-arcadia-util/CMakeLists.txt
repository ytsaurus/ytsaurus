set(CHARSET_BASE ${CMAKE_SOURCE_DIR}/util/charset)
set(CHARSET_SRCS
  ${CHARSET_BASE}/generated/unidata.cpp
  ${CHARSET_BASE}/recode_result.cpp
  ${CHARSET_BASE}/unicode_table.cpp
  ${CHARSET_BASE}/unidata.cpp
  ${CHARSET_BASE}/utf8.cpp
  ${CHARSET_BASE}/wide_sse41.cpp
  ${CHARSET_BASE}/wide.cpp
)

set(STRING_BASE ${CMAKE_SOURCE_DIR}/util/string)
set(STRING_SRCS
  ${STRING_BASE}/ascii.cpp
  ${STRING_BASE}/builder.cpp
  ${STRING_BASE}/cast.cc
  ${STRING_BASE}/cgiparam.cpp
  ${STRING_BASE}/cstriter.cpp
  ${STRING_BASE}/escape.cpp
  ${STRING_BASE}/hex.cpp
  ${STRING_BASE}/iterator.cpp
  ${STRING_BASE}/join.cpp
  ${STRING_BASE}/kmp.cpp
  ${STRING_BASE}/pcdata.cpp
  ${STRING_BASE}/printf.cpp
  ${STRING_BASE}/quote.cpp
  ${STRING_BASE}/scan.cpp
  ${STRING_BASE}/split.cpp
  ${STRING_BASE}/split_iterator.cpp
  ${STRING_BASE}/strip.cpp
  ${STRING_BASE}/strspn.cpp
  ${STRING_BASE}/subst.cpp
  ${STRING_BASE}/type.cpp
  ${STRING_BASE}/url.cpp
  ${STRING_BASE}/util.cpp
  ${STRING_BASE}/vector.cpp
)

set(GENERIC_BASE ${CMAKE_SOURCE_DIR}/util/generic)
set(GENERIC_SRCS
  ${GENERIC_BASE}/adaptor.cpp
  ${GENERIC_BASE}/algorithm.cpp
  ${GENERIC_BASE}/array_ref.cpp
  ${GENERIC_BASE}/array_size.cpp
  ${GENERIC_BASE}/bitmap.cpp
  ${GENERIC_BASE}/bitops.cpp
  ${GENERIC_BASE}/bt_exception.cpp
  ${GENERIC_BASE}/buffer.cpp
  ${GENERIC_BASE}/cast.cpp
  ${GENERIC_BASE}/chartraits.cpp
  ${GENERIC_BASE}/deque.cpp
  ${GENERIC_BASE}/explicit_type.cpp
  ${GENERIC_BASE}/fastqueue.cpp
  ${GENERIC_BASE}/flags.cpp
  ${GENERIC_BASE}/function.cpp
  ${GENERIC_BASE}/fuzz/vector/main.cpp
  ${GENERIC_BASE}/fwd.cpp
  ${GENERIC_BASE}/guid.cpp
  ${GENERIC_BASE}/hash.cpp
  ${GENERIC_BASE}/hash_primes.cpp
  ${GENERIC_BASE}/hash_set.cpp
  ${GENERIC_BASE}/hide_ptr.cpp
  ${GENERIC_BASE}/intrlist.cpp
  ${GENERIC_BASE}/is_in.cpp
  ${GENERIC_BASE}/iterator.cpp
  ${GENERIC_BASE}/iterator_range.cpp
  ${GENERIC_BASE}/lazy_value.cpp
  ${GENERIC_BASE}/list.cpp
  ${GENERIC_BASE}/map.cpp
  ${GENERIC_BASE}/mapfindptr.cpp
  ${GENERIC_BASE}/maybe.cpp
  ${GENERIC_BASE}/mem_copy.cpp
  ${GENERIC_BASE}/noncopyable.cpp
  ${GENERIC_BASE}/object_counter.cpp
  ${GENERIC_BASE}/ptr.cpp
  ${GENERIC_BASE}/queue.cpp
  ${GENERIC_BASE}/refcount.cpp
  ${GENERIC_BASE}/region.cpp
  ${GENERIC_BASE}/reinterpretcast.cpp
  ${GENERIC_BASE}/set.cpp
  ${GENERIC_BASE}/singleton.cpp
  ${GENERIC_BASE}/stack.cpp
  ${GENERIC_BASE}/stlfwd.cpp
  ${GENERIC_BASE}/store_policy.cpp
  ${GENERIC_BASE}/strbuf.cpp
  ${GENERIC_BASE}/strfcpy.cpp
  ${GENERIC_BASE}/string.cpp
  ${GENERIC_BASE}/typelist.cpp
  ${GENERIC_BASE}/type_name.cpp
  ${GENERIC_BASE}/typetraits.cpp
  ${GENERIC_BASE}/utility.cpp
  ${GENERIC_BASE}/va_args.cpp
  #${GENERIC_BASE}/variant.cpp
  ${GENERIC_BASE}/vector.cpp
  ${GENERIC_BASE}/vector_ops.cpp
  ${GENERIC_BASE}/xrange.cpp
  ${GENERIC_BASE}/yexception.cpp
  ${GENERIC_BASE}/ylimits.cpp
  ${GENERIC_BASE}/ymath.cpp
)

set(DIGEST_BASE ${CMAKE_SOURCE_DIR}/util/digest)
set(DIGEST_SRCS
  ${DIGEST_BASE}/city.cpp
  ${DIGEST_BASE}/fnv.cpp
  ${DIGEST_BASE}/multi.cpp
  ${DIGEST_BASE}/murmur.cpp
  ${DIGEST_BASE}/numeric.cpp
  ${DIGEST_BASE}/sequence.cpp
)

set(SYSTEM_BASE ${CMAKE_SOURCE_DIR}/util/system)
set(SYSTEM_SRCS
  ${SYSTEM_BASE}/align.cpp
  ${SYSTEM_BASE}/atexit.cpp
  ${SYSTEM_BASE}/atomic.cpp
  ${SYSTEM_BASE}/backtrace.cpp
  ${SYSTEM_BASE}/byteorder.cpp
  ${SYSTEM_BASE}/compat.cpp
  ${SYSTEM_BASE}/compiler.cpp
  ${SYSTEM_BASE}/condvar.cpp
  ${SYSTEM_BASE}/context.cpp
  ${SYSTEM_BASE}/cpu_id.cpp
  ${SYSTEM_BASE}/daemon.cpp
  ${SYSTEM_BASE}/datetime.cpp
  ${SYSTEM_BASE}/demangle.cpp
  ${SYSTEM_BASE}/direct_io.cpp
  ${SYSTEM_BASE}/dynlib.cpp
  ${SYSTEM_BASE}/env.cpp
  ${SYSTEM_BASE}/err.cpp
  ${SYSTEM_BASE}/error.cpp
  ${SYSTEM_BASE}/event.cpp
  ${SYSTEM_BASE}/execpath.cpp
  ${SYSTEM_BASE}/fasttime.cpp
  ${SYSTEM_BASE}/fhandle.cpp
  ${SYSTEM_BASE}/file.cpp
  ${SYSTEM_BASE}/file_lock.cpp
  ${SYSTEM_BASE}/filemap.cpp
  ${SYSTEM_BASE}/flock.cpp
  ${SYSTEM_BASE}/fs.cpp
  ${SYSTEM_BASE}/fstat.cpp
  ${SYSTEM_BASE}/getpid.cpp
  ${SYSTEM_BASE}/guard.cpp
  ${SYSTEM_BASE}/hostname.cpp
  ${SYSTEM_BASE}/hp_timer.cpp
  ${SYSTEM_BASE}/info.cpp
  ${SYSTEM_BASE}/madvise.cpp
  ${SYSTEM_BASE}/maxlen.cpp
  ${SYSTEM_BASE}/mem_info.cpp
  ${SYSTEM_BASE}/mktemp.cpp
  ${SYSTEM_BASE}/mlock.cpp
  ${SYSTEM_BASE}/mutex.cpp
  ${SYSTEM_BASE}/nice.cpp
  ${SYSTEM_BASE}/pipe.cpp
  ${SYSTEM_BASE}/platform.cpp
  ${SYSTEM_BASE}/progname.cpp
  ${SYSTEM_BASE}/protect.cpp
  ${SYSTEM_BASE}/rusage.cpp
  ${SYSTEM_BASE}/rwlock.cpp
  ${SYSTEM_BASE}/sanitizers.cpp
  ${SYSTEM_BASE}/sem.cpp
  ${SYSTEM_BASE}/shellcommand.cpp
  ${SYSTEM_BASE}/shmat.cpp
  ${SYSTEM_BASE}/sigset.cpp
  ${SYSTEM_BASE}/spinlock.cpp
  ${SYSTEM_BASE}/spin_wait.cpp
  ${SYSTEM_BASE}/src_location.cpp
  ${SYSTEM_BASE}/strlcpy.c
  ${SYSTEM_BASE}/sys_alloc.cpp
  ${SYSTEM_BASE}/sysstat.cpp
  ${SYSTEM_BASE}/tempfile.cpp
  ${SYSTEM_BASE}/thread.cpp
  ${SYSTEM_BASE}/tls.cpp
  ${SYSTEM_BASE}/types.cpp
  ${SYSTEM_BASE}/unaligned_mem.cpp
  ${SYSTEM_BASE}/user.cpp
  ${SYSTEM_BASE}/utime.cpp
  ${SYSTEM_BASE}/valgrind.cpp
  ${SYSTEM_BASE}/yassert.cpp
  ${SYSTEM_BASE}/yield.cpp
)

set(MEMORY_BASE ${CMAKE_SOURCE_DIR}/util/memory)
set(MEMORY_SRCS
  ${MEMORY_BASE}/addstorage.cpp
  ${MEMORY_BASE}/alloc.cpp
  ${MEMORY_BASE}/blob.cpp
  ${MEMORY_BASE}/mmapalloc.cpp
  ${MEMORY_BASE}/pool.cpp
  ${MEMORY_BASE}/segmented_string_pool.cpp
  ${MEMORY_BASE}/segpool_alloc.cpp
  ${MEMORY_BASE}/smallobj.cpp
  ${MEMORY_BASE}/tempbuf.cpp
)

set(STREAM_BASE ${CMAKE_SOURCE_DIR}/util/stream)
set(STREAM_SRCS
  ${STREAM_BASE}/aligned.cpp
  ${STREAM_BASE}/buffer.cpp
  ${STREAM_BASE}/buffered.cpp
  ${STREAM_BASE}/debug.cpp
  ${STREAM_BASE}/direct_io.cpp
  ${STREAM_BASE}/file.cpp
  ${STREAM_BASE}/format.cpp
  ${STREAM_BASE}/hex.cpp
  ${STREAM_BASE}/holder.cpp
  ${STREAM_BASE}/input.cpp
  ${STREAM_BASE}/labeled.cpp
  ${STREAM_BASE}/length.cpp
  ${STREAM_BASE}/mem.cpp
  ${STREAM_BASE}/multi.cpp
  ${STREAM_BASE}/null.cpp
  ${STREAM_BASE}/output.cpp
  ${STREAM_BASE}/pipe.cpp
  ${STREAM_BASE}/printf.cpp
  ${STREAM_BASE}/str.cpp
  ${STREAM_BASE}/tee.cpp
  ${STREAM_BASE}/tempbuf.cpp
  ${STREAM_BASE}/tokenizer.cpp
  ${STREAM_BASE}/trace.cpp
  ${STREAM_BASE}/walk.cpp
  ${STREAM_BASE}/zerocopy.cpp
  ${STREAM_BASE}/zlib.cpp
)

set(NETWORK_BASE ${CMAKE_SOURCE_DIR}/util/network)
set(NETWORK_SRCS
  ${NETWORK_BASE}/address.cpp
  ${NETWORK_BASE}/endpoint.cpp
  ${NETWORK_BASE}/hostip.cpp
  ${NETWORK_BASE}/init.cpp
  ${NETWORK_BASE}/interface.cpp
  ${NETWORK_BASE}/iovec.cpp
  ${NETWORK_BASE}/ip.cpp
  ${NETWORK_BASE}/netloss.cpp
  ${NETWORK_BASE}/nonblock.cpp
  ${NETWORK_BASE}/pair.cpp
  ${NETWORK_BASE}/poller.cpp
  ${NETWORK_BASE}/pollerimpl.cpp
  ${NETWORK_BASE}/sock.cpp
  ${NETWORK_BASE}/socket.cpp
)

set(FOLDER_BASE ${CMAKE_SOURCE_DIR}/util/folder)
set(FOLDER_SRCS
  ${FOLDER_BASE}/dirut.cpp
  ${FOLDER_BASE}/filelist.cpp
  ${FOLDER_BASE}/fts.cpp
  ${FOLDER_BASE}/iterator.cpp
  ${FOLDER_BASE}/path.cpp
  ${FOLDER_BASE}/pathsplit.cpp
  ${FOLDER_BASE}/tempdir.cpp
)

if(WIN32)
  set(FOLDER_SRCS ${FOLDER_SRCS}
    ${FOLDER_BASE}/dirent_win.c
    ${FOLDER_BASE}/lstat_win.c
  )
endif()

set(DATETIME_BASE ${CMAKE_SOURCE_DIR}/util/datetime)
set(DATETIME_SRCS
  ${DATETIME_BASE}/base.cpp
  ${DATETIME_BASE}/constants.cpp
  ${DATETIME_BASE}/cputimer.cpp
  ${DATETIME_BASE}/parser.cpp
  ${DATETIME_BASE}/strptime.cpp
  ${DATETIME_BASE}/systime.cpp
)

set(THREAD_BASE ${CMAKE_SOURCE_DIR}/util/thread)
set(THREAD_SRCS
  ${THREAD_BASE}/lfqueue.cpp
  ${THREAD_BASE}/lfstack.cpp
  ${THREAD_BASE}/pool.cpp
  ${THREAD_BASE}/queue.cpp
  ${THREAD_BASE}/singleton.cpp
)

set(RANDOM_BASE ${CMAKE_SOURCE_DIR}/util/random)
set(RANDOM_SRCS
  ${RANDOM_BASE}/common_ops.cpp
  ${RANDOM_BASE}/easy.cpp
  ${RANDOM_BASE}/entropy.cpp
  ${RANDOM_BASE}/fast.cpp
  ${RANDOM_BASE}/lcg_engine.cpp
  ${RANDOM_BASE}/mersenne32.cpp
  ${RANDOM_BASE}/mersenne64.cpp
  ${RANDOM_BASE}/mersenne.cpp
  ${RANDOM_BASE}/normal.cpp
  ${RANDOM_BASE}/random.cpp
  ${RANDOM_BASE}/shuffle.cpp
)

set( DRAFT_BASE ${CMAKE_SOURCE_DIR}/util/draft )
set( DRAFT_SRCS
 #${DRAFT_BASE}/autozero.cpp
 #${DRAFT_BASE}/backtrace_exception.cpp
 ${DRAFT_BASE}/bitutils.cpp
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
 #${DRAFT_BASE}/netloss.cpp
 #${DRAFT_BASE}/parse_utils.cpp
 #${DRAFT_BASE}/prob_distr.cpp
 #${DRAFT_BASE}/prog_options.cpp
)

set(ROOT_BASE ${CMAKE_SOURCE_DIR}/util)
set(ROOT_SRCS
  ${ROOT_BASE}/str_stl.cpp
  ${ROOT_BASE}/ysafeptr.cpp
  ${ROOT_BASE}/ysaveload.cpp
)

set(SRCS_ASM
  ${SYSTEM_BASE}/context_x86.asm
)

set(CAN_USE_ASSEMBLER TRUE)
enable_language(ASM_NASM)

foreach(f ${SRCS_ASM})
  set_property(SOURCE ${f} APPEND_STRING PROPERTY COMPILE_FLAGS " -D_x86_64_ ")
  set_property(SOURCE ${f} APPEND_STRING PROPERTY COMPILE_FLAGS " -I${SYSTEM_BASE}/ ")
  if(APPLE)
    set_property(SOURCE ${f} APPEND_STRING PROPERTY COMPILE_FLAGS " -DDARWIN ")
  endif()
endforeach()

set(SRCS
  ${CHARSET_SRCS}
  ${STRING_SRCS}
  ${GENERIC_SRCS}
  ${DIGEST_SRCS}
  ${SYSTEM_SRCS}
  ${MEMORY_SRCS}
  ${STREAM_SRCS}
  ${NETWORK_SRCS}
  ${FOLDER_SRCS}
  ${DATETIME_SRCS}
  ${THREAD_SRCS}
  ${RANDOM_SRCS}
  ${DRAFT_SRCS}
  ${ROOT_SRCS}
)


add_library(deps-arcadia-util STATIC
  ${SRCS}
  ${SRCS_ASM}
)

target_link_libraries(deps-arcadia-util
  deps-arcadia-contrib-libs-double--conversion
  deps-arcadia-contrib-libs-zlib
)

if(CMAKE_COMPILER_IS_GNUCXX)
  target_link_libraries(deps-arcadia-util -ldl)
  if(NOT APPLE)
    target_link_libraries(deps-arcadia-util -lrt)
  endif()
endif()

include_directories(${CMAKE_SOURCE_DIR})

if(CMAKE_COMPILER_IS_GNUCXX)
  foreach(f ${SRCS})
    set_source_files_properties(${f} PROPERTIES COMPILE_FLAGS -fPIC)
  endforeach()
endif()

if(APPLE)
  foreach(f ${SRCS})
    set_source_files_properties(${f} PROPERTIES COMPILE_DEFINITIONS "_REENTRANT;_XOPEN_SOURCE")
  endforeach()
endif()

if (_is_clang)
  foreach(f ${SRCS})
    set_source_files_properties(${f} PROPERTIES COMPILE_FLAGS -Wno-unknown-attributes)
  endforeach()
endif()
