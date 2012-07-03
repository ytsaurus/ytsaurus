set( BASE ${CMAKE_SOURCE_DIR}/util/string )

set( SRCS
  ${BASE}/base64.cpp
  ${BASE}/cast.cpp
  ${BASE}/escape.cpp
  ${BASE}/util.cpp
  ${BASE}/vector.cpp
  ${BASE}/split_iterator.cpp
  ${BASE}/split.cpp
  ${BASE}/url.cpp
  ${BASE}/kmp.cpp
  ${BASE}/http.cpp
  ${BASE}/ascii.cpp
  ${BASE}/printf.cpp
  ${BASE}/type.cpp
  ${BASE}/strip.cpp
  ${BASE}/pcdata.cpp
    
  ${BASE}/cat.h
  ${BASE}/hex.h
  ${BASE}/scan.h
  ${BASE}/subst.h
  ${BASE}/traits.h
)

set( G_BASE ${CMAKE_SOURCE_DIR}/util/generic )

set( G_SRCS
  ${G_BASE}/buffer.cpp
  ${G_BASE}/ptr.cpp
  ${G_BASE}/stroka.cpp
  ${G_BASE}/stroka_io.cpp
  ${G_BASE}/stroka_stdio.cpp
  ${G_BASE}/strfcpy.cpp
  ${G_BASE}/strbuf.cpp
  ${G_BASE}/yexception.cpp
  ${G_BASE}/ymath.cpp
  ${G_BASE}/chartraits.cpp

  ${G_BASE}/array.h
  ${G_BASE}/avltree.h
  ${G_BASE}/bitmap.h
  ${G_BASE}/cast.h
  ${G_BASE}/char_buf.h
  ${G_BASE}/chartraits.h
  ${G_BASE}/deque.h
  ${G_BASE}/fwd.h
  ${G_BASE}/hash.h
  ${G_BASE}/hash_set.h
  ${G_BASE}/hide.h
  ${G_BASE}/intrlist.h
  ${G_BASE}/iterator.h
  ${G_BASE}/list.h
  ${G_BASE}/map.h
  ${G_BASE}/noncopyable.h
  ${G_BASE}/pair.h
  ${G_BASE}/rbtree.h
  ${G_BASE}/refcount.h
  ${G_BASE}/set.h
  ${G_BASE}/singleton.h
  ${G_BASE}/stack.h
  ${G_BASE}/static_assert.h
  ${G_BASE}/stlfwd.h
  ${G_BASE}/typehelpers.h
  ${G_BASE}/typelist.h
  ${G_BASE}/typetraits.h
  ${G_BASE}/utility.h
  ${G_BASE}/vector.h
  ${G_BASE}/ylimits.h
)

set( D_BASE ${CMAKE_SOURCE_DIR}/util/digest )

set( D_SRCS
  ${D_BASE}/crc.cpp
  ${D_BASE}/city.cpp
  ${D_BASE}/md5.cpp

  ${D_BASE}/fnv.h
  ${D_BASE}/iterator.h
  ${D_BASE}/murmur.h
  ${D_BASE}/numeric.h
  ${D_BASE}/sfh.h
)

set( S_BASE ${CMAKE_SOURCE_DIR}/util/system )

set( S_SRCS
  ${S_BASE}/align.h
  ${S_BASE}/atexit.cpp
  ${S_BASE}/atomic.h
  ${S_BASE}/atomic_fake.h
  ${S_BASE}/atomic_gcc.h
  ${S_BASE}/atomic_win.h
  ${S_BASE}/atomic_x86.h
  ${S_BASE}/byteorder.h
  ${S_BASE}/backtrace.cpp
  ${S_BASE}/compat.cpp
  ${S_BASE}/condvar.cpp
  ${S_BASE}/context.cpp
  ${S_BASE}/daemon.cpp
  ${S_BASE}/datetime.cpp
  ${S_BASE}/dynlib.cpp
  ${S_BASE}/err.cpp
  ${S_BASE}/error.cpp
  ${S_BASE}/event.cpp
  ${S_BASE}/execpath.cpp
  ${S_BASE}/file.cpp
  ${S_BASE}/filemap.cpp
  ${S_BASE}/flock.cpp
  ${S_BASE}/guard.h
  ${S_BASE}/hostname.cpp
  ${S_BASE}/info.cpp
  ${S_BASE}/mutex.cpp
  ${S_BASE}/oldfile.cpp
  ${S_BASE}/rusage.cpp
  ${S_BASE}/rwlock.cpp
  ${S_BASE}/sem.cpp
  ${S_BASE}/spinlock.cpp
  ${S_BASE}/strlcpy.c
  ${S_BASE}/fstat.cpp
  ${S_BASE}/tempfile.h
  ${S_BASE}/thread.cpp
  ${S_BASE}/tls.cpp
  ${S_BASE}/yield.cpp
  ${S_BASE}/yassert.cpp
  ${S_BASE}/pipe.cpp
  ${S_BASE}/demangle.cpp
  ${S_BASE}/progname.cpp
  ${S_BASE}/fs.cpp
  ${S_BASE}/mktemp.cpp
  ${S_BASE}/user.cpp

  ${S_BASE}/defaults.h
  ${S_BASE}/platform.h
  ${S_BASE}/sigset.h
  ${S_BASE}/maxlen.h
)

if (LINUX)
  set( S_SRCS "${S_SRCS} ${S_BASE}/valgrind.h" )
endif()

if (LINUX OR SUN OR CYGWIN OR WIN32)
  set( S_SRCS ${S_SRCS} ${S_BASE}/freeBSD_mktemp.cpp )
endif()

set( M_BASE ${CMAKE_SOURCE_DIR}/util/memory )

set( M_SRCS
  ${M_BASE}/profile.cpp
  ${M_BASE}/tempbuf.cpp
  ${M_BASE}/blob.cpp
  ${M_BASE}/mmapalloc.cpp
  ${M_BASE}/alloc.cpp
  ${M_BASE}/pool.cpp
    
  ${M_BASE}/addstorage.h
  ${M_BASE}/gc.h
  ${M_BASE}/segmented_string_pool.h
  ${M_BASE}/segpool_alloc.h
  ${M_BASE}/smallobj.h
)

set( ST_BASE ${CMAKE_SOURCE_DIR}/util/stream )

set( ST_SRCS
  ${ST_BASE}/buffer.cpp
  ${ST_BASE}/buffered.cpp
  ${ST_BASE}/chunk.cpp
  ${ST_BASE}/debug.cpp
  ${ST_BASE}/file.cpp
  ${ST_BASE}/helpers.cpp
  ${ST_BASE}/http.cpp
  ${ST_BASE}/input.cpp
  ${ST_BASE}/mem.cpp
  ${ST_BASE}/multi.cpp
  ${ST_BASE}/null.cpp
  ${ST_BASE}/output.cpp
  ${ST_BASE}/pipe.cpp
  ${ST_BASE}/str.cpp
  ${ST_BASE}/tee.cpp
  ${ST_BASE}/zerocopy.cpp
  ${ST_BASE}/zlib.cpp
  ${ST_BASE}/printf.cpp
  ${ST_BASE}/format.cpp
  ${ST_BASE}/lz.cpp
    
  ${ST_BASE}/aligned.h
  ${ST_BASE}/base.h
  ${ST_BASE}/ios.h
  ${ST_BASE}/length.h
  ${ST_BASE}/tempbuf.h
  ${ST_BASE}/tokenizer.h
  ${ST_BASE}/walk.h
)

set( CF_BASE ${CMAKE_SOURCE_DIR}/util/config )

set( CF_SRCS
  ${CF_BASE}/last_getopt.cpp
  ${CF_BASE}/last_getopt_support.h
  ${CF_BASE}/opt.cpp
  ${CF_BASE}/opt2.cpp
  ${CF_BASE}/posix_getopt.cpp
  ${CF_BASE}/ygetopt.cpp
)

set( NW_BASE ${CMAKE_SOURCE_DIR}/util/network )

set( NW_SRCS
  ${NW_BASE}/hostip.cpp
  ${NW_BASE}/init.cpp
  ${NW_BASE}/poller.cpp
  ${NW_BASE}/socket.cpp
  ${NW_BASE}/address.cpp
  ${NW_BASE}/pair.cpp
    
  ${NW_BASE}/ip.h
  ${NW_BASE}/pollerimpl.h
  ${NW_BASE}/sock.h
)

set( FR_BASE ${CMAKE_SOURCE_DIR}/util/folder )

set( FR_SRCS
  ${FR_BASE}/iterator.h
  ${FR_BASE}/fts.cpp
  ${FR_BASE}/filelist.cpp
  ${FR_BASE}/dirut.cpp
)

if (WIN32)
  set( FR_SRCS ${FR_SRCS}
       ${FR_BASE}/lstat_win.c
       ${FR_BASE}/dirent_win.c
  )
endif()

set( DT_BASE ${CMAKE_SOURCE_DIR}/util/datetime )

set( DT_SRCS
  ${DT_BASE}/base.cpp
  ${DT_BASE}/parser.cpp
  ${DT_BASE}/constants.h
  ${DT_BASE}/cputimer.cpp
  ${DT_BASE}/systime.cpp
)

if (WIN32)
  set( DT_SRCS ${DT_SRCS}
    ${DT_BASE}/strptime.cpp
  )
endif()

set( TR_BASE ${CMAKE_SOURCE_DIR}/util/thread )

set( TR_SRCS
  ${TR_BASE}/lfqueue.h
  ${TR_BASE}/lfstack.h
  ${TR_BASE}/pool.cpp
  ${TR_BASE}/queue.cpp
  ${TR_BASE}/tasks.cpp
  ${TR_BASE}/threadable.h
)

set( SR_BASE ${CMAKE_SOURCE_DIR}/util/sorter )

set( SR_SRCS
  ${SR_BASE}/buffile.h
  ${SR_BASE}/filesort.h
  ${SR_BASE}/filesortst.h
  ${SR_BASE}/sorter.cpp
  ${SR_BASE}/sorttree.h
)

set( GR_BASE ${CMAKE_SOURCE_DIR}/util/green )

set( GR_SRCS
  ${GR_BASE}/impl.cpp
)

set( RN_BASE ${CMAKE_SOURCE_DIR}/util/random )

set( RN_SRCS
  ${RN_BASE}/randcpp.cpp
  ${RN_BASE}/random.cpp
  ${RN_BASE}/entropy.cpp
  ${RN_BASE}/rc4.cpp
    
  ${RN_BASE}/mersenne.h
  ${RN_BASE}/mersenne32.h
  ${RN_BASE}/mersenne64.h
  ${RN_BASE}/shuffle.h
)

set( SV_BASE ${CMAKE_SOURCE_DIR}/util/server )

set( SV_SRCS
  ${SV_BASE}/http.cpp
  ${SV_BASE}/listen.cpp
  ${SV_BASE}/options.cpp
)

set( UT_BASE ${CMAKE_SOURCE_DIR}/util )

set( UT_SRCS
  ${UT_BASE}/fileptr.cpp
  ${UT_BASE}/str_hash.cpp
  ${UT_BASE}/mbitmap.cpp
  ${UT_BASE}/ysafeptr.cpp
  ${UT_BASE}/yarchive.cpp
  ${UT_BASE}/ysaveload.cpp
  ${UT_BASE}/array2d.h
  ${UT_BASE}/array2d_writer.h
  ${UT_BASE}/atomizer.h
  ${UT_BASE}/autoarray.h
  ${UT_BASE}/cont_init.h
  ${UT_BASE}/fgood.h
  ${UT_BASE}/fput.h
  ${UT_BASE}/save_stl.h
  ${UT_BASE}/spars_ar.h
  ${UT_BASE}/static_hash.h
  ${UT_BASE}/static_hash_map.h
  ${UT_BASE}/str_map.h
  ${UT_BASE}/str_stl.h
  ${UT_BASE}/httpdate.cpp
  ${UT_BASE}/httpdate.h
)

if (UNIX)
  # not important for Win, may be useful only for Unixes
  set( LFA_SRC ${CMAKE_SOURCE_DIR}/util/private/lfalloc/lf_allocX64.cpp )
else()
  set( LFA_SRC )
endif()

add_library( ytext-arcadia-util STATIC
  ${SRCS}
  ${G_SRCS}
  ${D_SRCS}
  ${S_SRCS}
  ${M_SRCS}
  ${ST_SRCS}
  ${CF_SRCS}
  ${DU_SRCS}
  ${DJ_SRCS}
  ${NW_SRCS}
  ${FR_SRCS}
  ${DT_SRCS}
  ${TR_SRCS}
  ${SR_SRCS}
  ${GR_SRCS}
  ${RN_SRCS}
  ${SV_SRCS}
  ${UT_SRCS}
  ${LFA_SRC}
)

if (CMAKE_COMPILER_IS_GNUCXX)
  target_link_libraries ( ytext-arcadia-util
    -ldl
    -lrt
  )
endif()

if (YT_BUILD_WITH_STLPORT)
  target_link_libraries( ytext-arcadia-util stlport )
  if (CMAKE_COMPILER_IS_GNUCXX)
    set_target_properties( ytext-arcadia-util PROPERTIES LINK_FLAGS "-nodefaultlibs -L${CMAKE_BINARY_DIR}/lib" )
  endif()
endif()

include_directories( ${CMAKE_SOURCE_DIR} )

if (CMAKE_COMPILER_IS_GNUCXX)
  set_target_properties( ytext-arcadia-util PROPERTIES COMPILE_FLAGS "-fPIC" )
endif()

if (APPLE)
  set_target_properties( ytext-arcadia-util PROPERTIES COMPILE_DEFINITIONS "_REENTRANT;_XOPEN_SOURCE" )
endif()
