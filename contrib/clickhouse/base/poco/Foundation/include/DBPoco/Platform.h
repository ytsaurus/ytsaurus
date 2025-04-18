//
// Platform.h
//
// Library: Foundation
// Package: Core
// Module:  Platform
//
// Platform and architecture identification macros.
//
// NOTE: This file may be included from both C++ and C code, so it
//       must not contain any C++ specific things.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_Platform_INCLUDED
#define DB_Foundation_Platform_INCLUDED


//
// Platform Identification
//
#define DB_POCO_OS_FREE_BSD 0x0001
#define DB_POCO_OS_AIX 0x0002
#define DB_POCO_OS_HPUX 0x0003
#define DB_POCO_OS_TRU64 0x0004
#define DB_POCO_OS_LINUX 0x0005
#define DB_POCO_OS_MAC_OS_X 0x0006
#define DB_POCO_OS_NET_BSD 0x0007
#define DB_POCO_OS_OPEN_BSD 0x0008
#define DB_POCO_OS_IRIX 0x0009
#define DB_POCO_OS_SOLARIS 0x000a
#define DB_POCO_OS_QNX 0x000b
#define DB_POCO_OS_VXWORKS 0x000c
#define DB_POCO_OS_CYGWIN 0x000d
#define DB_POCO_OS_NACL 0x000e
#define DB_POCO_OS_ANDROID 0x000f
#define DB_POCO_OS_UNKNOWN_UNIX 0x00ff
#define DB_POCO_OS_WINDOWS_NT 0x1001
#define DB_POCO_OS_WINDOWS_CE 0x1011
#define DB_POCO_OS_VMS 0x2001


#if defined(__FreeBSD__) || defined(__FreeBSD_kernel__)
#    define DB_POCO_OS_FAMILY_UNIX 1
#    define DB_POCO_OS_FAMILY_BSD 1
#    define DB_POCO_OS DB_POCO_OS_FREE_BSD
#elif defined(_AIX) || defined(__TOS_AIX__)
#    define DB_POCO_OS_FAMILY_UNIX 1
#    define DB_POCO_OS DB_POCO_OS_AIX
#elif defined(hpux) || defined(_hpux) || defined(__hpux)
#    define DB_POCO_OS_FAMILY_UNIX 1
#    define DB_POCO_OS DB_POCO_OS_HPUX
#elif defined(__digital__) || defined(__osf__)
#    define DB_POCO_OS_FAMILY_UNIX 1
#    define DB_POCO_OS DB_POCO_OS_TRU64
#elif defined(__NACL__)
#    define DB_POCO_OS_FAMILY_UNIX 1
#    define DB_POCO_OS DB_POCO_OS_NACL
#elif defined(linux) || defined(__linux) || defined(__linux__) || defined(__TOS_LINUX__) || defined(EMSCRIPTEN)
#    define DB_POCO_OS_FAMILY_UNIX 1
#    if defined(__ANDROID__)
#        define DB_POCO_OS DB_POCO_OS_ANDROID
#    else
#        define DB_POCO_OS DB_POCO_OS_LINUX
#    endif
#elif defined(__APPLE__) || defined(__TOS_MACOS__)
#    define DB_POCO_OS_FAMILY_UNIX 1
#    define DB_POCO_OS_FAMILY_BSD 1
#    define DB_POCO_OS DB_POCO_OS_MAC_OS_X
#elif defined(__NetBSD__)
#    define DB_POCO_OS_FAMILY_UNIX 1
#    define DB_POCO_OS_FAMILY_BSD 1
#    define DB_POCO_OS DB_POCO_OS_NET_BSD
#elif defined(__OpenBSD__)
#    define DB_POCO_OS_FAMILY_UNIX 1
#    define DB_POCO_OS_FAMILY_BSD 1
#    define DB_POCO_OS DB_POCO_OS_OPEN_BSD
#elif defined(sgi) || defined(__sgi)
#    define DB_POCO_OS_FAMILY_UNIX 1
#    define DB_POCO_OS DB_POCO_OS_IRIX
#elif defined(sun) || defined(__sun)
#    define DB_POCO_OS_FAMILY_UNIX 1
#    define DB_POCO_OS DB_POCO_OS_SOLARIS
#elif defined(unix) || defined(__unix) || defined(__unix__)
#    define DB_POCO_OS_FAMILY_UNIX 1
#    define DB_POCO_OS DB_POCO_OS_UNKNOWN_UNIX
#elif defined(_WIN32) || defined(_WIN64)
#    define DB_POCO_OS_FAMILY_WINDOWS 1
#    define DB_POCO_OS DB_POCO_OS_WINDOWS_NT
#endif


#if !defined(DB_POCO_OS)
#    error "Unknown Platform."
#endif


//
// Hardware Architecture and Byte Order
//
#define DB_POCO_ARCH_ALPHA 0x01
#define DB_POCO_ARCH_IA32 0x02
#define DB_POCO_ARCH_IA64 0x03
#define DB_POCO_ARCH_MIPS 0x04
#define DB_POCO_ARCH_HPPA 0x05
#define DB_POCO_ARCH_PPC 0x06
#define DB_POCO_ARCH_POWER 0x07
#define DB_POCO_ARCH_SPARC 0x08
#define DB_POCO_ARCH_AMD64 0x09
#define DB_POCO_ARCH_ARM 0x0a
#define DB_POCO_ARCH_M68K 0x0b
#define DB_POCO_ARCH_S390 0x0c
#define DB_POCO_ARCH_SH 0x0d
#define DB_POCO_ARCH_NIOS2 0x0e
#define DB_POCO_ARCH_AARCH64 0x0f
#define DB_POCO_ARCH_ARM64 0x0f // same as DB_POCO_ARCH_AARCH64
#define DB_POCO_ARCH_RISCV64 0x10
#define DB_POCO_ARCH_LOONGARCH64 0x12


#if defined(__ALPHA) || defined(__alpha) || defined(__alpha__) || defined(_M_ALPHA)
#    define DB_POCO_ARCH DB_POCO_ARCH_ALPHA
#    define DB_POCO_ARCH_LITTLE_ENDIAN 1
#elif defined(i386) || defined(__i386) || defined(__i386__) || defined(_M_IX86) || defined(EMSCRIPTEN)
#    define DB_POCO_ARCH DB_POCO_ARCH_IA32
#    define DB_POCO_ARCH_LITTLE_ENDIAN 1
#elif defined(_IA64) || defined(__IA64__) || defined(__ia64__) || defined(__ia64) || defined(_M_IA64)
#    define DB_POCO_ARCH DB_POCO_ARCH_IA64
#    if defined(hpux) || defined(_hpux)
#        define DB_POCO_ARCH_BIG_ENDIAN 1
#    else
#        define DB_POCO_ARCH_LITTLE_ENDIAN 1
#    endif
#elif defined(__x86_64__) || defined(_M_X64)
#    define DB_POCO_ARCH DB_POCO_ARCH_AMD64
#    define DB_POCO_ARCH_LITTLE_ENDIAN 1
#elif defined(__mips__) || defined(__mips) || defined(__MIPS__) || defined(_M_MRX000)
#    define DB_POCO_ARCH DB_POCO_ARCH_MIPS
#    if   defined(__MIPSEB__) || defined(_MIPSEB) || defined(__MIPSEB)
#        define DB_POCO_ARCH_BIG_ENDIAN 1
#    elif defined(__MIPSEL__) || defined(_MIPSEL) || defined(__MIPSEL)
#        define DB_POCO_ARCH_LITTLE_ENDIAN 1
#    else
#        error "MIPS but neither MIPSEL nor MIPSEB?"
#    endif
#elif defined(__hppa) || defined(__hppa__)
#    define DB_POCO_ARCH DB_POCO_ARCH_HPPA
#    define DB_POCO_ARCH_BIG_ENDIAN 1
#elif defined(__PPC) || defined(__POWERPC__) || defined(__powerpc) || defined(__PPC__) || defined(__powerpc__) || defined(__ppc__) \
    || defined(__ppc) || defined(_ARCH_PPC) || defined(_M_PPC)
#    define DB_POCO_ARCH DB_POCO_ARCH_PPC
#    if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
#        define DB_POCO_ARCH_LITTLE_ENDIAN 1
#    else
#        define DB_POCO_ARCH_BIG_ENDIAN 1
#    endif
#elif defined(_POWER) || defined(_ARCH_PWR) || defined(_ARCH_PWR2) || defined(_ARCH_PWR3) || defined(_ARCH_PWR4) || defined(__THW_RS6000)
#    define DB_POCO_ARCH DB_POCO_ARCH_POWER
#    define DB_POCO_ARCH_BIG_ENDIAN 1
#elif defined(__sparc__) || defined(__sparc) || defined(sparc)
#    define DB_POCO_ARCH DB_POCO_ARCH_SPARC
#    define DB_POCO_ARCH_BIG_ENDIAN 1
#elif defined(__arm__) || defined(__arm) || defined(ARM) || defined(_ARM_) || defined(__ARM__) || defined(_M_ARM)
#    define DB_POCO_ARCH DB_POCO_ARCH_ARM
#    if defined(__ARMEB__)
#        define DB_POCO_ARCH_BIG_ENDIAN 1
#    else
#        define DB_POCO_ARCH_LITTLE_ENDIAN 1
#    endif
#elif defined(__arm64__) || defined(__arm64)
#    define DB_POCO_ARCH DB_POCO_ARCH_ARM64
#    if defined(__ARMEB__)
#        define DB_POCO_ARCH_BIG_ENDIAN 1
#    elif defined(__BYTE_ORDER__) && defined(__ORDER_BIG_ENDIAN__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#        define DB_POCO_ARCH_BIG_ENDIAN 1
#    else
#        define DB_POCO_ARCH_LITTLE_ENDIAN 1
#    endif
#elif defined(__m68k__)
#    define DB_POCO_ARCH DB_POCO_ARCH_M68K
#    define DB_POCO_ARCH_BIG_ENDIAN 1
#elif defined(__s390__)
#    define DB_POCO_ARCH DB_POCO_ARCH_S390
#    define DB_POCO_ARCH_BIG_ENDIAN 1
#elif defined(__sh__) || defined(__sh) || defined(SHx) || defined(_SHX_)
#    define DB_POCO_ARCH DB_POCO_ARCH_SH
#    if defined(__LITTLE_ENDIAN__) || (DB_POCO_OS == DB_POCO_OS_WINDOWS_CE)
#        define DB_POCO_ARCH_LITTLE_ENDIAN 1
#    else
#        define DB_POCO_ARCH_BIG_ENDIAN 1
#    endif
#elif defined(nios2) || defined(__nios2) || defined(__nios2__)
#    define DB_POCO_ARCH DB_POCO_ARCH_NIOS2
#    if defined(__nios2_little_endian) || defined(nios2_little_endian) || defined(__nios2_little_endian__)
#        define DB_POCO_ARCH_LITTLE_ENDIAN 1
#    else
#        define DB_POCO_ARCH_BIG_ENDIAN 1
#    endif
#elif defined(__AARCH64EL__)
#    define DB_POCO_ARCH DB_POCO_ARCH_AARCH64
#    define DB_POCO_ARCH_LITTLE_ENDIAN 1
#elif defined(__AARCH64EB__)
#    define DB_POCO_ARCH DB_POCO_ARCH_AARCH64
#    define DB_POCO_ARCH_BIG_ENDIAN 1
#elif defined(__riscv) && (__riscv_xlen == 64)
#    define DB_POCO_ARCH DB_POCO_ARCH_RISCV64
#    define DB_POCO_ARCH_LITTLE_ENDIAN 1
#elif defined(__loongarch64)
#    define DB_POCO_ARCH DB_POCO_ARCH_LOONGARCH64
#    define DB_POCO_ARCH_LITTLE_ENDIAN 1
#endif


#ifdef __GNUC__
#    define DB_POCO_UNUSED __attribute__((unused))
#else
#    define DB_POCO_UNUSED
#endif // __GNUC__


#if !defined(DB_POCO_ARCH)
#    error "Unknown Hardware Architecture."
#endif


#    define DB_POCO_DEFAULT_NEWLINE_CHARS "\n"


#endif // DB_Foundation_Platform_INCLUDED
