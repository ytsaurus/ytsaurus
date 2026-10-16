#pragma once

#if defined(__linux__)
#   define _linux_
#elif defined(_WIN64)
#   define _win64_
#   define _win32_
#elif defined(__WIN32__) || defined(_WIN32)
#   define _win32_
#elif defined(__APPLE__)
#   define _darwin_
#endif

#if defined(_win32_) || defined(_win64_)
#   define _win_
#   if !defined(_WIN32_WINNT) || (_WIN32_WINNT < 0x0600)
#      undef _WIN32_WINNT
#      define _WIN32_WINNT 0x0600 // The WSAPoll function is defined on Windows Vista and later.
#   endif
#   define WIN32_LEAN_AND_MEAN 1  // don't include too much header automatically
#endif

#if defined(_linux_) || defined (_darwin_)
#   define _unix_
#endif

#if defined(_MSC_VER)
#   undef NOMINMAX
#   define NOMINMAX
#   include <basetsd.h>
#   define ssize_t SSIZE_T
#   define HAVE_SSIZE_T 1
#endif
