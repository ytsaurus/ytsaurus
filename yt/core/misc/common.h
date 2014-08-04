#pragma once

#include <map>
#include <set>
#include <vector>
#include <list>
#include <queue>

#include <util/system/atomic.h>
#include <util/system/defaults.h>
#include <util/system/spinlock.h>

#include <util/generic/stroka.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/singleton.h>

#include <util/datetime/base.h>

#include <util/string/printf.h>
#include <util/string/cast.h>

// Check platform bitness.
#if !defined(__x86_64__) && !defined(_M_X64)
    #error YT requires 64-bit platform
#endif

// This define enables tracking of reference-counted objects to provide
// various insightful information on memory usage and object creation patterns.
#define YT_ENABLE_REF_COUNTED_TRACKING

// This define causes printing enormous amount of debugging information to stderr.
// Use when you really have to.
#undef YT_ENABLE_REF_COUNTED_DEBUGGING

#ifndef NDEBUG
    // This define enables thread affinity check -- a user-defined verification ensuring
    // that some functions are called from particular threads.
    #define YT_ENABLE_THREAD_AFFINITY_CHECK

    // This define enables logging with TRACE level.
    #define YT_ENABLE_TRACE_LOGGING

    // This define enables tracking of bind location
    #define YT_ENABLE_BIND_LOCATION_TRACKING
#endif

// Configure SSE usage.
#ifdef __SSE4_2__
    #define YT_USE_SSE42
    #ifndef __APPLE__
        #define YT_USE_CRC_PCLMUL
    #endif
#endif

#ifdef _win_
    // Someone above has defined this by including one of Windows headers.
    #undef GetMessage
    #undef Yield

    // For protobuf-generated files:
    // C4125: decimal digit terminates octal escape sequence
    #pragma warning (disable: 4125)
    // C4505: unreferenced local function has been removed
    #pragma warning (disable : 4505)
    // C4121: alignment of a member was sensitive to packing
    #pragma warning (disable: 4121)
    // C4503: decorated name length exceeded, name was truncated
    #pragma warning (disable : 4503)
    // C4714: function marked as __forceinline not inlined
    #pragma warning (disable: 4714)
    // C4250: inherits via dominance
    #pragma warning (disable: 4250)
#endif

#if defined(_MSC_VER)
    // VS does not support alignof natively yet.
    #define alignof __alignof
#endif

// Used to mark Logger and Profiler static variables as probably unused
// to silent static analyzer.
#if defined(__GNUC__) || defined(__clang__)
    #define SILENT_UNUSED __attribute__((unused))
#else
    #define SILENT_UNUSED
#endif

#ifdef _unix_
    #define TLS_STATIC static __thread
#else
    #define TLS_STATIC static __declspec(thread)
#endif

namespace std {

#ifdef __GNUC__

// As of now, GCC does not support make_unique.
// See https://gcc.gnu.org/ml/libstdc++/2014-06/msg00010.html
template <typename TResult, typename ...TArgs>
std::unique_ptr<TResult> make_unique(TArgs&& ...args)
{
    return std::unique_ptr<TResult>(new TResult(std::forward<TArgs>(args)...));
}

// As of now, GCC does not have std::aligned_union.
template <typename... _Types>
  struct __strictest_alignment
  {
    static const size_t _S_alignment = 0;
    static const size_t _S_size = 0;
  };

template <typename _Tp, typename... _Types>
  struct __strictest_alignment<_Tp, _Types...>
  {
    static const size_t _S_alignment =
      alignof(_Tp) > __strictest_alignment<_Types...>::_S_alignment
 ? alignof(_Tp) : __strictest_alignment<_Types...>::_S_alignment;
    static const size_t _S_size =
      sizeof(_Tp) > __strictest_alignment<_Types...>::_S_size
 ? sizeof(_Tp) : __strictest_alignment<_Types...>::_S_size;
  };

template <size_t _Len, typename... _Types>
  struct aligned_union
  {
  private:
    static_assert(sizeof...(_Types) != 0, "At least one type is required");

    using __strictest = __strictest_alignment<_Types...>;
    static const size_t _S_len = _Len > __strictest::_S_size
 ? _Len : __strictest::_S_size;
  public:
    /// The value of the strictest alignment of _Types.
    static const size_t alignment_value = __strictest::_S_alignment;
    /// The storage.
    typedef typename aligned_storage<_S_len, alignment_value>::type type;
  };

template <size_t _Len, typename... _Types>
  const size_t aligned_union<_Len, _Types...>::alignment_value;

#endif

#if defined(__GNUC__) && __GNUC__ == 4 && __GNUC_MINOR__ == 7
// GCC 4.7 defines has_trivial_destructor instead of is_trivially_destructible.
template<typename T>
using is_trivially_destructible = std::has_trivial_destructor<T>;
#endif

} // namespace std

#include "enum.h"
#include "assert.h"
#include "intrusive_ptr.h"
#include "weak_ptr.h"
#include "ref_counted.h"
#include "new.h"
#include "hash.h"

namespace NYT {

using ::ToString;

} // namespace NYT
