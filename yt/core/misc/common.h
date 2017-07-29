#pragma once

#include <util/datetime/base.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/singleton.h>
#include <util/generic/string.h>

#include <util/string/cast.h>
#include <util/string/printf.h>

#include <util/system/atomic.h>
#include <util/system/compiler.h>
#include <util/system/defaults.h>
#include <util/system/spinlock.h>

#include <list>
#include <map>
#include <queue>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <type_traits>

namespace std {

// Assume sane platform by default.
#define YT_PLATFORM_HAS_MAKE_UNIQUE
#define YT_PLATFORM_HAS_ALIGNED_UNION
#define YT_PLATFORM_HAS_IS_TRIVIALLY_CONSTRUCTIBLE

/*
 * Ubuntu Precise Feature Compatibility.
 *
 * GCC                  |  4.8.1-2ubuntu1  |  4.9.2-0ubuntu1
 * __GLIBCXX__          |  20130604        |  20141104
 * ---------------------+------------------+-----------------
 * make_unique          |                  |  ok
 * aligned_union        |                  |
 * is_trivially_*_ctor  |                  |
 *
 */

/*
 * Ubuntu Trusty Feature Compatibility.
 *
 * GCC                  |  4.8.4-2ubuntu1  |  4.9.3-8ubuntu2  |  5.3.0-3ubuntu1
 * __GLIBCXX__          |  20150426        |  20151129        |  20151204
 * ---------------------+------------------+------------------+-----------------
 * make_unique          |                  |  ok              |  ok
 * aligned_union        |                  |                  |  ok
 * is_trivially_*_ctor  |                  |                  |  ok
 *
 */

/*
 * Other GCC Versions
 *
 * GCC                  | 4.9.4
 * __GLIBCXX__          | 20160726
 * ---------------------+---------
 * make_unique          |  ok
 * aligned_union        |
 * is_trivially_*_ctor  |                
 */
 
#if defined(__GLIBCXX__) && __GLIBCXX__ > 20140716 && !defined(__cpp_lib_make_unique)
#undef YT_PLATFORM_HAS_MAKE_UNIQUE
#endif

#if defined(__GLIBCXX__) && (__GLIBCXX__ < 20151204 || __GLIBCXX__ == 20160726)
#undef YT_PLATFORM_HAS_ALIGNED_UNION
#endif

#if defined(__GLIBCXX__) && (__GLIBCXX__ < 20151204 || __GLIBCXX__ == 20160726)
#undef YT_PLATFORM_HAS_IS_TRIVIALLY_CONSTRUCTIBLE
#endif

////////////////////////////////////////////////////////////////////////////////
#ifndef YT_PLATFORM_HAS_MAKE_UNIQUE

template <typename TResult, typename ...TArgs>
std::unique_ptr<TResult> make_unique(TArgs&& ...args)
{
    return std::unique_ptr<TResult>(new TResult(std::forward<TArgs>(args)...));
}

#endif

////////////////////////////////////////////////////////////////////////////////
#ifndef YT_PLATFORM_HAS_ALIGNED_UNION

// GCC 4.x does not have std::aligned_union.
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

////////////////////////////////////////////////////////////////////////////////
#ifndef YT_PLATFORM_HAS_IS_TRIVIALLY_CONSTRUCTIBLE

template <typename T>
using is_trivially_copy_constructible = is_trivial<T>;
template <typename T>
using is_trivially_move_constructible = is_trivial<T>;

#endif

////////////////////////////////////////////////////////////////////////////////

// Make global hash functions from util/ visible for STL.
template <> struct hash<TStringBuf> : public ::hash<TStringBuf> { };

} // namespace std

#include "port.h"
#include "enum.h"
#include "assert.h"
#include "ref_counted.h"
#include "intrusive_ptr.h"
#include "weak_ptr.h"
#include "new.h"
#include "hash.h"

namespace NYT {

using ::ToString;

} // namespace NYT
