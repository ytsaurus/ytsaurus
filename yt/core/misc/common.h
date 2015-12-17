#pragma once

#include <util/datetime/base.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/singleton.h>
#include <util/generic/stroka.h>

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

namespace std {

#if defined(YMAKE) || (defined(__GNUC__) && !defined(__clang__) && __GNUC__ == 4 && __GNUC_MINOR__ < 9)
// As of now, GCC does not support make_unique.
// See https://gcc.gnu.org/ml/libstdc++/2014-06/msg00010.html
template <typename TResult, typename ...TArgs>
std::unique_ptr<TResult> make_unique(TArgs&& ...args)
{
    return std::unique_ptr<TResult>(new TResult(std::forward<TArgs>(args)...));
}
#endif

// std::aligned_union is not available in early versions of libstdc++.
#if defined(__GLIBCXX__) && __GLIBCXX__ <= 20151129

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
