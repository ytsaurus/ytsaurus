#pragma once

#include "preprocessor.h"

#include  <util/generic/intrlist.h>

namespace NYT {
namespace NForeach {

////////////////////////////////////////////////////////////////////////////////

inline bool SetFalse(bool& flag)
{
    flag = false;
    return false;
}

template <class T>
inline auto Begin(T& collection) -> decltype(collection.begin())
{
    return collection.begin();
}

template <class T>
inline auto End(T& collection) -> decltype(collection.end())
{
    return collection.end();
}

template <class T>
inline auto Deref(T& it) -> decltype(*it)
{
    return *it;
}

template <class T>
inline void MoveNext(T& it)
{
    ++it;
}

// Provide a specialization for TIntrusiveList, which has Begin/End instead of begin/end.
template <class T>
inline auto Begin(TIntrusiveList<T>& collection) -> decltype(collection.Begin())
{
    return collection.Begin();
}

template <class T>
inline auto End(TIntrusiveList<T>& collection) -> decltype(collection.End())
{
    return collection.End();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NForeach
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

//! Provides a compact way of iterating over a collection.
/*!
 *  This macro implements a functionality similar to that of BOOST_FOREACH.
 *  The main advantage is its relative simplicity.
 *  It is heavily dependent on C++0x features (r-value references
 *  and type deduction, to be precise) and is thus less portable.
 *  It also suffers from some known limitations (see below).
 *  
 *  Briefly speaking, the code like
 *  \code
 *  FOREACH (int var, collection)
 *  \endcode
 *  causes iterating over 'collection' and capturing the copy
 *  of every its element into 'var'. The macro calls begin() and end()
 *  methods of 'collection' to figure out the range.
 *  
 *  There are some important aspects to keep in mind:
 *  - 'collection' is evaluated only once
 *  - 'collection' is captured by a reference (unless it's a temporary)
 *  - begin- and end- iterators are also evaluated once when the loop starts
 *  - you can declare 'var' as a reference to enable mutating its value
 *  - you can use 'auto' (or 'auto&') to avoid giving the type of 'var' explicitly
 *  
 *  Known limitations:
 *  - you cannot use FOREACH as a non-compound statement in, e.g., if or loop body
 *  - you cannot apply FOREACH to C-style arrays
 *  - using FOREACH incurs a small overhead due to manipulations with the flag,
 *    hence one should avoid using at hotspots
 */
#define FOREACH(var, collection) \
    auto&& PP_CONCAT(foreach_collection_, __LINE__) = collection; \
    auto   PP_CONCAT(foreach_current_,    __LINE__) = ::NYT::NForeach::Begin(PP_CONCAT(foreach_collection_, __LINE__)); \
    auto   PP_CONCAT(foreach_end_,        __LINE__) = ::NYT::NForeach::End(  PP_CONCAT(foreach_collection_, __LINE__)); \
    for (bool \
         PP_CONCAT(foreach_continue_, __LINE__) = true; \
         PP_CONCAT(foreach_continue_, __LINE__) && \
         PP_CONCAT(foreach_current_,  __LINE__) != PP_CONCAT(foreach_end_, __LINE__); \
         PP_CONCAT(foreach_continue_, __LINE__) \
         ? ::NYT::NForeach::MoveNext(PP_CONCAT(foreach_current_, __LINE__)) \
         : (void)0) \
    if (::NYT::NForeach::SetFalse(PP_CONCAT(foreach_continue_, __LINE__))) { } else \
    for (var = ::NYT::NForeach::Deref(PP_CONCAT(foreach_current_, __LINE__)); \
         !PP_CONCAT(foreach_continue_, __LINE__); \
          PP_CONCAT(foreach_continue_, __LINE__) = true)

////////////////////////////////////////////////////////////////////////////////
