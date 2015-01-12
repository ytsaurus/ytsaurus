#pragma once

#include <algorithm>
#include <string>
#include <map>
#include <set>
#include <vector>
#include <list>
#include <utility>
#include <tuple>

#include <util/system/atomic.h>
#include <util/system/defaults.h>
#include <util/system/spinlock.h>
#include <util/system/mutex.h>
#include <util/system/condvar.h>
#include <util/system/event.h>
#include <util/system/thread.h>

#include <util/generic/list.h>
#include <util/generic/deque.h>
#include <util/generic/utility.h>
#include <util/generic/stroka.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/singleton.h>
#include <util/generic/pair.h>

#include <util/datetime/base.h>

#include <util/string/printf.h>
#include <util/string/cast.h>
#include <util/string/split.h>

// This define enables tracking of reference-counted objects to provide
// various insightful information on memory usage and object creation patterns.
#define ENABLE_REF_COUNTED_TRACKING

// This define causes printing enormous amount of debugging information to stderr.
// Use when you really have to.
#undef ENABLE_REF_COUNTED_DEBUGGING

#ifndef NDEBUG
    // This define enables thread affinity check -- a user-defined verification ensuring
    // that some functions are called from particular threads.
    #define ENABLE_THREAD_AFFINITY_CHECK

    // This define enables logging with TRACE level.
    #define ENABLE_TRACE_LOGGING
#endif

// This define enables tracking of bind location
#define ENABLE_BIND_LOCATION_TRACKING

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

// A temporary workaround until we switch to a fresh VS version.
#if defined(_MSC_VER) && (_MSC_VER < 1700)
namespace std {
    using ::std::tr1::tuple;
    using ::std::tr1::tie;
    using ::std::tr1::get;
} // namespace std
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

#include "enum.h"
#include "assert.h"
#include "intrusive_ptr.h"
#include "weak_ptr.h"
#include "ref_counted.h"
#include "new.h"
#include "arcadia_helper.h"
#include "hash.h"
#include "foreach.h"

namespace NYT {

using ::ToString;

} // namespace NYT

// Forward declrations.
namespace std {

template <class T, class A>
class list;

template <class T, class A>
class vector;

template <class T, class C, class A>
class set;

template <class T, class C, class A>
class multiset;

template <class T, class H, class P, class A>
class unordered_set;

template <class T, class H, class P, class A>
class unordered_multiset;

template <class K, class T, class C, class A>
class map;

template <class K, class T, class C, class A>
class multimap;

template <class K,class T, class H, class P, class A>
class unordered_map;

template <class K,class T, class H, class P, class A>
class unordered_multimap;

} // namespace std
