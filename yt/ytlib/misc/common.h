#pragma once

#include <algorithm>
#include <string>
#include <map>
#include <set>
#include <vector>
#include <list>

// TODO: try to get rid of this
using std::string; // hack for guid.h to work

#include <util/system/atomic.h>
#include <util/system/defaults.h>
#include <util/system/spinlock.h>
#include <util/system/mutex.h>
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
#include <util/generic/yexception.h>
#include <util/generic/pair.h>

#include <util/datetime/base.h>

#include <util/string/printf.h>
#include <util/string/cast.h>
#include <util/string/split.h>

// TODO(babenko): get rid of this
#include <util/autoarray.h>

// This define enables tracking of reference-counted objects to provide
// various insightful information on memory usage and object creation patterns.
#define ENABLE_REF_COUNTED_TRACKING

// This define enormous amount of debugging information on stderr.
// Use when you really have to.
#define ENABLE_REF_COUNTED_DEBUGGING

// This define enables thread affinity check -- a user-defined verification ensuring
// that some functions are called from particular threads.
#ifndef NDEBUG
#define ENABLE_THREAD_AFFINITY_CHECK
#endif

#ifdef _MSC_VER
    // Someone above has defined this by including one of Windows headers.
    #undef GetMessage

    // For protobuf-generated files:
    // C4125: decimal digit terminates octal escape sequence
    #pragma warning (disable : 4125)
    // C4505: unreferenced local function has been removed
    #pragma warning (disable : 4505)
    // C4121: alignment of a member was sensitive to packing
    #pragma warning (disable : 4121)
    // C4503: decorated name length exceeded, name was truncated
    #pragma warning (disable : 4503)
    // C4714: function marked as __forceinline not inlined
    #pragma warning (disable: 4714)
    // C4250: inherits via dominance
    #pragma warning (disable: 4250)
#endif

#include "assert.h"
#include "rvalue.h"
#include "intrusive_ptr.h"
#include "weak_ptr.h"
#include "ref_counted.h"
#include "new.h"
#include "arcadia_helper.h"
#include "hash.h"
