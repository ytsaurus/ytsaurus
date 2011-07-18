#pragma once

#include <util/system/defaults.h>
#include <util/system/mutex.h>
#include <util/system/event.h>

#include <util/generic/utility.h>
#include <util/generic/stroka.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <util/string/printf.h>

#include <string>

using std::string; // hack for guid.h to work
#include <quality/Misc/Guid.h>

#ifdef _MSC_VER
    // C4505: unreferenced local function has been removed
    #pragma warning (disable : 4505)
    // C4121: alignment of a member was sensitive to packing
    #pragma warning (disable : 4121)
    // C4503: decorated name length exceeded, name was truncated
    #pragma warning (disable : 4503)
#endif

// This define enabled tracking of reference-counted objects to provide
// various insightful information on memory usage and object creation patterns.
#define ENABLE_REF_COUNTED_TRACKING

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TFixedChunkFooter
{
    ui64 HeaderOffset;
    ui32 HeaderSize;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT


