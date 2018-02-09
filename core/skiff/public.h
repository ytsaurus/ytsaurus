#pragma once

#include <yt/core/misc/public.h>

namespace NYT {
namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EWireType,
    (Nothing)
    (Int64)
    (Uint64)
    (Double)
    (Boolean)
    (String32)
    (Yson32)

    (Tuple)
    (Variant8)
    (Variant16)
    (RepeatedVariant16)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSkiffSchema);

////////////////////////////////////////////////////////////////////////////////

class TSkiffValidator;

class TUncheckedSkiffParser;
class TCheckedSkiffParser;

class TUncheckedSkiffWriter;
class TCheckedSkiffWriter;

#ifdef DEBUG
using TCheckedInDebugSkiffParser = TCheckedSkiffParser;
using TCheckedInDebugSkiffWriter = TCheckedSkiffWriter;
#else
using TCheckedInDebugSkiffParser = TUncheckedSkiffParser;
using TCheckedInDebugSkiffWriter = TUncheckedSkiffWriter;
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff
} // namespace NYT
