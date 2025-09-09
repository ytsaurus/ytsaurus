#pragma once

#include <yt/yt/client/table_client/unversioned_row.h>

#include <library/cpp/yt/misc/enum.h>
#include <library/cpp/yt/misc/strong_typedef.h>

namespace NYT::NFlow::NYsonSerializer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EYsonStateValueType,
    ((Simple)             (0))
    ((Compressed)         (1))
    ((Packable)           (2))
);

inline constexpr TStringBuf StateFormatColumn = "format";

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TFormat);
DECLARE_REFCOUNTED_STRUCT(TStateSchema);
DECLARE_REFCOUNTED_CLASS(TState);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYsonSerializer
