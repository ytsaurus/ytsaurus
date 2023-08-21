#pragma once

#include "public.h"

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/yson/writer.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

NYson::EYsonFormat GetBaseYsonFormat(EExtendedYsonFormat format);

bool IsUnescapedYsonFormat(EExtendedYsonFormat format);

////////////////////////////////////////////////////////////////////////////////

//! YSON-parser with support of CHYT-specific yson formats.
class TExtendedYsonWriter
    : public NYson::TYsonWriter
{
public:
    TExtendedYsonWriter(
        IOutputStream* stream,
        EExtendedYsonFormat format = EExtendedYsonFormat::Binary,
        NYson::EYsonType type = NYson::EYsonType::Node,
        bool enableRaw = false,
        int indent = DefaultIndent);

    void OnStringScalar(TStringBuf value) override;

protected:
    bool Unescaped_;
    TString Buffer_;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
NYson::TYsonString ConvertToYsonStringExtendedFormat(const T& value, EExtendedYsonFormat format);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

#define UNESCAPED_YSON_INL_H_
#include "unescaped_yson-inl.h"
#undef UNESCAPED_YSON_INL_H_
