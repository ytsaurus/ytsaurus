#pragma once

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NJson {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJsonFormat,
    (Text)
    (Pretty)
);

DEFINE_ENUM(EJsonAttributesMode,
    (Always)
    (Never)
    (OnDemand)
);

class TJsonFormatConfig
    : public NYTree::TYsonSerializable
{
public:
    EJsonFormat Format;
    EJsonAttributesMode AttributesMode;
    bool Plain;
    bool EncodeUtf8;
    i64 MemoryLimit;

    std::optional<int> StringLengthLimit;

    bool Stringify;
    bool AnnotateWithTypes;

    bool SupportInfinity;
    bool StringifyNanAndInfinity;

    // Size of buffer used read out input stream in parser.
    // NB: in case of parsing long string yajl holds in memory whole string prefix and copy it on every parse call.
    // Therefore parsing long strings works faster with larger buffer.
    int BufferSize;

    //! Only works for tabular data.
    bool SkipNullValues;

    TJsonFormatConfig();
};

DEFINE_REFCOUNTED_TYPE(TJsonFormatConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJson
