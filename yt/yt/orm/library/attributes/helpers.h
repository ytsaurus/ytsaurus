#pragma once

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

const NYson::TProtobufMessageType* GetMessageTypeByYPath(
    const NYson::TProtobufMessageType* rootType,
    const NYPath::TYPath& path,
    bool allowAttributeDictionary);

NYTree::INodePtr ConvertProtobufToNode(
    const NYson::TProtobufMessageType* rootType,
    const NYPath::TYPath& path,
    const TString& payload);

////////////////////////////////////////////////////////////////////////////////

class TYsonStringWriterHelper
{
public:
    explicit TYsonStringWriterHelper(
        NYson::EYsonFormat format = NYson::EYsonFormat::Binary,
        NYson::EYsonType type = NYson::EYsonType::Node);

    NYson::IYsonConsumer* GetConsumer();
    NYson::TYsonString Flush();
    bool IsEmpty() const;

private:
    TString ValueString_;
    TStringOutput Output_;
    const std::unique_ptr<NYson::IFlushableYsonConsumer> Writer_;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NYson::IYsonConsumer> CreateAttributesDetectingConsumer(std::function<void()> reporter);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EListIndexType,
    (Absolute)
    (Relative)
);

struct TIndexParseResult
{
    i64 Index;
    EListIndexType IndexType;

    void EnsureIndexType(EListIndexType indexType, TStringBuf path);
    void EnsureIndexIsWithinBounds(i64 count, TStringBuf path);
    bool IsOutOfBounds(i64 count);
};

// Parses list index from 'end', 'begin', 'before:<index>', 'after:<index>' or Integer in [-count, count).
TIndexParseResult ParseListIndex(TStringBuf token, i64 count);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
