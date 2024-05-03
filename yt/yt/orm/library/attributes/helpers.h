#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>
#include <yt/yt/core/yson/public.h>
#include <yt/yt/core/ytree/public.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

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

void ReduceErrors(TError& base, TError incoming, EErrorCode mismatchErrorCode);

std::partial_ordering CompareScalarFields(
    const NProtoBuf::Message* lhsMessage,
    const NProtoBuf::FieldDescriptor* lhsFieldDescriptor,
    const NProtoBuf::Message* rhsMessage,
    const NProtoBuf::FieldDescriptor* rhsFieldDescriptor);

std::partial_ordering CompareRepeatedFieldEntries(
    const NProtoBuf::Message* lhsMessage,
    const NProtoBuf::FieldDescriptor* lhsFieldDescriptor,
    int lhsIndex,
    const NProtoBuf::Message* rhsMessage,
    const NProtoBuf::FieldDescriptor* rhsFieldDescriptor,
    int rhsIndex);

TErrorOr<int> LocateMapEntry(
    const NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    const NProtoBuf::Message* keyMessage);

TErrorOr<TString> MapKeyFieldToString(
    const NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* keyFieldDescriptor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
