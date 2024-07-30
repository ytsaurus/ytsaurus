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

std::partial_ordering CompareScalarRepeatedFieldEntries(
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

TError SetScalarField(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    const NYTree::INodePtr& value);

TError SetScalarRepeatedFieldEntry(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    int index,
    const NYTree::INodePtr& value);

TError AddScalarRepeatedFieldEntry(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    const NYTree::INodePtr& value);

std::pair<int, TError> FindAttributeDictionaryEntry(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    const TString& key);

TErrorOr<NYson::TYsonString> GetAttributeDictionaryEntryValue(const NProtoBuf::Message* entry);

TError SetAttributeDictionaryEntryValue(
    NProtoBuf::Message* entry,
    const NYson::TYsonString& value);

TErrorOr<NProtoBuf::Message*> AddAttributeDictionaryEntry(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    const TString& key);

////////////////////////////////////////////////////////////////////////////////

// These methods do two things:
// - Wrap scalar and repeated proto field modifications in a perfectly consistent manner, so they
//   can be driven from a, say, variant visit with an auto lambda.
// - Establish the policy for type conversions. It's mostly obvious; floats are not cast to ints,
//   bools are bools and enums are best effort from ints or strings.
TError SetScalarField(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    i64 value);
TError SetScalarField(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    ui64 value);
TError SetScalarField(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    double value);
TError SetScalarField(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    bool value);
TError SetScalarField(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    TString value);
TError SetDefaultScalarFieldValue(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor);

TError SetScalarRepeatedFieldEntry(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    int index,
    i64 value);
TError SetScalarRepeatedFieldEntry(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    int index,
    ui64 value);
TError SetScalarRepeatedFieldEntry(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    int index,
    double value);
TError SetScalarRepeatedFieldEntry(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    int index,
    bool value);
TError SetScalarRepeatedFieldEntry(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    int index,
    TString value);
TError SetDefaultScalarRepeatedFieldEntryValue(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    int index);

TError AddScalarRepeatedFieldEntry(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    i64 value);
TError AddScalarRepeatedFieldEntry(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    ui64 value);
TError AddScalarRepeatedFieldEntry(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    double value);
TError AddScalarRepeatedFieldEntry(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    bool value);
TError AddScalarRepeatedFieldEntry(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    TString value);
TError AddDefaultScalarFieldEntryValue(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor);

} // namespace NYT::NOrm::NAttributes
