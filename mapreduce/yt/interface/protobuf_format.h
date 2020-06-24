#pragma once

///
/// @file mapreduce/yt/interface/protobuf_format.h
///
/// Header containing declarations of types and functions
/// to work with protobuf format flags and options

#include "common.h"

#include <mapreduce/yt/interface/protos/extension.pb.h>

#include <util/generic/maybe.h>

#include <contrib/libs/protobuf/message.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

enum class EProtobufType
{
    EnumInt       /* "enum_int" */,
    EnumString    /* "enum_string" */,
    Any           /* "any" */,
    OtherColumns  /* "other_columns" */,
};

enum class EProtobufSerializationMode
{
    Protobuf,
    Yt,
};

enum class EListMode
{
    Optional,
    Required,
};

enum class EFieldSortOrder
{
    AsInProtoFile,
    ByFieldNumber,
};

struct TProtobufFieldOptions
{
    TMaybe<EProtobufType> Type;
    EProtobufSerializationMode SerializationMode = EProtobufSerializationMode::Protobuf;
    EListMode ListMode = EListMode::Required;
};

struct TProtobufMessageOptions
{
    EFieldSortOrder FieldSortOrder = EFieldSortOrder::ByFieldNumber;
};

void ParseProtobufFieldOptions(
    const ::google::protobuf::RepeatedField<EWrapperFieldFlag::Enum>& flags,
    TProtobufFieldOptions* fieldOptions);

void ParseProtobufMessageOptions(
    const ::google::protobuf::RepeatedField<EWrapperMessageFlag::Enum>& flags,
    TProtobufMessageOptions* messageOptions);

TMaybe<TVector<TString>> InferColumnFilter(const ::google::protobuf::Descriptor& descriptor);

TNode MakeProtoFormatConfig(const TVector<const ::google::protobuf::Descriptor*>& descriptors);

TTableSchema CreateTableSchemaImpl(
    const ::google::protobuf::Descriptor& messageDescriptor,
    bool keepFieldsWithoutExtension);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail

