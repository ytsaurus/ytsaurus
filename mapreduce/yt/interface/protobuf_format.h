#pragma once

///
/// @file mapreduce/yt/interface/protobuf_format.h
///
/// Header containing declarations of types and functions
/// to work with protobuf format flags and options

#include "common.h"

#include <mapreduce/yt/interface/protos/extension.pb.h>

#include <util/generic/maybe.h>

#include <google/protobuf/message.h>

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

enum class EProtobufListMode
{
    Optional,
    Required,
};

enum class EProtobufMapMode
{
    ListOfStructsLegacy,
    ListOfStructs,
    Dict,
    OptionalDict,
};

enum class EProtobufFieldSortOrder
{
    AsInProtoFile,
    ByFieldNumber,
};

enum class EProtobufOneofMode
{
    SeparateFields,
    Variant,
};

struct TProtobufOneofOptions
{
    EProtobufOneofMode Mode = EProtobufOneofMode::Variant;
    TString VariantFieldName;
};

struct TProtobufFieldOptions
{
    TMaybe<EProtobufType> Type;
    EProtobufSerializationMode SerializationMode = EProtobufSerializationMode::Protobuf;
    EProtobufListMode ListMode = EProtobufListMode::Required;
    EProtobufMapMode MapMode = EProtobufMapMode::ListOfStructsLegacy;
};

struct TProtobufMessageOptions
{
    EProtobufFieldSortOrder FieldSortOrder = EProtobufFieldSortOrder::ByFieldNumber;
};

TProtobufFieldOptions GetFieldOptions(
    const ::google::protobuf::FieldDescriptor* fieldDescriptor,
    const TMaybe<TProtobufFieldOptions>& defaultFieldOptions = {});

TProtobufOneofOptions GetOneofOptions(
    const ::google::protobuf::OneofDescriptor* oneofDescriptor,
    const TMaybe<TProtobufOneofOptions>& defaultOneofOptions = {});

TProtobufMessageOptions GetMessageOptions(const ::google::protobuf::Descriptor* descriptor);

TMaybe<TVector<TString>> InferColumnFilter(const ::google::protobuf::Descriptor& descriptor);

TNode MakeProtoFormatConfig(const TVector<const ::google::protobuf::Descriptor*>& descriptors);

TTableSchema CreateTableSchemaImpl(
    const ::google::protobuf::Descriptor& messageDescriptor,
    bool keepFieldsWithoutExtension);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail

