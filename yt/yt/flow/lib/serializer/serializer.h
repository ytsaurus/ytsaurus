#pragma once

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NFlow::NYsonSerializer {

////////////////////////////////////////////////////////////////////////////////

NTableClient::TLogicalTypePtr GetYsonLogicalType(const NYTree::TYsonStructPtr& ysonStruct);
NTableClient::TTableSchemaPtr GetYsonSchema(const NYTree::TYsonStructPtr& ysonStruct);

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableSchemaPtr ToTableSchema(const NTableClient::TLogicalTypePtr& logicalType);
NTableClient::TLogicalTypePtr ToLogicalType(const NTableClient::TTableSchemaPtr& tableSchema);

////////////////////////////////////////////////////////////////////////////////

NTableClient::TUnversionedOwningRow Serialize(
    const NYTree::TYsonStructPtr& ysonStruct,
    const NTableClient::TLogicalTypePtr& logicalType);

NTableClient::TUnversionedOwningRow Serialize(
    const NYTree::TYsonStructPtr& ysonStruct,
    const NTableClient::TTableSchemaPtr& schema);

void Deserialize(
    const NYTree::TYsonStructPtr& ysonStruct,
    const NTableClient::TUnversionedRow& row,
    const NTableClient::TLogicalTypePtr& logicalType);

void Deserialize(
    const NYTree::TYsonStructPtr& ysonStruct,
    const NTableClient::TUnversionedRow& row,
    const NTableClient::TTableSchemaPtr& schema);

template <class TYsonStruct>
TIntrusivePtr<TYsonStruct> Deserialize(
    const NTableClient::TUnversionedRow& row,
    const NTableClient::TLogicalTypePtr& logicalType);

template <class TYsonStruct>
TIntrusivePtr<TYsonStruct> Deserialize(
    const NTableClient::TUnversionedRow& row,
    const NTableClient::TTableSchemaPtr& schema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYsonSerializer

#define SERIALIZER_INL_H_
#include "serializer-inl.h"
#undef SERIALIZER_INL_H_
