#pragma once

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NFlow::NYsonSerializer {

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableSchemaPtr GetYsonTableSchema(const NYTree::TYsonStructPtr& ysonStruct);

template <class T>
NTableClient::TTableSchemaPtr GetYsonTableSchema();

////////////////////////////////////////////////////////////////////////////////

NTableClient::TUnversionedOwningRow Serialize(const NYTree::TYsonStructPtr& ysonStruct, const NTableClient::TTableSchemaPtr& schema);

void Deserialize(const NYTree::TYsonStructPtr& ysonStruct, const NTableClient::TUnversionedRow& row, const NTableClient::TTableSchemaPtr& schema);

template <class T>
TIntrusivePtr<T> Deserialize(const NTableClient::TUnversionedRow& row, const NTableClient::TTableSchemaPtr& schema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYsonSerializer

#define SERIALIZER_INL_H_
#include "serializer-inl.h"
#undef SERIALIZER_INL_H_
