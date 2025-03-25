#pragma once

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TSerializer
{
    static NTableClient::TLogicalTypePtr GetSchema(const NYTree::TYsonStructPtr& ysonStruct);

    static NTableClient::TUnversionedOwningRow Serialize(
        const NYTree::TYsonStructPtr& ysonStruct,
        const NTableClient::TLogicalTypePtr& schema);

    static void Deserialize(
        const NYTree::TYsonStructPtr& ysonStruct,
        const NTableClient::TUnversionedRow& row,
        const NTableClient::TLogicalTypePtr& schema);
};

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableSchemaPtr ToTableSchema(const NTableClient::TLogicalTypePtr& logicalType);
NTableClient::TLogicalTypePtr ToLogicalType(const NTableClient::TTableSchemaPtr& tableSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
