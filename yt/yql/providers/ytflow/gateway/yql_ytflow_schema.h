#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include <yt/yt/client/table_client/schema.h>


namespace NYql::NYtflow {

NYT::NTableClient::TTableSchemaPtr BuildTableSchema(const TTypeAnnotationNode* type);

NYT::NTableClient::TTableSchemaPtr ConvertToQueueWriteSchema(
    NYT::NTableClient::TTableSchemaPtr tableSchema);

NYT::NTableClient::TTableSchemaPtr ConvertToQueueCreateSchema(
    NYT::NTableClient::TTableSchemaPtr tableSchema);

NYT::NTableClient::TTableSchemaPtr ConvertToSortedTableCreateSchema(
    NYT::NTableClient::TTableSchemaPtr tableSchema,
    const TVector<TString>& keyColumns);

} // namespace NYql::NYtflow
