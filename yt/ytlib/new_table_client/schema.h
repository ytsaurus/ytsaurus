#pragma once

#include "public.h"

#include <ytlib/new_table_client/chunk_meta.pb.h>

#include <core/ytree/public.h>
#include <core/yson/public.h>
#include <core/misc/nullable.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

TNullable<EColumnType> FindColumnType(
    const TStringBuf& columnName,
    const NProto::TTableSchemaExt& schema);

TNullable<int> FindColumnIndex(
    const TStringBuf& columnName,
    const NProto::TTableSchemaExt& schema);

int GetColumnIndex(
    const TStringBuf& columnName,
    const NProto::TTableSchemaExt& schema);

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void Serialize(const TColumnSchema& columnSchema, NYson::IYsonConsumer* consumer);
void Deserialize(TColumnSchema& columnSchema, NYTree::INodePtr node);

void Serialize(const TTableSchemaExt& tableSchema, NYson::IYsonConsumer* consumer);
void Deserialize(TTableSchemaExt& tableSchema, NYTree::INodePtr node);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
