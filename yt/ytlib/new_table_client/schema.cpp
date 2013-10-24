#include "stdafx.h"
#include "schema.h"

#include <ytlib/new_table_client/chunk_meta.pb.h>

#include <core/ytree/convert.h>
#include <core/ytree/node.h>

#include <core/misc/protobuf_helpers.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NYTree;
using namespace NYson;
using namespace  NProto;

////////////////////////////////////////////////////////////////////////////////

TNullable<EColumnType> FindColumnType(
    const TStringBuf& columnName,
    const TTableSchemaExt& schema)
{
    for (const auto& column: schema.columns()) {
        if (column.name() == columnName) {
            return MakeNullable(EColumnType(column.type()));
        }
    }
    return Null;
}

TNullable<int> FindColumnIndex(
    const TStringBuf& columnName,
    const TTableSchemaExt& schema)
{
    for (int index = 0; index < schema.columns_size(); ++index) {
        if (schema.columns(index).name() == columnName) {
            return MakeNullable(index);
        }
    }
    return Null;
}

int GetColumnIndex(
    const TStringBuf& columnName,
    const TTableSchemaExt& schema)
{
    auto index = FindColumnIndex(columnName, schema);
    if (!index) {
        THROW_ERROR_EXCEPTION("Column \"%s\" not found in schema", columnName);
    }
    return *index;
}

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

//void Serialize(const TColumnSchema& columnSchema, NYson::IYsonConsumer* consumer);

void Deserialize(TColumnSchema& columnSchema, INodePtr node)
{
    columnSchema.set_name(ConvertTo<Stroka>(node->AsMap()->GetChild("name")));
    columnSchema.set_type(ConvertTo<EColumnType>(node->AsMap()->GetChild("type")));
}

//void Serialize(const TTableSchema& tableSchema, NYson::IYsonConsumer* consumer);

void Deserialize(TTableSchemaExt& tableSchema, INodePtr node)
{
    for (const auto& column: ConvertTo<std::vector<TColumnSchema>>(node)) {
        *tableSchema.add_columns() = column;
    }
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
