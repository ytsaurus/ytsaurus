#include "secondary_index_type_handler.h"

#include "type_handler_detail.h"
#include "client_impl.h"

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/library/query/secondary_index/schema.h>

#include <yt/yt/library/query/base/query_preparer.h>
#include <yt/yt/library/query/base/query_visitors.h>

namespace NYT::NApi::NNative {

using namespace NYson;
using namespace NYTree;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NCypressClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TSecondaryIndexTypeHandler
    : public TTypeHandlerBase
{
public:
    using TTypeHandlerBase::TTypeHandlerBase;

    std::optional<TObjectId> CreateObject(
        EObjectType type,
        const TCreateObjectOptions& options) override
    {
        if (type != EObjectType::SecondaryIndex) {
            return {};
        }

        auto attributes = options.Attributes ? options.Attributes->Clone() : EmptyAttributes().Clone();

        auto tablePath = attributes->GetAndRemove<TYPath>("table_path");
        auto indexTablePath = attributes->GetAndRemove<TYPath>("index_table_path");
        auto kind = attributes->Get<ESecondaryIndexKind>("kind");
        auto predicate = attributes->Find<std::string>("predicate");
        auto evaluatedColumnsSchema = attributes->Find<TTableSchemaPtr>("evaluated_columns_schema");

        NApi::TGetNodeOptions getNodeOptions;
        getNodeOptions.Attributes = {"id", "schema", "schema_id", "external_cell_tag"};
        getNodeOptions.ReadFrom = EMasterChannelKind::Cache;

        auto tablesMetadata = WaitFor(AllSucceeded(std::vector{
                Client_->GetNode(tablePath, getNodeOptions),
                Client_->GetNode(indexTablePath, getNodeOptions),
            }))
            .ValueOrThrow();

        auto tableNode = ConvertToNode(tablesMetadata[0]);
        auto indexTableNode = ConvertToNode(tablesMetadata[1]);

        auto tableSchema = tableNode->Attributes().Get<TTableSchema>("schema");
        auto indexTableSchema = indexTableNode->Attributes().Get<TTableSchema>("schema");

        auto tableId = tableNode->Attributes().Get<TTableId>("id");
        auto indexTableId = indexTableNode->Attributes().Get<TTableId>("id");

        auto tableType = TypeFromId(tableId);

        THROW_ERROR_EXCEPTION_UNLESS(IsSequoiaId(tableId) == IsSequoiaId(indexTableId),
            "Cannot bind table and index table from different metadata trees (TableIsSequoia: %v, IndexTableIsSequoia: %v)",
            IsSequoiaId(tableId),
            IsSequoiaId(indexTableId));

        if (tableType != TypeFromId(indexTableId)) {
            THROW_ERROR_EXCEPTION("Table type mismatch")
                .With("table_type", tableType)
                .With("index_table_type", TypeFromId(indexTableId));
        }

        if (tableId == indexTableId) {
            THROW_ERROR_EXCEPTION("Table cannot be an index to itself")
                .With("table_id", tableId);
        }

        if (tableType != EObjectType::ReplicatedTable && tableType != EObjectType::Table) {
            THROW_ERROR_EXCEPTION("Unsupported table type %Qlv", tableType);
        }

        auto tableCellTag = CellTagFromId(tableId);
        auto indexTableCellTag = CellTagFromId(indexTableId);
        if (!IsSequoiaId(tableId) && tableCellTag != indexTableCellTag) {
            THROW_ERROR_EXCEPTION("Table and index table native cell tags differ")
                .With("table_cell_tag", tableCellTag)
                .With("index_table_cell_tag", indexTableCellTag);
        }

        auto getUnfoldedColumns = [&] {
            if (auto unfoldedColumn = attributes->Find<std::string>("unfolded_column")) {
                // TODO(sabdenovch): remove "unfolded_column" from attributes
                // after next stable release.
                attributes->Set("unfolded_table_column", *unfoldedColumn);
                attributes->Set("unfolded_index_column", *unfoldedColumn);
                return TUnfoldedColumns(*unfoldedColumn, *unfoldedColumn);
            } else {
                return TUnfoldedColumns(
                    attributes->Get<std::string>("unfolded_table_column"),
                    attributes->Get<std::string>("unfolded_index_column"));
            }
        };

        auto unfoldedColumns = kind == ESecondaryIndexKind::Unfolding
            ? std::optional(getUnfoldedColumns())
            : std::nullopt;

        ValidateIndexSchema(
            kind,
            tableSchema,
            indexTableSchema,
            predicate,
            evaluatedColumnsSchema,
            unfoldedColumns);

        attributes->Set("table_id", tableId);
        attributes->Set("index_table_id", indexTableId);

        if (IsSequoiaId(tableId)) {
            TGetNodeOptions getNodeOptions;

            attributes->Set("table_schema_id", tableNode->Attributes().Get<TMasterTableSchemaId>("schema_id"));
            attributes->Set("index_table_schema_id", indexTableNode->Attributes().Get<TMasterTableSchemaId>("schema_id"));

            attributes->Set("table_external_cell_tag", tableNode->Attributes().Get<TCellTag>("external_cell_tag"));
        }

        return Client_->CreateObjectImpl(
            type,
            tableCellTag,
            *attributes,
            options);
    }
};

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreateSecondaryIndexTypeHandler(TClient* client)
{
    return New<TSecondaryIndexTypeHandler>(client);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
