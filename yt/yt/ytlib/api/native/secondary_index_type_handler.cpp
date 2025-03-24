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
        auto predicate = attributes->Find<TString>("predicate");

        auto mountInfos = WaitFor(AllSucceeded(std::vector{
                Client_->Connection_->GetTableMountCache()->GetTableInfo(tablePath),
                Client_->Connection_->GetTableMountCache()->GetTableInfo(indexTablePath),
            }))
            .ValueOrThrow();

        auto tableSchema = mountInfos[0]->Schemas[ETableSchemaKind::Primary];
        auto indexTableSchema = mountInfos[1]->Schemas[ETableSchemaKind::Primary];

        auto tableId = mountInfos[0]->TableId;
        auto indexTableId = mountInfos[1]->TableId;

        if (TypeFromId(tableId) != TypeFromId(indexTableId)) {
            THROW_ERROR_EXCEPTION("Table type mismatch")
                << TErrorAttribute("table_type", TypeFromId(tableId))
                << TErrorAttribute("index_table_type", TypeFromId(indexTableId));
        }

        auto tableType = TypeFromId(tableId);
        if (tableType != EObjectType::ReplicatedTable && tableType != EObjectType::Table) {
            THROW_ERROR_EXCEPTION("Unsupported table type %Qlv", tableType);
        }

        auto cellTag = CellTagFromId(tableId);
        if (cellTag != CellTagFromId(indexTableId)) {
            THROW_ERROR_EXCEPTION("Table and index table native cell tags differ")
                << TErrorAttribute("table_cell_tag", cellTag)
                << TErrorAttribute("index_table_cell_tag", CellTagFromId(indexTableId));
        }

        switch(kind) {
            case ESecondaryIndexKind::FullSync:
                ValidateFullSyncIndexSchema(
                    *tableSchema,
                    *indexTableSchema);
                break;

            case ESecondaryIndexKind::Unfolding: {
                auto unfoldedColumnName = attributes->Find<TString>("unfolded_column");
                // COMPAT(sabdenovch)
                if (unfoldedColumnName) {
                    ValidateUnfoldingIndexSchema(
                        *tableSchema,
                        *indexTableSchema,
                        *unfoldedColumnName);
                } else {
                    FindUnfoldedColumnAndValidate(
                        *tableSchema,
                        *indexTableSchema);
                }
                break;
            }

            case ESecondaryIndexKind::Unique:
                ValidateUniqueIndexSchema(
                    *tableSchema,
                    *indexTableSchema);
                break;

            default:
                YT_ABORT();
        }

        if (predicate) {
            auto expr = PrepareExpression(*predicate, *tableSchema);
            THROW_ERROR_EXCEPTION_IF(expr->GetWireType() != EValueType::Boolean,
                "Expected boolean expression as predicate, got %v",
                *expr->LogicalType);

            TColumnSet predicateColumns;
            TReferenceHarvester(&predicateColumns).Visit(expr);

            ValidateColumnsAreInIndexLockGroup(predicateColumns, *tableSchema, *indexTableSchema);
        }

        attributes->Set("table_id", tableId);
        attributes->Set("index_table_id", indexTableId);

        return Client_->CreateObjectImpl(
            type,
            cellTag,
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
