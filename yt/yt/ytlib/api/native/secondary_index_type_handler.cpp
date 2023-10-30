#include "secondary_index_type_handler.h"

#include "type_handler_detail.h"
#include "client_impl.h"

#include <yt/yt/ytlib/table_client/helpers.h>

namespace NYT::NApi::NNative {

using namespace NYson;
using namespace NYTree;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NCypressClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TSecondaryIndexTypeHandler
    : public TNullTypeHandler
{
public:
    explicit TSecondaryIndexTypeHandler(TClient* client)
        : Client_(client)
    { }

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

        TTableId tableId;
        TTableId indexTableId;
        TCellTag tableCellTag;
        TCellTag indexTableCellTag;

        ResolveExternalTable(Client_, tablePath, &tableId, &tableCellTag);
        ResolveExternalTable(Client_, indexTablePath, &indexTableId, &indexTableCellTag);

        if (CellTagFromId(tableId) != CellTagFromId(indexTableId)) {
            THROW_ERROR_EXCEPTION("Table and index table native cell tags differ")
                << TErrorAttribute("table_cell_tag", CellTagFromId(tableId))
                << TErrorAttribute("index_table_cell_tag", CellTagFromId(indexTableId));
        }
        if (tableCellTag != indexTableCellTag) {
            THROW_ERROR_EXCEPTION("Table and index table external cell tags differ")
                << TErrorAttribute("table_external_cell_tag", tableCellTag)
                << TErrorAttribute("index_table_external_cell_tag", indexTableCellTag);
        }

        attributes->Set("table_id", tableId);
        attributes->Set("index_table_id", indexTableId);

        return Client_->CreateObjectImpl(
            type,
            PrimaryMasterCellTagSentinel,
            *attributes,
            options);
    }

private:
    TClient* const Client_;
};

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreateSecondaryIndexTypeHandler(TClient* client)
{
    return New<TSecondaryIndexTypeHandler>(client);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
