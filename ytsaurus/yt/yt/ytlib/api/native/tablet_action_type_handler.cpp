#include "table_collocation_type_handler.h"

#include "type_handler_detail.h"
#include "client_impl.h"

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

namespace NYT::NApi::NNative {

using namespace NYson;
using namespace NYTree;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NCypressClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TTabletActionTypeHandler
    : public TNullTypeHandler
{
public:
    explicit TTabletActionTypeHandler(TClient* client)
        : Client_(client)
    { }

    std::optional<TObjectId> CreateObject(
        EObjectType type,
        const TCreateObjectOptions& options) override
    {
        if (type != EObjectType::TabletAction) {
            return {};
        }

        auto attributes = options.Attributes ? options.Attributes->Clone() : EmptyAttributes().Clone();

        auto tabletIds = attributes->Get<std::vector<TTabletId>>("tablet_ids");
        if (tabletIds.empty()) {
            THROW_ERROR_EXCEPTION("\"tablet_ids\" are empty");
        }

        auto cellTag = CellTagFromId(tabletIds[0]);
        for (auto tabletId : tabletIds) {
            if (CellTagFromId(tabletId) != cellTag) {
                THROW_ERROR_EXCEPTION("All tablets listed in \"tablet_ids\" must belong to the same master cell");
            }
        }

        return Client_->CreateObjectImpl(
            type,
            cellTag,
            *attributes,
            options);
    }

private:
    TClient* const Client_;
};

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreateTabletActionTypeHandler(TClient* client)
{
    return New<TTabletActionTypeHandler>(client);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
