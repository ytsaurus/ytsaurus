#ifndef SCHEMA_TYPE_HANDLER_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include schema_type_handler_detail.h"
// For the sake of sane code completion.
#include "schema_type_handler_detail.h"
#endif

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/config.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <std::derived_from<IObjectTypeHandler> TGeneratedSchemaTypeHandler>
class TSchemaTypeHandlerBase
    : public TGeneratedSchemaTypeHandler
{
    using TBase = TGeneratedSchemaTypeHandler;

public:
    using TBase::TBase;

    void Initialize() override
    {
        TBase::Initialize();

        auto* bootstrap = TBase::GetBootstrap();

        if (bootstrap->GetInitialConfig()->GetObjectManagerConfig()->EnableHistory) {
            this->MetaAttributeSchema_->GetChild("acl")
                ->EnableHistory();
        }
    }

    bool IsBuiltin(const TObject* /*object*/) const override
    {
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <std::derived_from<IObjectTypeHandler> TGeneratedSchemaTypeHandler>
std::unique_ptr<IObjectTypeHandler> CreateSchemaTypeHandler(
    NMaster::IBootstrap* bootstrap,
    TObjectManagerConfigPtr config)
{
    return std::make_unique<TSchemaTypeHandlerBase<TGeneratedSchemaTypeHandler>>(bootstrap, std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
