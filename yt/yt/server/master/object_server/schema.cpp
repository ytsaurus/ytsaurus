#include "schema.h"
#include "private.h"
#include "schema_proxy.h"
#include "type_handler.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NObjectServer {

using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NObjectClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TSchemaObject::TSchemaObject(TObjectId id)
    : TObject(id)
    , Acd_(this)
{ }

void TSchemaObject::Save(NCellMaster::TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, Acd_);
}

void TSchemaObject::Load(NCellMaster::TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, Acd_);
}

////////////////////////////////////////////////////////////////////////////////

class TSchemaTypeHandler
    : public TObjectTypeHandlerBase<TSchemaObject>
{
public:
    TSchemaTypeHandler(
        NCellMaster::TBootstrap* bootstrap,
        EObjectType type)
        : TBase(bootstrap)
        , Type_(type)
    { }

    ETypeFlags GetFlags() const override
    {
        return ETypeFlags::ReplicateAttributes;
    }

    EObjectType GetType() const override
    {
        return SchemaTypeFromType(Type_);
    }

    TObject* FindObject(TObjectId id) override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* object = objectManager->GetSchema(Type_);
        return id == object->GetId() ? object : nullptr;
    }

    std::unique_ptr<TObject> InstantiateObject(TObjectId /*id*/) override
    {
        YT_ABORT();
    }

private:
    using TBase = TObjectTypeHandlerBase<TSchemaObject>;

    const EObjectType Type_;

    void DoDestroyObject(TSchemaObject* /*object*/) noexcept override
    {
        YT_ABORT();
    }

    void DoRecreateObjectAsGhost(TSchemaObject* /*object*/) noexcept override
    {
        YT_ABORT();
    }

    TCellTagList DoGetReplicationCellTags(const TSchemaObject* /*object*/) override
    {
        return AllSecondaryCellTags();
    }

    TString DoGetName(const TSchemaObject* /*object*/) override
    {
        return Format("%Qlv schema", Type_);
    }

    IObjectProxyPtr DoGetProxy(
        TSchemaObject* object,
        NTransactionServer::TTransaction* /*transaction*/) override
    {
        return CreateSchemaProxy(Bootstrap_, &Metadata_, object);
    }

    NSecurityServer::TAccessControlDescriptor* DoFindAcd(TSchemaObject* object) override
    {
        return &object->Acd();
    }

    TObject* DoGetParent(TSchemaObject* /*object*/) override
    {
        return nullptr;
    }

    void CheckInvariants(NCellMaster::TBootstrap* /*bootstrap*/) override
    { }
};

IObjectTypeHandlerPtr CreateSchemaTypeHandler(NCellMaster::TBootstrap* bootstrap, EObjectType type)
{
    return New<TSchemaTypeHandler>(bootstrap, type);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
