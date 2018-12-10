#include "schema.h"
#include "private.h"
#include "type_handler.h"
#include "schema_proxy.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/object_server/type_handler_detail.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/misc/string.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NObjectServer {

using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NObjectClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TSchemaObject::TSchemaObject(const TObjectId& id)
    : TNonversionedObjectBase(id)
    , Acd_(this)
{ }

void TSchemaObject::Save(NCellMaster::TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, Acd_);
}

void TSchemaObject::Load(NCellMaster::TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

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

    virtual ETypeFlags GetFlags() const override
    {
        return ETypeFlags::ReplicateAttributes;
    }

    virtual EObjectType GetType() const override
    {
        return SchemaTypeFromType(Type_);
    }

    virtual TObjectBase* FindObject(const TObjectId& id) override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* object = objectManager->GetSchema(Type_);
        return id == object->GetId() ? object : nullptr;
    }

    virtual void DestroyObject(TObjectBase* /*object*/) noexcept override
    {
        Y_UNREACHABLE();
    }

private:
    typedef TObjectTypeHandlerBase<TSchemaObject> TBase;

    const EObjectType Type_;

    virtual TCellTagList DoGetReplicationCellTags(const TSchemaObject* /*object*/) override
    {
        return AllSecondaryCellTags();
    }

    virtual TString DoGetName(const TSchemaObject* /*object*/) override
    {
        return Format("%Qlv schema", Type_);
    }

    virtual IObjectProxyPtr DoGetProxy(
        TSchemaObject* object,
        NTransactionServer::TTransaction* /*transaction*/) override
    {
        return CreateSchemaProxy(Bootstrap_, &Metadata_, object);
    }

    virtual NSecurityServer::TAccessControlDescriptor* DoFindAcd(TSchemaObject* object) override
    {
        return &object->Acd();
    }

    virtual TObjectBase* DoGetParent(TSchemaObject* /*object*/) override
    {
        return nullptr;
    }
};

IObjectTypeHandlerPtr CreateSchemaTypeHandler(NCellMaster::TBootstrap* bootstrap, EObjectType type)
{
    return New<TSchemaTypeHandler>(bootstrap, type);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
