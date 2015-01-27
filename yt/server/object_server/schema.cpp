#include "stdafx.h"
#include "schema.h"
#include "private.h"
#include "type_handler.h"

#include <core/misc/string.h>

#include <core/ytree/fluent.h>

#include <ytlib/object_client/helpers.h>

#include <server/cell_master/bootstrap.h>

#include <server/object_server/type_handler_detail.h>

namespace NYT {
namespace NObjectServer {

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

class TSchemaProxy
    : public TNonversionedObjectProxyBase<TSchemaObject>
{
public:
    TSchemaProxy(
        NCellMaster::TBootstrap* bootstrap,
        TSchemaObject* object)
        : TBase(bootstrap, object)
    { }

private:
    typedef TNonversionedObjectProxyBase<TSchemaObject> TBase;

    virtual NLog::TLogger CreateLogger() const override
    {
        return ObjectServerLogger;
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, NYson::IYsonConsumer* consumer) override
    {
        if (key == "type") {
            auto type = TypeFromSchemaType(TypeFromId(GetId()));
            BuildYsonFluently(consumer)
                .Value(Format("schema:%v", type));
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

};

IObjectProxyPtr CreateSchemaProxy(NCellMaster::TBootstrap* bootstrap, TSchemaObject* object)
{
    return New<TSchemaProxy>(bootstrap, object);
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
        , Type(type)
    { }

    virtual EObjectType GetType() const override
    {
        return SchemaTypeFromType(Type);
    }

    virtual TObjectBase* FindObject(const TObjectId& id) override
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        auto* object = objectManager->GetSchema(Type);
        return id == object->GetId() ? object : nullptr;
    }

    virtual void Destroy(TObjectBase* /*object*/) override
    {
        YUNREACHABLE();
    }

    virtual EPermissionSet GetSupportedPermissions() const override
    {
        auto permissions = NonePermissions;

        auto objectManager = Bootstrap_->GetObjectManager();
        auto handler = objectManager->GetHandler(Type);

        if (!IsVersionedType(Type)) {
            permissions |= handler->GetSupportedPermissions();
        }

        auto options = handler->GetCreationOptions();
        if (options) {
            permissions |= EPermission::Create;
        }

        return permissions;
    }

private:
    typedef TObjectTypeHandlerBase<TSchemaObject> TBase;

    const EObjectType Type;

    virtual Stroka DoGetName(TSchemaObject* /*object*/) override
    {
        return Format("%Qlv schema", Type);
    }

    virtual IObjectProxyPtr DoGetProxy(
        TSchemaObject* /*object*/,
        NTransactionServer::TTransaction* /*transaction*/) override
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetSchemaProxy(Type);
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

} // namespace NObjectServer
} // namespace NYT
