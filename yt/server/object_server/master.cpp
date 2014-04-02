#include "stdafx.h"
#include "master.h"
#include "type_handler_detail.h"
#include "private.h"

#include <core/ytree/attribute_helpers.h>

#include <ytlib/object_client/master_ypath.pb.h>

#include <server/security_server/security_manager.h>

#include <server/transaction_server/transaction.h>
#include <server/transaction_server/transaction_manager.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NObjectServer {

using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NObjectClient;
using namespace NYTree;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TMasterObject::TMasterObject(const TObjectId& id)
    : TNonversionedObjectBase(id)
{ }

////////////////////////////////////////////////////////////////////////////////

class TMasterProxy
    : public TNonversionedObjectProxyBase<TMasterObject>
{
public:
    explicit TMasterProxy(TBootstrap* bootstrap, TMasterObject* object)
        : TBase(bootstrap, object)
    { }

private:
    typedef TNonversionedObjectProxyBase<TMasterObject> TBase;

    virtual NLog::TLogger CreateLogger() const override
    {
        return ObjectServerLogger;
    }

    virtual bool DoInvoke(NRpc::IServiceContextPtr context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(CreateObjects);
        return TObjectProxyBase::DoInvoke(context);
    }

    TAccount* GetAccount(const Stroka& name)
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        auto* account = securityManager->FindAccountByName(name);
        if (!account) {
            THROW_ERROR_EXCEPTION("No such account %s", ~name.Quote());
        }
        return account;
    }

    TTransaction* GetTransaction(const TTransactionId& id)
    {
        auto transactionManager = Bootstrap->GetTransactionManager();
        auto* transaction = transactionManager->FindTransaction(id);
        if (!IsObjectAlive(transaction)) {
            THROW_ERROR_EXCEPTION("No such transaction %s", ~ToString(id));
        }
        transaction->ValidateActive();
        return transaction;
    }

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, CreateObjects)
    {
        DeclareMutating();

        auto transactionId =
            request->has_transaction_id()
            ? FromProto<TTransactionId>(request->transaction_id())
            : NullTransactionId;
        auto type = EObjectType(request->type());

        context->SetRequestInfo("TransactionId: %s, Type: %s, Account: %s, ObjectCount: %d",
            ~ToString(transactionId),
            ~ToString(type),
            request->has_account() ? ~request->account() : "<Null>",
            request->object_count());

        auto* transaction =
            transactionId != NullTransactionId
            ? GetTransaction(transactionId)
            : nullptr;

        auto* account =
            request->has_account()
            ? GetAccount(request->account())
            : nullptr;

        auto attributes =
            request->has_object_attributes()
            ? FromProto(request->object_attributes())
            : CreateEphemeralAttributes();

        auto objectManager = Bootstrap->GetObjectManager();

        for (int index = 0; index < request->object_count(); ++index) {
            auto* object = objectManager->CreateObject(
                transaction,
                account,
                type,
                attributes.get(),
                request,
                response);
            const auto& objectId = object->GetId();

            ToProto(response->add_object_ids(), objectId);

            if (index == 0) {
                context->SetResponseInfo("ObjectId: %s", ~ToString(objectId));
            }
        }
        
        context->Reply();
    }

};

IObjectProxyPtr CreateMasterProxy(TBootstrap* bootstrap, TMasterObject* object)
{
    return New<TMasterProxy>(bootstrap, object);
}

////////////////////////////////////////////////////////////////////////////////

class TMasterTypeHandler
    : public TObjectTypeHandlerBase<TMasterObject>
{
public:
    explicit TMasterTypeHandler(TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap)
    { }

    virtual EObjectType GetType() const override
    {
        return EObjectType::Master;
    }

    virtual TObjectBase* FindObject(const TObjectId& id) override
    {
        auto objectManager = Bootstrap->GetObjectManager();
        auto* object = objectManager->GetMasterObject();
        return id == object->GetId() ? object : nullptr;
    }

    virtual void Destroy(TObjectBase* /*object*/) override
    {
        YUNREACHABLE();
    }

    virtual EPermissionSet GetSupportedPermissions() const override
    {
        return NonePermissions;
    }

private:
    virtual Stroka DoGetName(TMasterObject* /*object*/) override
    {
        return "master";
    }

    virtual IObjectProxyPtr DoGetProxy(
        TMasterObject* /*object*/,
        NTransactionServer::TTransaction* /*transaction*/) override
    {
        auto objectManager = Bootstrap->GetObjectManager();
        return objectManager->GetMasterProxy();
    }

};

IObjectTypeHandlerPtr CreateMasterTypeHandler(TBootstrap* bootstrap)
{
    return New<TMasterTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
