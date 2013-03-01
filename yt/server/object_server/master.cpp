#include "stdafx.h"
#include "master.h"
#include "private.h"

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
    : public TObjectProxyBase
{
public:
    explicit TMasterProxy(TBootstrap* bootstrap, TMasterObject* object)
        : TObjectProxyBase(bootstrap, object)
    {
        Logger = ObjectServerLogger;
    }

    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const override
    {
        DECLARE_YPATH_SERVICE_WRITE_METHOD(CreateObject);
        return TObjectProxyBase::IsWriteRequest(context);
    }

private:
    virtual bool DoInvoke(NRpc::IServiceContextPtr context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(CreateObject);
        return TObjectProxyBase::DoInvoke(context);
    }

    TAccount* GetAccount(const Stroka& name)
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        auto* account = securityManager->FindAccountByName(name);
        if (!account) {
            THROW_ERROR_EXCEPTION("No such account: %s", ~name);
        }
        return account;
    }

    TTransaction* GetTransaction(const TTransactionId& id)
    {
        auto transactionManager = Bootstrap->GetTransactionManager();
        auto* transaction = transactionManager->FindTransaction(id);
        if (!transaction || !transaction->IsAlive()) {
            THROW_ERROR_EXCEPTION("No such transaction: %s", ~ToString(id));
        }
        if (!transaction->IsActive()) {
            THROW_ERROR_EXCEPTION("Transaction is not active: %s", ~ToString(id));
        }
        return transaction;
    }

    DECLARE_RPC_SERVICE_METHOD(NObjectClient::NProto, CreateObject)
    {
        auto transactionId =
            request->has_transaction_id()
            ? TTransactionId::FromProto(request->transaction_id())
            : NullTransactionId;
        auto type = EObjectType(request->type());

        context->SetRequestInfo("TransactionId: %s, Type: %s",
            ~ToString(transactionId),
            ~type.ToString());

        ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

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
        auto* object = objectManager->CreateObject(
            transaction,
            account,
            type,
            ~attributes,
            request,
            response);
        const auto& objectId = object->GetId();

        *response->mutable_object_id() = objectId.ToProto();

        context->SetResponseInfo("ObjectId: %s", ~objectId.ToString());
        context->Reply();
    }

};

IObjectProxyPtr CreateMasterProxy(TBootstrap* bootstrap, TMasterObject* object)
{
    return New<TMasterProxy>(bootstrap, object);
}

////////////////////////////////////////////////////////////////////////////////

class TMasterTypeHandler
    : public IObjectTypeHandler
{
public:
    explicit TMasterTypeHandler(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

    virtual EObjectType GetType() const override
    {
        return EObjectType::Master;
    }

    virtual Stroka GetName(TObjectBase* object) override
    {
        UNUSED(object);
        return "master";
    }

    virtual TObjectBase* FindObject(const TObjectId& id) override
    {
        auto objectManager = Bootstrap->GetObjectManager();
        auto* object = objectManager->GetMasterObject();
        return id == object->GetId() ? object : nullptr;
    }

    virtual IObjectProxyPtr GetProxy(
        TObjectBase* object,
        NTransactionServer::TTransaction* transaction) override
    {
        UNUSED(object);
        UNUSED(transaction);
        auto objectManager = Bootstrap->GetObjectManager();
        return objectManager->GetMasterProxy();
    }

    virtual TObjectBase* Create(
        NTransactionServer::TTransaction* transaction,
        NSecurityServer::TAccount* account,
        NYTree::IAttributeDictionary* attributes,
        TReqCreateObject* request,
        TRspCreateObject* response) override
    {
        UNUSED(transaction);
        UNUSED(account);
        UNUSED(attributes);
        UNUSED(request);
        UNUSED(response);
        YUNREACHABLE();
    }

    virtual void Destroy(TObjectBase* object) override
    {
        UNUSED(object);
        YUNREACHABLE();
    }

    virtual void Unstage(
        TObjectBase* object,
        NTransactionServer::TTransaction* transaction,
        bool recursive) override
    {
        UNUSED(object);
        UNUSED(transaction);
        UNUSED(recursive);
        YUNREACHABLE();
    }

    virtual TNullable<TTypeCreationOptions> GetCreationOptions() const override
    {
        return Null;
    }

    virtual NSecurityServer::TAccessControlDescriptor* FindAcd(TObjectBase* object) override
    {
        UNUSED(object);
        return nullptr;
    }

    virtual TObjectBase* GetParent(TObjectBase* object) override
    {
        UNUSED(object);
        return nullptr;
    }

    virtual EPermissionSet GetSupportedPermissions() const override
    {
        // TODO(babenko): flagged enums
        return EPermissionSet(
            EPermission::Read |
            EPermission::Write);
    }

private:
    TBootstrap* Bootstrap;

};

IObjectTypeHandlerPtr CreateMasterTypeHandler(TBootstrap* bootstrap)
{
    return New<TMasterTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
