#include "cypress_integration.h"
#include "transaction.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/cypress_server/virtual.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/transaction_server/transaction_manager.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NTransactionServer {

using namespace NYTree;
using namespace NCypressServer;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TVirtualTransactionMapBase
    : public TVirtualMulticellMapBase
{
public:
    using TVirtualMulticellMapBase::TVirtualMulticellMapBase;

private:
    const THashSet<TTransaction*>& Transactions() const
    {
        return static_cast<const T*>(this)->Transactions();
    }

    TFuture<std::vector<TObjectId>> GetKeys(i64 sizeLimit) const override
    {
        return MakeFuture(ToObjectIds(Transactions(), sizeLimit));
    }

    bool IsValid(TObject* object) const override
    {
        return IsObjectAlive(object);
    }

    bool NeedSuppressUpstreamSync() const override
    {
        return false;
    }

    TFuture<i64> GetSize() const override
    {
        return MakeFuture<i64>(Transactions().size());
    }

    NYPath::TYPath GetWellKnownPath() const override
    {
        return static_cast<const T*>(this)->GetWellKnownPath();
    }
};

class TVirtualTopmostTransactionMap
    : public TVirtualTransactionMapBase<TVirtualTopmostTransactionMap>
{
public:
    using TBase = TVirtualTransactionMapBase<TVirtualTopmostTransactionMap>;

    using TBase::TBase;

    const THashSet<TTransaction*>& Transactions() const
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        return transactionManager->NativeTopmostTransactions();
    }

    NYPath::TYPath GetWellKnownPath() const override
    {
        return "//sys/topmost_transactions";
    }
};

class TVirtualTransactionMap
    : public TVirtualTransactionMapBase<TVirtualTransactionMap>
{
public:
    using TBase = TVirtualTransactionMapBase<TVirtualTransactionMap>;

    using TBase::TBase;

    const THashSet<TTransaction*>& Transactions() const
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        return transactionManager->NativeTransactions();
    }

    NYPath::TYPath GetWellKnownPath() const override
    {
        return "//sys/transactions";
    }

    bool NeedSuppressTransactionCoordinatorSync() const override
    {
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateTransactionMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::TransactionMap,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualTransactionMap>(bootstrap, std::move(owningNode));
        }),
        EVirtualNodeOptions::RedirectSelf);
}

INodeTypeHandlerPtr CreateTopmostTransactionMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::TopmostTransactionMap,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualTopmostTransactionMap>(bootstrap, std::move(owningNode));
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
