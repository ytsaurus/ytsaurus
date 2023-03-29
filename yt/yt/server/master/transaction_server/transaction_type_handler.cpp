#include "transaction_type_handler.h"

#include "transaction.h"
#include "transaction_manager.h"
#include "transaction_proxy.h"

namespace NYT::NTransactionServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

TTransactionTypeHandler::TTransactionTypeHandler(
    TBootstrap* bootstrap,
    NObjectClient::EObjectType objectType)
    : TObjectTypeHandlerWithMapBase<TTransaction>(
        bootstrap,
        bootstrap->GetTransactionManager()->MutableTransactionMap())
    , Bootstrap_(bootstrap)
    , ObjectType_(objectType)
{ }

ETypeFlags TTransactionTypeHandler::GetFlags() const
{
    return ETypeFlags::None;
}

EObjectType TTransactionTypeHandler::GetType() const
{
    return ObjectType_;
}

TCellTagList TTransactionTypeHandler::DoGetReplicationCellTags(const TTransaction* transaction)
{
    return transaction->ReplicatedToCellTags();
}

IObjectProxyPtr TTransactionTypeHandler::DoGetProxy(
    TTransaction* transaction,
    TTransaction* /*dummyTransaction*/)
{
    return CreateTransactionProxy(Bootstrap_, &Metadata_, transaction);
}

TAccessControlDescriptor* TTransactionTypeHandler::DoFindAcd(TTransaction* transaction)
{
    return &transaction->Acd();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
