#include "stdafx.h"
#include "cypress_ypath_proxy.h"

#include <ytlib/ytree/attribute_helpers.h>

#include <ytlib/rpc/client.h>
#include <ytlib/rpc/service.h>

namespace NYT {
namespace NCypressClient {

using namespace NYTree;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static Stroka TransactionIdAttribute("transaction_id");

////////////////////////////////////////////////////////////////////////////////

TStringBuf ObjectIdPathPrefix("#");

TYPath FromObjectId(const TObjectId& id)
{
    return Stroka(ObjectIdPathPrefix) + ToString(id);
}

TTransactionId GetTransactionId(IServiceContextPtr context)
{
    return context->RequestAttributes().Get<TTransactionId>(TransactionIdAttribute, NullTransactionId);
}

void SetTransactionId(IAttributeDictionary* attributes, const TTransactionId& transactionId)
{
    if (transactionId == NullTransactionId) {
        attributes->Remove(TransactionIdAttribute);
    } else {
        attributes->Set(TransactionIdAttribute, transactionId);
    }
}

void SetTransactionId(IClientRequestPtr request, const TTransactionId& transactionId)
{
    SetTransactionId(&request->Attributes(), transactionId);
}

void SetTransactionId(NRpc::NProto::TRequestHeader* header, const TTransactionId& transactionId)
{
    auto attributes = FromProto(header->attributes());
    SetTransactionId(~attributes, transactionId);
    ToProto(header->mutable_attributes(), *attributes);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressClient
} // namespace NYT

