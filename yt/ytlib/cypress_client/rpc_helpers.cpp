#include "stdafx.h"
#include "rpc_helpers.h"

#include <core/ytree/attribute_helpers.h>

#include <core/rpc/client.h>
#include <core/rpc/service.h>

#include <ytlib/cypress_client/rpc.pb.h>

namespace NYT {
namespace NCypressClient {

using namespace NRpc;
using namespace NRpc::NProto;
using namespace NCypressClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TTransactionId GetTransactionId(IServiceContextPtr context)
{
    return GetTransactionId(context->RequestHeader());
}

TTransactionId GetTransactionId(const TRequestHeader& header)
{
    return header.HasExtension(TTransactionalExt::transaction_id)
           ? FromProto<TTransactionId>(header.GetExtension(TTransactionalExt::transaction_id))
           : NullTransactionId;
}

void SetTransactionId(IClientRequestPtr request, const TTransactionId& transactionId)
{
    SetTransactionId(&request->Header(), transactionId);
}

void SetTransactionId(TRequestHeader* header, const TTransactionId& transactionId)
{
    ToProto(header->MutableExtension(TTransactionalExt::transaction_id), transactionId);
}

void SetSuppressAccessTracking(IClientRequestPtr request, bool value)
{
    SetSuppressAccessTracking(&request->Header(), value);
}

void SetSuppressAccessTracking(TRequestHeader* header, bool value)
{
    header->SetExtension(TAccessTrackingExt::suppress_access_tracking, value);
}

bool GetSuppressAccessTracking(const TRequestHeader& header)
{
    return header.HasExtension(TAccessTrackingExt::suppress_access_tracking)
        ? header.GetExtension(TAccessTrackingExt::suppress_access_tracking)
        : false;
}

void SetSuppressModificationTracking(IClientRequestPtr request, bool value)
{
    SetSuppressModificationTracking(&request->Header(), value);
}

void SetSuppressModificationTracking(TRequestHeader* header, bool value)
{
    header->SetExtension(TAccessTrackingExt::suppress_modification_tracking, value);
}

bool GetSuppressModificationTracking(const TRequestHeader& header)
{
    return header.HasExtension(TAccessTrackingExt::suppress_modification_tracking)
        ? header.GetExtension(TAccessTrackingExt::suppress_modification_tracking)
        : false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressClient
} // namespace NYT

