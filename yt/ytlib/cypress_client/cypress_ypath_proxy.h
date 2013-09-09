#pragma once

#include "public.h"

#include <core/rpc/public.h>
#include <core/rpc/rpc.pb.h>

#include <ytlib/object_client/object_ypath_proxy.h>

#include <ytlib/cypress_client/cypress_ypath.pb.h>

namespace NYT {
namespace NCypressClient {

////////////////////////////////////////////////////////////////////////////////

//! |#|-prefix.
extern TStringBuf ObjectIdPathPrefix;

//! Creates the YPath pointing to an object with a given id.
NYPath::TYPath FromObjectId(const TObjectId& id);

//! Extracts transaction id associated with the given request.
TTransactionId GetTransactionId(NRpc::IServiceContextPtr context);

//! Extracts transaction id associated with the given request.
TTransactionId GetTransactionId(const NRpc::NProto::TRequestHeader& header);

//! Attaches transaction id to the request.
void SetTransactionId(NRpc::IClientRequestPtr request, const TTransactionId& transactionId);

//! Attaches transaction id to the request.
void SetTransactionId(NRpc::NProto::TRequestHeader* header, const TTransactionId& transactionId);

//! Sets access tracking suppression flag.
void SetSuppressAccessTracking(NRpc::IClientRequestPtr request, bool value);

//! Sets access tracking suppression flag.
void SetSuppressAccessTracking(NRpc::NProto::TRequestHeader* header, bool value);

//! Gets access tracking suppression flag.
bool GetSuppressAccessTracking(const NRpc::NProto::TRequestHeader& header);

////////////////////////////////////////////////////////////////////////////////

struct TCypressYPathProxy
    : public NObjectClient::TObjectYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, Create);
    DEFINE_YPATH_PROXY_METHOD(NProto, Lock);
    DEFINE_YPATH_PROXY_METHOD(NProto, Copy);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressClient
} // namespace NYT
