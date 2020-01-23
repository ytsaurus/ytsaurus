#pragma once

#include <yt/server/lib/transaction_server/public.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqStartTransaction;
using TRspStartTransaction = NTransactionClient::NProto::TRspStartTransaction;

using TReqRegisterTransactionActions = NTransactionClient::NProto::TReqRegisterTransactionActions;
using TRspRegisterTransactionActions = NTransactionClient::NProto::TRspRegisterTransactionActions;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTransactionManager)
DECLARE_REFCOUNTED_CLASS(TDynamicTransactionManagerConfig)

DECLARE_ENTITY_TYPE(TTransaction, TTransactionId, ::THash<TTransactionId>)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
