#pragma once

#include <yt/server/hive/public.h>

#include <yt/server/hydra/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/misc/public.h>
#include <yt/core/misc/small_vector.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqStartTransaction;
using TRspStartTransaction = NTransactionClient::NProto::TRspStartTransaction;

using TReqRegisterTransactionActions = NTransactionClient::NProto::TReqRegisterTransactionActions;
using TRspRegisterTransactionActions = NTransactionClient::NProto::TRspRegisterTransactionActions;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using NObjectClient::TTransactionId;
using NObjectClient::NullTransactionId;

using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;
using NTransactionClient::TTransactionActionData;

using NHiveServer::ETransactionState;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTransactionManager)
DECLARE_REFCOUNTED_CLASS(TTimestampManager)

DECLARE_REFCOUNTED_CLASS(TTransactionManagerConfig)
DECLARE_REFCOUNTED_CLASS(TTimestampManagerConfig)

DECLARE_ENTITY_TYPE(TTransaction, TTransactionId, NObjectClient::TDirectObjectIdHash)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
