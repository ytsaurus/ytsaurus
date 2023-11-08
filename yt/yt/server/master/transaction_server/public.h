#pragma once

#include <yt/yt/server/lib/transaction_server/public.h>

#include <yt/yt/ytlib/cypress_transaction_client/public.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqStartTransaction;
using TRspStartTransaction = NTransactionClient::NProto::TRspStartTransaction;
using TRspStartCypressTransaction = NCypressTransactionClient::NProto::TRspStartTransaction;

using TReqRegisterTransactionActions = NTransactionClient::NProto::TReqRegisterTransactionActions;
using TRspRegisterTransactionActions = NTransactionClient::NProto::TRspRegisterTransactionActions;

using TReqReplicateTransactions = NTransactionClient::NProto::TReqReplicateTransactions;
using TRspReplicateTransactions = NTransactionClient::NProto::TRspReplicateTransactions;

using TReqIssueLeases = NTransactionClient::NProto::TReqIssueLeases;
using TRspIssueLeases = NTransactionClient::NProto::TRspIssueLeases;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ITransactionManager)

DECLARE_REFCOUNTED_CLASS(TDynamicTransactionManagerConfig)

DECLARE_REFCOUNTED_CLASS(TTransactionPresenceCache)
DECLARE_REFCOUNTED_CLASS(TTransactionPresenceCacheConfig)

DECLARE_REFCOUNTED_CLASS(TBoomerangTracker)
DECLARE_REFCOUNTED_CLASS(TBoomerangTrackerConfig)

DECLARE_REFCOUNTED_CLASS(TTransactionReplicationSessionWithoutBoomerangs)
DECLARE_REFCOUNTED_CLASS(TTransactionReplicationSessionWithBoomerangs)

DECLARE_ENTITY_TYPE(TTransaction, TTransactionId, ::THash<TTransactionId>)

using TBoomerangWaveId = TGuid;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionLeasesState,
    // Transaction is active: new leases can be issued.
    ((Active)                (0))
    // Leases that were already issued are revoking, new leases cannot be issued.
    ((Revoking)              (1))
    // Leases are revoked and new leases cannot be issued.
    ((Revoked)               (2))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
