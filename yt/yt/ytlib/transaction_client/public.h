#pragma once

#include <yt/yt/client/transaction_client/public.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TTransactionActionData;

class TReqStartTransaction;
class TRspStartTransaction;

class TReqRegisterTransactionActions;
class TRspRegisterTransactionActions;

class TReqReplicateTransactions;
class TRspReplicateTransactions;

class TReqIssueLeases;
class TRspIssueLeases;

class TReqRegisterLockableDynamicTables;
class TRspRegisterLockableDynamicTables;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct TTransactionActionData;
DECLARE_REFCOUNTED_CLASS(TTransaction)
DECLARE_REFCOUNTED_CLASS(TTransactionManager)
DECLARE_REFCOUNTED_STRUCT(IClockManager)

DECLARE_REFCOUNTED_STRUCT(TTransactionManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TPingBatcherConfig)
DECLARE_REFCOUNTED_STRUCT(TClockManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicClockManagerConfig)

////////////////////////////////////////////////////////////////////////////////

const std::string ShouldBeStrippedErrorAttributeKey = "should_be_stripped";
const std::string StrippedErrorMessages = "stripped_error_messages";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
