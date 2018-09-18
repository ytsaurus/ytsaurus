#pragma once

#include <yt/client/transaction_client/public.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TTransactionActionData;

class TReqStartTransaction;
class TRspStartTransaction;

class TReqRegisterTransactionActions;
class TRspRegisterTransactionActions;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct TTransactionActionData;
DECLARE_REFCOUNTED_CLASS(TTransaction)
DECLARE_REFCOUNTED_CLASS(TTransactionManager)

DECLARE_REFCOUNTED_CLASS(TTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
