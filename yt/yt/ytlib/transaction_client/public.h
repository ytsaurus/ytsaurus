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

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct TTransactionActionData;
DECLARE_REFCOUNTED_CLASS(TTransaction)
DECLARE_REFCOUNTED_CLASS(TTransactionManager)

DECLARE_REFCOUNTED_CLASS(TTransactionManagerConfig)

//! Signatures enable checking tablet transaction integrity.
/*!
 *  When a transaction is created, its signature is #InitialTransactionSignature.
 *  Each change within a transaction is annotated with a signature; these signatures are
 *  added to the transaction's signature. For a commit to be successful, the final signature must
 *  be equal to #FinalTransactionSignature.
 */
using TTransactionSignature = ui32;
const TTransactionSignature InitialTransactionSignature = 0;
const TTransactionSignature FinalTransactionSignature = 0xffffffffU;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
