#pragma once

#include "public.h"

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TOperation
    : public TIntrinsicRefCounted
{
public:
    explicit TOperation(const NScheduler::TOperation* underlying);

    DEFINE_BYVAL_RO_PROPERTY(TOperationId, Id);
    DEFINE_BYVAL_RO_PROPERTY(EOperationType, Type);
    DEFINE_BYVAL_RO_PROPERTY(NYTree::IMapNodePtr, Spec);
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);
    DEFINE_BYVAL_RO_PROPERTY(TString, AuthenticatedUser);
    DEFINE_BYVAL_RO_PROPERTY(NScheduler::EOperationCypressStorageMode, StorageMode);
    DEFINE_BYVAL_RO_PROPERTY(NYTree::IMapNodePtr, SecureVault);
    DEFINE_BYVAL_RO_PROPERTY(std::vector<TString>, Owners);
    DEFINE_BYVAL_RO_PROPERTY(NTransactionClient::TTransactionId, UserTransactionId);

    DEFINE_BYVAL_RW_PROPERTY(std::vector<NApi::ITransactionPtr>, Transactions);

    DEFINE_BYVAL_RW_PROPERTY(IOperationControllerPtr, Controller);
    DEFINE_BYVAL_RW_PROPERTY(TOperationControllerHostPtr, Host);
};

DEFINE_REFCOUNTED_TYPE(TOperation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

