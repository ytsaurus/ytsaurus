#pragma once

#include "public.h"

#include <ytlib/misc/property.h>
#include <ytlib/misc/error.h>
#include <ytlib/ytree/ytree.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TOperation
    : public TRefCounted
{
    DEFINE_BYVAL_RO_PROPERTY(TOperationId, OperationId);
    
    DEFINE_BYVAL_RO_PROPERTY(EOperationType, Type);
    
    DEFINE_BYVAL_RW_PROPERTY(EOperationState, State);

    //! User-supplied transaction where the operation resides.
    DEFINE_BYVAL_RO_PROPERTY(TTransactionId, TransactionId);

    DEFINE_BYVAL_RO_PROPERTY(NYTree::IMapNodePtr, Spec);

    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);

    //! Currently existing jobs in the operation.
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TJobPtr>, Jobs);

    DEFINE_BYVAL_RW_PROPERTY(IOperationControllerPtr, Controller);

    //! Contains the error for failed operations.
    DEFINE_BYVAL_RW_PROPERTY(TError, Error);

public:
    TOperation(
        const TOperationId& operationId,
        EOperationType type,
        const TTransactionId& transactionId,
        NYTree::IMapNodePtr spec,
        TInstant startTime);

};


////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
