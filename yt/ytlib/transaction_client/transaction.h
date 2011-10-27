#pragma once

#include "common.h"

namespace NYT {
namespace NTransactionClient {

struct ITransaction
    : virtual public TRefCountedBase
{
    typedef TIntrusivePtr<ITransaction> TPtr;

    TTransactionId GetId() const  = 0;
    void Commit() = 0;
    void Abort() = 0;

    void SubscribeOnCommit(IAction::TPtr callback) = 0;
    void SubscribeOnAbort(IAction::TPtr callback) = 0;
};

} // namespace NTransactionClient
} // namespace NYT
