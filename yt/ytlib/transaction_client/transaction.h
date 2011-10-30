#pragma once

#include "common.h"

namespace NYT {
namespace NTransactionClient {

struct ITransaction
    : virtual public TRefCountedBase
{
    typedef TIntrusivePtr<ITransaction> TPtr;

    //! Throws exception if commit fails.
    /*!
     * \note Client thread only. Should be called no more than once.
     */
    virtual void Commit() = 0;

    /*!
     * \note Client thread only. Should be called no more than once.
     */
    virtual void Abort() = 0;

    /*!
     * \note Thread-safe.
     */
    virtual TTransactionId GetId() const  = 0;

    /*!
     * \note Thread-safe.
     */
    virtual void SubscribeOnCommit(IAction::TPtr callback) = 0;

    /*!
     * \note Thread-safe.
     */
    virtual void SubscribeOnAbort(IAction::TPtr callback) = 0;
};

} // namespace NTransactionClient
} // namespace NYT
