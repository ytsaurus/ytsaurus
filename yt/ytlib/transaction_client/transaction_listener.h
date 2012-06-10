#pragma once

#include "public.h"

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! A simple base class that listens for transaction aborts.
class TTransactionListener
    : public virtual TRefCounted
{
protected:
    TTransactionListener();

    //! Starts listening for transaction abort.
    void ListenTransaction(ITransactionPtr transaction);

    //! Checks if any of transactions that we are listening to were aborted.
    //! If so, raises an exception.
    void CheckAborted() const;

private:
    volatile bool IsAborted;

    void OnAborted();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
