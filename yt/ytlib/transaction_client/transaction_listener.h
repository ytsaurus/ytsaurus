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
    void ListenTransaction(TTransactionPtr transaction);

    //! Checks if any of transactions that we are listening to were aborted.
    //! If so, raises an exception.
    void CheckAborted() const;
    
    //! Return is aborted flag.
    bool IsAborted() const;

private:
    volatile bool IsAborted_;

    void OnAborted();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
