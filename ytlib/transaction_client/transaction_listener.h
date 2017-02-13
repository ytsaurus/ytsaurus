#pragma once

#include "public.h"

#include <yt/ytlib/api/public.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! A simple base class that listens for transaction aborts.
class TTransactionListener
    : public virtual TRefCounted
{
protected:
    //! Starts listening for transaction abort.
    void ListenTransaction(NApi::ITransactionPtr transaction);

    //! Checks if any of transactions that we are listening to were aborted.
    //! If so, raises an exception.
    void ValidateAborted() const;
    
    //! Return is aborted flag.
    bool IsAborted() const;

private:
    volatile bool IsAborted_ = false;

    void OnAborted();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
