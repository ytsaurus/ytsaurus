#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/ref.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

struct IJournalWriter
    : public virtual TRefCounted
{
    //! Opens the writer.
    //! No other method can be called prior to the success of this one.
    virtual TFuture<void> Open() = 0;

    //! Writes another portion of rows into the journal.
    //! The result is set when the rows are successfully flushed by an appropriate number
    //! of replicas.
    virtual TFuture<void> Write(TRange<TSharedRef> rows) = 0;

    //! Closes the writer.
    //! No other method can be called after this one.
    virtual TFuture<void> Close() = 0;
};

DEFINE_REFCOUNTED_TYPE(IJournalWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

