#pragma once

#include "public.h"
#include "client.h"

#include <core/misc/ref.h>
#include <core/misc/error.h>

#include <core/ypath/public.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct IJournalWriter
    : public virtual TRefCounted
{
    //! Opens the writer.
    //! No other method can be called prior to the success of this one.
    virtual TAsyncError Open() = 0;

    //! Writes another portion of rows into the journal.
    //! The result is set when the rows are successfully flushed by an appropriate number
    //! of replicas.
    virtual TAsyncError Write(const std::vector<TSharedRef>& rows) = 0;

    //! Closes the writer.
    //! No other method can be called after this one.
    virtual TAsyncError Close() = 0;
};

DEFINE_REFCOUNTED_TYPE(IJournalWriter)

IJournalWriterPtr CreateJournalWriter(
    IClientPtr client,
    const NYPath::TYPath& path,
    const TJournalWriterOptions& options = TJournalWriterOptions(),
    TJournalWriterConfigPtr config = TJournalWriterConfigPtr());

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

