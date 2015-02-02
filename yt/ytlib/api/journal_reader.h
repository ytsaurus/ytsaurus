#pragma once

#include "public.h"
#include "client.h"

#include <core/misc/ref.h>

#include <core/actions/future.h>

#include <core/ypath/public.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct IJournalReader
    : public virtual TRefCounted
{
    //! Opens the reader.
    //! No other method can be called prior to the success of this one.
    virtual TFuture<void> Open() = 0;

    //! Reads another portion of the journal.
    //! Each row is passed in its own TSharedRef.
    //! When no more rows remain, an empty vector is returned.
    virtual TFuture<std::vector<TSharedRef>> Read() = 0;
};

DEFINE_REFCOUNTED_TYPE(IJournalReader)

IJournalReaderPtr CreateJournalReader(
    IClientPtr client,
    const NYPath::TYPath& path,
    const TJournalReaderOptions& options = TJournalReaderOptions());

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

