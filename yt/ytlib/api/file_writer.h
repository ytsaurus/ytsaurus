#pragma once

#include "public.h"

#include <core/misc/ref.h>
#include <core/misc/error.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct IFileWriter
    : public virtual TRefCounted
{
    //! Opens the writer. No other method can be called prior to the success of this one.
    virtual TAsyncError Open() = 0;

    //! Writes the next portion of file data.
    virtual TAsyncError Write(const TRef& data) = 0;

    //! Closes the writer and commits the upload transaction.
    virtual TAsyncError Close() = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileWriter)

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

