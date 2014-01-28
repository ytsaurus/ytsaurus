#pragma once

#include "public.h"

#include <core/misc/ref.h>
#include <core/misc/error.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct IFileReader
    : public virtual TRefCounted
{
    //! Opens the reader. No other method can be called prior to the success of this one.
    virtual TAsyncError Open() = 0;

    //! Reads another portion of file.
    virtual TFuture<TErrorOr<TSharedRef>> Read() = 0;

    //! Returns the file (uncompressed) size.
    virtual i64 GetSize() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileReader)

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

