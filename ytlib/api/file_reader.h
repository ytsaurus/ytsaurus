#pragma once

#include "public.h"
#include "client.h"

#include <yt/core/actions/future.h>

#include <yt/core/misc/ref.h>

#include <yt/core/ypath/public.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct IFileReader
    : public virtual TRefCounted
{
    //! Opens the reader. No other method can be called prior to the success of this one.
    virtual TFuture<void> Open() = 0;

    //! Reads another portion of file.
    virtual TFuture<TSharedRef> Read() = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileReader)

IFileReaderPtr CreateFileReader(
    IClientPtr client,
    const NYPath::TYPath& path,
    const TFileReaderOptions& options);

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

