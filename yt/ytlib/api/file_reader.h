#pragma once

#include "public.h"
#include "client.h"

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/async_stream.h>

#include <yt/core/misc/ref.h>

#include <yt/core/ypath/public.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct IFileReader
    : public NConcurrency::IAsyncZeroCopyInputStream
{
    //! Opens the reader. No other method can be called prior to the success of this one.
    virtual TFuture<void> Open() = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileReader)

IFileReaderPtr CreateFileReader(
    INativeClientPtr client,
    const NYPath::TYPath& path,
    const TFileReaderOptions& options);

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

