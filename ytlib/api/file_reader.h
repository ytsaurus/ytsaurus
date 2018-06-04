#pragma once

#include "public.h"
#include "client.h"

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/ypath/public.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

struct IFileReader
    : public NConcurrency::IAsyncZeroCopyInputStream
{
    //! Returns revision of file
    virtual ui64 GetRevision() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileReader)

TFuture<IFileReaderPtr> CreateFileReader(
    INativeClientPtr client,
    const NYPath::TYPath& path,
    const TFileReaderOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
