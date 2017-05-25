#pragma once

#include "public.h"
#include "client.h"

#include <yt/core/misc/error.h>
#include <yt/core/misc/ref.h>

#include <yt/core/ypath/public.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

struct IFileWriter
    : public virtual TRefCounted
{
    //! Opens the writer.
    //! No other method can be called prior to the success of this one.
    virtual TFuture<void> Open() = 0;

    //! Writes the next portion of file data.
    /*!
     *  #data must remain alive until this asynchronous operation completes.
     */
    virtual TFuture<void> Write(const TSharedRef& data) = 0;

    //! Closes the writer.
    //! No other method can be called after this one.
    virtual TFuture<void> Close() = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileWriter)

IFileWriterPtr CreateFileWriter(
    INativeClientPtr client,
    const NYPath::TYPath& path,
    const TFileWriterOptions& options = TFileWriterOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

