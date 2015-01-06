#pragma once

#include "public.h"
#include "client.h"

#include <core/misc/ref.h>
#include <core/misc/error.h>

#include <core/ypath/public.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

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
    virtual TFuture<void> Write(const TRef& data) = 0;

    //! Closes the writer.
    //! No other method can be called after this one.
    virtual TFuture<void> Close() = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileWriter)

IFileWriterPtr CreateFileWriter(
    IClientPtr client,
    const NYPath::TYPath& path,
    const TFileWriterOptions& options = TFileWriterOptions(),
    TFileWriterConfigPtr config = TFileWriterConfigPtr());

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

