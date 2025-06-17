#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

// struct IFileBuilder
//     : public virtual TRefCounted
// {
//     //! Opens the builder.
//     //! No other method can be called prior to the success of this one.
//     virtual TFuture<void> Open() = 0;

//     //! Closes the writer.
//     //! No other method can be called after this one.
//     virtual TFuture<void> Close() = 0;
// };

// DEFINE_REFCOUNTED_TYPE(IFileBuilder)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

