#pragma once

#include "public.h"

#include <yt/core/concurrency/public.h>

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
