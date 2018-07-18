#pragma once

#include "public.h"

#include <yt/client/api/client.h>

#include <yt/core/ypath/public.h>

namespace NYT {
namespace NApi {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

IFileWriterPtr CreateFileWriter(
    IClientPtr client,
    const NYPath::TYPath& path,
    const TFileWriterOptions& options = TFileWriterOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NApi
} // namespace NYT

