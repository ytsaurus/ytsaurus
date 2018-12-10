#pragma once

#include "public.h"

#include <yt/client/api/client.h>

#include <yt/core/ypath/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

IFileWriterPtr CreateFileWriter(
    IClientPtr client,
    const NYPath::TYPath& path,
    const TFileWriterOptions& options = TFileWriterOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

