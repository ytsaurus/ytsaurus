#pragma once

#include "public.h"

#include <yt/client/api/client.h>

#include <yt/core/ypath/public.h>

namespace NYT {
namespace NApi {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

IJournalWriterPtr CreateJournalWriter(
    IClientPtr client,
    const NYPath::TYPath& path,
    const TJournalWriterOptions& options = TJournalWriterOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NApi
} // namespace NYT

