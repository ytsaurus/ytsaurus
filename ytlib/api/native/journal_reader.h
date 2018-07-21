#pragma once

#include "public.h"

#include <yt/client/api/client.h>

#include <yt/core/ypath/public.h>

namespace NYT {
namespace NApi {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

IJournalReaderPtr CreateJournalReader(
    IClientPtr client,
    const NYPath::TYPath& path,
    const TJournalReaderOptions& options = TJournalReaderOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NApi
} // namespace NYT

