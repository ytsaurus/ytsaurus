#pragma once

#include "public.h"

#include <yt/client/api/client.h>

#include <yt/core/ypath/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

IJournalReaderPtr CreateJournalReader(
    IClientPtr client,
    const NYPath::TYPath& path,
    const TJournalReaderOptions& options = TJournalReaderOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

