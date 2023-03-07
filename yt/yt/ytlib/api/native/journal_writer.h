#pragma once

#include "public.h"

#include <yt/client/api/client.h>

#include <yt/core/ypath/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

IJournalWriterPtr CreateJournalWriter(
    IClientPtr client,
    const NYPath::TYPath& path,
    const TJournalWriterOptions& options = TJournalWriterOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

