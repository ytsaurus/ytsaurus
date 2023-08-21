#pragma once

#include "public.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

IJournalWriterPtr CreateJournalWriter(
    IClientPtr client,
    NYPath::TYPath path,
    TJournalWriterOptions options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
