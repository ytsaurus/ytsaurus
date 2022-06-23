#pragma once

#include "public.h"

#include <yt/yt/core/net/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NZookeeper {

////////////////////////////////////////////////////////////////////////////////

TSharedRef ReceiveMessage(const NNet::IConnectionReaderPtr& reader);

void PostMessage(
    TSharedRef message,
    const NNet::IConnectionWriterPtr& writer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
