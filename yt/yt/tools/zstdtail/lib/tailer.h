#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/error/error.h>

namespace NYT::NZstdtail {

////////////////////////////////////////////////////////////////////////////////

struct ITailListener
{
    virtual ~ITailListener() = default;

    virtual void OnData(TRef ref) = 0;
    virtual void OnError(const TError& error) = 0;
};

void RunTailer(
    std::string path,
    ITailListener* listener);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZstdtail
