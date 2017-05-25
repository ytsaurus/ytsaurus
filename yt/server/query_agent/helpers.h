#pragma once

#include "public.h"

#include <yt/core/logging/public.h>

#include <yt/core/misc/error.h>

#include <functional>

namespace NYT {
namespace NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

bool IsRetriableError(const TError& error);

void ExecuteRequestWithRetries(
    int maxRetries,
    const NLogging::TLogger& logger,
    const std::function<void()>& callback);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT
