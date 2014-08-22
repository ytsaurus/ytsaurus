#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/logging/public.h>

#include <functional>

namespace NYT {
namespace NQueryAgent {

////////////////////////////////////////////////////////////////////

bool IsRetriableError(const TError& error);

void ExecuteRequestWithRetries(
    int maxRetries,
    const NLog::TLogger& logger,
    const std::function<void()>& callback);

////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT
