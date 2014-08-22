#include "stdafx.h"
#include "helpers.h"

#include <core/logging/log.h>

#include <server/data_node/public.h>

namespace NYT {
namespace NQueryAgent {

////////////////////////////////////////////////////////////////////

bool IsRetriableError(const TError& error)
{
    if (error.FindMatching(NDataNode::EErrorCode::LocalChunkReaderFailed)) {
        return true;
    }
    return false;
}

void ExecuteRequestWithRetries(
    int maxRetries,
    const NLog::TLogger& logger,
    const std::function<void()>& callback)
{
    const auto& Logger = logger;
    for (int retryIndex = 0; retryIndex < maxRetries; ++retryIndex) {
        try {
            callback();
            break;
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            if (IsRetriableError(error)) {
                LOG_INFO(error, "Request failed, retrying");
                continue;
            } else{
                throw;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

