#include "stdafx.h"
#include "helpers.h"

#include <core/logging/log.h>

#include <ytlib/chunk_client/public.h>

#include <server/data_node/public.h>

namespace NYT {
namespace NQueryAgent {

////////////////////////////////////////////////////////////////////

bool IsRetriableError(const TError& error)
{
    return
        error.FindMatching(NDataNode::EErrorCode::LocalChunkReaderFailed) ||
        error.FindMatching(NChunkClient::EErrorCode::NoSuchChunk);
}

void ExecuteRequestWithRetries(
    int maxRetries,
    const NLog::TLogger& logger,
    const std::function<void()>& callback)
{
    const auto& Logger = logger;
    std::vector<TError> errors;
    for (int retryIndex = 0; retryIndex < maxRetries; ++retryIndex) {
        try {
            callback();
            return;
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            if (IsRetriableError(error)) {
                LOG_INFO(error, "Request failed, retrying");
                errors.push_back(error);
                continue;
            } else{
                throw;
            }
        }
    }
    THROW_ERROR_EXCEPTION("Request failed after %v retries", maxRetries)
        << errors;
}

////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

