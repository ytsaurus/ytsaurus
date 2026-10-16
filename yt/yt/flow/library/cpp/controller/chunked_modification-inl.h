#pragma once

#ifndef CHUNKED_MODIFICATION_INL_H_
    #error "Direct inclusion of this file is not allowed, include chunked_modification.h"
    // For the sake of sane code completion.
    #include "chunked_modification.h"
#endif

#include "private.h"

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

template <class T>
std::vector<TError> ModifyInChunks(
    TStringBuf phase,
    const std::vector<T>& items,
    ssize_t itemsPerChunk,
    bool splitOnConflict,
    const TChunkCommitter<T>& commitChunk,
    const TChunkCommittedHandler<T>& onCommitted,
    const TChunkRetryDelayer& delay)
{
    constinit static const auto Logger = ControllerLogger;

    if (items.empty()) {
        return {};
    }

    YT_VERIFY(itemsPerChunk > 0);

    std::vector<std::vector<T>> chunks;
    for (ssize_t begin = 0; begin < std::ssize(items); begin += itemsPerChunk) {
        auto end = std::min(begin + itemsPerChunk, std::ssize(items));
        chunks.emplace_back(items.begin() + begin, items.begin() + end);
    }

    std::vector<TError> failures;
    for (int round = 1;; ++round) {
        // The whole round is committed before anything is published: an #onCommitted that mutates
        // the caller's state must not do so in the middle of a round.
        std::vector<TError> results;
        results.reserve(chunks.size());
        for (const auto& chunk : chunks) {
            results.push_back(commitChunk(chunk));
        }

        ssize_t modifiedItems = 0;
        ssize_t conflictedChunks = 0;
        ssize_t transientChunks = 0;
        std::vector<std::vector<T>> retryChunks;
        for (ssize_t index = 0; index < std::ssize(results); ++index) {
            auto& chunk = chunks[index];
            auto& error = results[index];
            if (error.IsOK()) {
                modifiedItems += std::ssize(chunk);
                if (onCommitted) {
                    onCommitted(chunk);
                }
                continue;
            }
            bool conflicted = static_cast<bool>(error.FindMatching(NTabletClient::EErrorCode::TransactionLockConflict));
            bool transient = IsTransientTabletError(error);
            if ((!conflicted && !transient) || round >= MaxChunkedModificationRounds) {
                failures.push_back(std::move(error));
                continue;
            }
            conflictedChunks += conflicted;
            transientChunks += transient;
            if (conflicted && splitOnConflict && std::ssize(chunk) > 1) {
                auto middle = chunk.begin() + std::ssize(chunk) / 2;
                retryChunks.emplace_back(chunk.begin(), middle);
                retryChunks.emplace_back(middle, chunk.end());
            } else {
                retryChunks.push_back(std::move(chunk));
            }
        }

        YT_TLOG_INFO("Dyntable lease modification round")
            .With("Phase", phase)
            .With("Round", round)
            .With("Chunks", std::ssize(chunks))
            .With("ModifiedItems", modifiedItems)
            .With("ConflictedChunks", conflictedChunks)
            .With("TransientChunks", transientChunks)
            .With("FailedChunks", std::ssize(failures))
            .With("RetryChunks", std::ssize(retryChunks));

        if (retryChunks.empty()) {
            return failures;
        }
        // A moving tablet is back within seconds, so the rounds have to wait it out rather than
        // spend themselves on it: without this the whole budget burns in well under a second and
        // the pass fails for a condition that had not even cleared yet. Conflicts need no delay —
        // they resolve by the halving, not by waiting — so the sleep happens only when a transient
        // failure is what forced the round.
        if (transientChunks > 0) {
            auto backoff = std::min(
                ChunkedModificationTransientRetryBackoff * round,
                MaxChunkedModificationTransientRetryBackoff);
            if (delay) {
                delay(backoff);
            } else {
                NConcurrency::TDelayedExecutor::WaitForDuration(backoff);
            }
        }
        chunks = std::move(retryChunks);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
