#pragma once

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <library/cpp/yt/logging/public.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

//! Checks if request is already executed and kept in "response_keeper" Sequoia
//! table. Returns:
//!   - response message if request is already executed;
//!   - "Duplicate is not marked as retry" error if request is present in table
//!     but #retry is |false|;
//!   - |nullopt| otherwise.
TFuture<std::optional<TSharedRefArray>> FindKeptResponseInSequoiaAndLog(
    const NSequoiaClient::ISequoiaTransactionPtr& transaction,
    NRpc::TMutationId mutationId,
    bool retry,
    const NLogging::TLogger& logger);
TFuture<std::optional<TSharedRefArray>> FindKeptResponseInSequoiaAndLog(
    const NSequoiaClient::ISequoiaClientPtr& client,
    NTransactionClient::TTimestamp timestamp,
    NRpc::TMutationId mutationId,
    bool retry,
    const NLogging::TLogger& logger);

//! Keeps response in "response_keeper" Sequoia table if needed (i.e.
//! #mutationId is not null).
/*!
 *  NB: #responseMessage must not be null.
 */
void KeepResponseInSequoiaAndLog(
    const NSequoiaClient::ISequoiaTransactionPtr& transaction,
    NRpc::TMutationId mutationId,
    TSharedRefArray responseMessage,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
