#include "response_keeper.h"

#include <yt/yt/ytlib/sequoia_client/client.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/ytlib/sequoia_client/records/response_keeper.record.h>

#include <yt/yt/core/rpc/response_keeper.h>

#include <algorithm>

namespace NYT::NSequoiaServer {

using namespace NLogging;
using namespace NRpc;
using namespace NSequoiaClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TFuture<std::optional<TSharedRefArray>> FindKeptResponseInSequoiaAndLog(
    const ISequoiaTransactionPtr& transaction,
    TMutationId mutationId,
    bool retry,
    const TLogger& logger)
{
    return FindKeptResponseInSequoiaAndLog(
        transaction->GetClient(),
        transaction->GetStartTimestamp(),
        mutationId,
        retry,
        logger);
}

TFuture<std::optional<TSharedRefArray>> FindKeptResponseInSequoiaAndLog(
    const ISequoiaClientPtr& client,
    TTimestamp timestamp,
    TMutationId mutationId,
    bool retry,
    const TLogger& logger)
{
    if (!mutationId) {
        static const auto NullOpt = MakeFuture<std::optional<TSharedRefArray>>(std::nullopt);
        return NullOpt;
    }

    const auto& Logger = logger;
    YT_TLOG_DEBUG("Started looking for response in Sequoia response keeper")
        .With("MutationId", mutationId)
        .With("Retry", retry);

    return client
        ->LookupRows<NRecords::TSequoiaResponseKeeperKey>(
            {{.MutationId = mutationId}},
            /*columnFilter*/ {},
            timestamp)
        .Apply(BIND([=] (const std::vector<std::optional<NRecords::TSequoiaResponseKeeper>>& rows) {
            YT_VERIFY(rows.size() == 1);
            auto& row = rows.front();

            i64 responseSize = 0;
            if (row.has_value()) {
                for (const auto& part : row->Response) {
                    responseSize += part.size();
                }
            }

            YT_TLOG_DEBUG("Response lookup in Sequoia response keeper finished")
                .With("Found", row.has_value())
                .With("MutationId", mutationId)
                .With("Retry", retry)
                .WithIf(row.has_value(), "ResponseSize", responseSize);

            if (!row.has_value()) {
                return std::optional<TSharedRefArray>();
            }

            ValidateRetry(mutationId, retry);

            auto& serializedResponseParts = row->Response;
            std::vector<TSharedRef> responseParts(serializedResponseParts.size());
            std::ranges::transform(
                serializedResponseParts,
                responseParts.begin(),
                static_cast<TSharedRef(*)(std::string)>(&TSharedRef::FromString));

            return std::optional(TSharedRefArray(std::move(responseParts), TSharedRefArray::TMoveParts{}));
        }));
}

void KeepResponseInSequoiaAndLog(
    const ISequoiaTransactionPtr& transaction,
    TMutationId mutationId,
    TSharedRefArray response,
    const TLogger& logger)
{
    const auto& Logger = logger;

    if (!response) {
        YT_TLOG_ALERT("Null response is passed to Sequoia response keeper")
            .With("MutationId", mutationId);
        return;
    }

    if (!mutationId) {
        return;
    }

    std::vector<std::string> serializedParts(response.Size());
    std::ranges::transform(
        response.ToVector(),
        serializedParts.begin(),
        &TSharedRef::ToStringBuf);

    transaction->WriteRow(NRecords::TSequoiaResponseKeeper{
        .Key = {.MutationId = mutationId},
        .Response = std::move(serializedParts),
    });

    YT_TLOG_DEBUG("Response is kept in Sequoia response keeper")
        .With("MutationId", mutationId)
        .With("ResponseSize", response.ByteSize());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
