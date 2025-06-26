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
    YT_LOG_DEBUG("Started looking for response in Sequoia response keeper (MutationId: %v, Retry: %v)",
        mutationId,
        retry);

    return client
        ->LookupRows<NRecords::TSequoiaResponseKeeperKey>(
            {{.MutationId = mutationId}},
            /*columnFilter*/ {},
            timestamp)
        .Apply(BIND([=] (const std::vector<std::optional<NRecords::TSequoiaResponseKeeper>>& rows) {
            YT_VERIFY(rows.size() == 1);
            auto& row = rows.front();

            YT_LOG_DEBUG("Response was %v in Sequoia response keeper (MutationId: %v, Retry: %v%v)",
                row.has_value() ? "found" : "not found",
                mutationId,
                retry,
                MakeFormatterWrapper([&] (TStringBuilderBase* builder) {
                    if (!row.has_value()) {
                        return;
                    }

                    i64 size = 0;
                    for (const auto& part : row->Response) {
                        size += part.size();
                    }
                    builder->AppendFormat(", ResponseSize: %v", size);
                }));

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
        YT_LOG_ALERT("Null response is passed to Sequoia response keeper (MutationId: %v)",
            mutationId);
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

    YT_LOG_DEBUG("Response is kept in Sequoia response keeper (MutationId: %v, ResponseSize: %v)",
        mutationId,
        response.ByteSize());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
