#include "versioned_row_digest.h"
#include "config.h"

#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/library/quantile_digest/quantile_digest.h>

namespace NYT::NTableClient {

using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TVersionedRowDigestBuilder
    : public IVersionedRowDigestBuilder
{
public:
    explicit TVersionedRowDigestBuilder(const TVersionedRowDigestConfigPtr& config)
    {
        Digest_.LastTimestampDigest = CreateTDigest(config->TDigest);
        Digest_.AllButLastTimestampDigest = CreateTDigest(config->TDigest);
    }

    void OnRow(TVersionedRow row) override
    {
        auto* currentValueBegin = row.BeginValues();
        auto* currentValueEnd = row.BeginValues();
        while (currentValueBegin != row.EndValues()) {
            while (currentValueEnd != row.EndValues() &&
                currentValueBegin->Id == currentValueEnd->Id)
            {
                ++currentValueEnd;
            }

            Digest_.LastTimestampDigest->AddValue(TimestampToSecond(currentValueBegin->Timestamp));
            for (auto* value = currentValueBegin + 1; value < currentValueEnd; ++value) {
                Digest_.AllButLastTimestampDigest->AddValue(TimestampToSecond(value->Timestamp));
            }

            int timestampCount = currentValueEnd - currentValueBegin;
            for (int logIndex = 0; (1 << logIndex) - 1 < timestampCount; ++logIndex) {
                if (logIndex == ssize(Digest_.EarliestNthTimestamp)) {
                    Digest_.EarliestNthTimestamp.push_back(TimestampToSecond(MaxTimestamp));
                }
                Digest_.EarliestNthTimestamp[logIndex] = std::min(
                    Digest_.EarliestNthTimestamp[logIndex],
                    TimestampToSecond(currentValueBegin[(1 << logIndex) - 1].Timestamp));
            }

            currentValueBegin = currentValueEnd;
        }

        for (int logIndex = 0; (1 << logIndex) - 1 < row.GetDeleteTimestampCount(); ++logIndex) {
            if (logIndex == ssize(Digest_.EarliestNthTimestamp)) {
                Digest_.EarliestNthTimestamp.push_back(TimestampToSecond(MaxTimestamp));
            }
            Digest_.EarliestNthTimestamp[logIndex] = std::min(
                Digest_.EarliestNthTimestamp[logIndex],
                TimestampToSecond(row.DeleteTimestamps()[(1 << logIndex) - 1]));
        }
    }

    TVersionedRowDigest FlushDigest() override
    {
        return std::move(Digest_);
    }

private:
    TVersionedRowDigest Digest_;

    static i64 TimestampToSecond(TTimestamp timestamp)
    {
        return TimestampToInstant(timestamp).first.Seconds();
    }
};

////////////////////////////////////////////////////////////////////////////////

IVersionedRowDigestBuilderPtr CreateVersionedRowDigestBuilder(
    const TVersionedRowDigestConfigPtr& config)
{
    if (!config->Enable) {
        return nullptr;
    }
    return New<TVersionedRowDigestBuilder>(config);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TVersionedRowDigestExt* protoDigest, const TVersionedRowDigest& digest)
{
    using NYT::ToProto;

    ToProto(protoDigest->mutable_earliest_nth_timestamp(), digest.EarliestNthTimestamp);
    ToProto(
        protoDigest->mutable_last_timestamp_digest(),
        digest.LastTimestampDigest->Serialize());
    ToProto(
        protoDigest->mutable_all_but_last_timestamp_digest(),
        digest.AllButLastTimestampDigest->Serialize());
}

void FromProto(TVersionedRowDigest* digest, const NProto::TVersionedRowDigestExt& protoDigest)
{
    using NYT::FromProto;

    FromProto(&digest->EarliestNthTimestamp, protoDigest.earliest_nth_timestamp());

    {
        auto serialized = TStringBuf(
            protoDigest.last_timestamp_digest().begin(),
            protoDigest.last_timestamp_digest().end());
        digest->LastTimestampDigest = LoadQuantileDigest(serialized);
    }
    {
        auto serialized = TStringBuf(
            protoDigest.all_but_last_timestamp_digest().begin(),
            protoDigest.all_but_last_timestamp_digest().end());
        digest->AllButLastTimestampDigest = LoadQuantileDigest(serialized);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
