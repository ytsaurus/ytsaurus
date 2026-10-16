#include "versioned_row_digest.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/library/quantile_digest/quantile_digest.h>

namespace NYT::NTableClient {

using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TVersionedRowDigest::TVersionedRowDigest(const TTDigestConfigPtr& config)
    : LastTimestampDigest(CreateTDigest(config))
    , AllButLastTimestampDigest(CreateTDigest(config))
    , FirstTimestampDigest(CreateTDigest(config))
{ }

void TVersionedRowDigest::MergeWith(const TVersionedRowDigestPtr& other)
{
    LastTimestampDigest->MergeWith(other->LastTimestampDigest);
    AllButLastTimestampDigest->MergeWith(other->AllButLastTimestampDigest);
    if (FirstTimestampDigest && other->FirstTimestampDigest) {
        FirstTimestampDigest->MergeWith(other->FirstTimestampDigest);
    }

    auto mergeEarliestNthTimestamps = [](std::vector<i64>* destination, const std::vector<i64>& source) {
        destination->reserve(source.size());
        for (int index = 0; index < ssize(source); ++index) {
            if (ssize(*destination) > index) {
                (*destination)[index] = std::min((*destination)[index], source[index]);
            } else {
                destination->push_back(source[index]);
            }
        }
    };

    mergeEarliestNthTimestamps(
        &EarliestNthTimestamp,
        other->EarliestNthTimestamp);
    mergeEarliestNthTimestamps(
        &EarliestAggregateOrDeleteNthTimestamp,
        other->EarliestAggregateOrDeleteNthTimestamp);
}

////////////////////////////////////////////////////////////////////////////////

class TVersionedRowDigestBuilder
    : public IVersionedRowDigestBuilder
{
public:
    TVersionedRowDigestBuilder(const TTDigestConfigPtr& config, const TTableSchemaPtr& schema)
        : Digest_(New<TVersionedRowDigest>(config))
        , Schema_(schema)
    { }

    void OnRow(TVersionedRow row) override
    {
        auto updateEarliestNthTimestamp = [&] (
            std::vector<i64>* earliestNthTimestamp,
            auto getTimestamp,
            int timestampCount)
        {
            for (int logIndex = 0; (1 << logIndex) - 1 < timestampCount; ++logIndex) {
                if (logIndex == ssize(*earliestNthTimestamp)) {
                    earliestNthTimestamp->push_back(TimestampToSecond(MaxTimestamp));
                }
                (*earliestNthTimestamp)[logIndex] = std::min(
                    (*earliestNthTimestamp)[logIndex],
                    getTimestamp(timestampCount - (1 << logIndex)));
            }
        };

        auto* currentValueBegin = row.BeginValues();
        auto* currentValueEnd = row.BeginValues();
        while (currentValueBegin != row.EndValues()) {
            while (currentValueEnd != row.EndValues() &&
                currentValueBegin->Id == currentValueEnd->Id)
            {
                ++currentValueEnd;
            }

            Digest_->LastTimestampDigest->AddValue(TimestampToSecond(currentValueBegin->Timestamp));
            for (auto* value = currentValueBegin + 1; value < currentValueEnd; ++value) {
                Digest_->AllButLastTimestampDigest->AddValue(TimestampToSecond(value->Timestamp));
            }
            Digest_->FirstTimestampDigest->AddValue(TimestampToSecond(std::prev(currentValueEnd)->Timestamp));

            int timestampCount = currentValueEnd - currentValueBegin;
            auto* earliestNthTimestamp = Schema_->Columns()[currentValueBegin->Id].Aggregate()
                ? &Digest_->EarliestAggregateOrDeleteNthTimestamp
                : &Digest_->EarliestNthTimestamp;

            updateEarliestNthTimestamp(
                earliestNthTimestamp,
                [&] (int index) { return TimestampToSecond(currentValueBegin[index].Timestamp); },
                timestampCount);

            currentValueBegin = currentValueEnd;
        }

        auto deleteTimestampCount = row.GetDeleteTimestampCount();
        updateEarliestNthTimestamp(
            &Digest_->EarliestAggregateOrDeleteNthTimestamp,
            [&] (int index) { return TimestampToSecond(row.DeleteTimestamps()[index]); },
            deleteTimestampCount);
    }

    TVersionedRowDigestPtr FlushDigest() override
    {
        return std::move(Digest_);
    }

private:
    TVersionedRowDigestPtr Digest_;
    TTableSchemaPtr Schema_;

    static i64 TimestampToSecond(TTimestamp timestamp)
    {
        return TimestampToInstant(timestamp).first.Seconds();
    }
};

////////////////////////////////////////////////////////////////////////////////

IVersionedRowDigestBuilderPtr CreateVersionedRowDigestBuilder(
    const TTDigestConfigPtr& config,
    const TTableSchemaPtr& schema)
{
    if (!config) {
        return nullptr;
    }

    return New<TVersionedRowDigestBuilder>(config, schema);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TVersionedRowDigestExt* protoDigest, const TVersionedRowDigest& digest)
{
    using NYT::ToProto;

    ToProto(protoDigest->mutable_earliest_nth_timestamp(), digest.EarliestNthTimestamp);
    ToProto(
        protoDigest->mutable_earliest_aggregate_or_delete_nth_timestamp(),
        digest.EarliestAggregateOrDeleteNthTimestamp);

    ToProto(
        protoDigest->mutable_last_timestamp_digest(),
        digest.LastTimestampDigest->Serialize());
    ToProto(
        protoDigest->mutable_all_but_last_timestamp_digest(),
        digest.AllButLastTimestampDigest->Serialize());
    ToProto(
        protoDigest->mutable_first_timestamp_digest(),
        digest.FirstTimestampDigest->Serialize());
}

void FromProto(TVersionedRowDigest* digest, const NProto::TVersionedRowDigestExt& protoDigest)
{
    using NYT::FromProto;

    FromProto(&digest->EarliestNthTimestamp, protoDigest.earliest_nth_timestamp());
    FromProto(&digest->EarliestAggregateOrDeleteNthTimestamp,
        protoDigest.earliest_aggregate_or_delete_nth_timestamp());

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
    if (protoDigest.has_first_timestamp_digest()) {
        auto serialized = TStringBuf(
            protoDigest.first_timestamp_digest().begin(),
            protoDigest.first_timestamp_digest().end());
        digest->FirstTimestampDigest = LoadQuantileDigest(serialized);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
