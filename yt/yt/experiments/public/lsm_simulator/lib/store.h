#pragma once

#include "row.h"
#include "public.h"

#include <yt/yt/server/lib/lsm/store.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

class TStore
    : public NLsm::TStore
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TPartition*, Partition);

    TStore()
    {
        static int Counter = 1;
        Id_.Parts32[0] = Counter++;
        Id_.Parts32[1] = 0x64;
        Id_.Parts32[2] = 1;
        Id_.Parts32[3] = EpochIndex;

        Type_ = EStoreType::SortedChunk;
        StoreState_ = EStoreState::Persistent;
    }

    std::unique_ptr<NLsm::TStore> ToLsmStore(NLsm::TTablet* tablet)
    {
        this->NLsm::TStore::Tablet_ = tablet;
        // XXX
        SetIsCompactable(true);

        // NB: We return unique_ptr(this) but ensure that owner will
        // call .reset() on it before destructing.
        return std::unique_ptr<NLsm::TStore>(this);
    }

    void SetRows(std::vector<TRow> rows, double compressionRatio)
    {
        std::sort(rows.begin(), rows.end());
        for (int index = 0; index + 1 < ssize(rows); ++index) {
            YT_VERIFY(rows[index] < rows[index + 1]);
        }

        Rows_ = std::move(rows);

        CompressedDataSize_ = 0;
        UncompressedDataSize_ = 0;
        RowCount_ = 0;
        MinTimestamp_ = NTransactionClient::MaxTimestamp;
        MaxTimestamp_ = NTransactionClient::MinTimestamp;
        MinKey_ = NTableClient::MaxKey();
        UpperBoundKey_ = NTableClient::EmptyKey();

        for (const auto& row : Rows_) {
            UncompressedDataSize_ += row.DataSize;
            RowCount_ += 1;
            for (const auto& value : row.Values) {
                MinTimestamp_ = std::min(MinTimestamp_, value.Timestamp);
                MaxTimestamp_ = std::max(MaxTimestamp_, value.Timestamp);
            }

            NTableClient::TUnversionedOwningRowBuilder builder;
            builder.AddValue(NTableClient::MakeUnversionedInt64Value(row.Key));
            auto key = builder.FinishRow();

            MinKey_ = ChooseMinKey(MinKey_, key);
            auto successor = GetKeySuccessor(key);
            UpperBoundKey_ = ChooseMaxKey(UpperBoundKey_, successor);
        }
        CompressedDataSize_ = UncompressedDataSize_ * compressionRatio;
    }

    const auto& Rows() const
    {
        return Rows_;
    }

    void Persist(const TStreamPersistenceContext& context);

private:
    std::vector<TRow> Rows_;
};

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE auto MakeStore(std::vector<TRow> rows, double compressionRatio)
{
    auto store = std::make_unique<TStore>();
    store->SetRows(std::move(rows), compressionRatio);
    store->SetIsCompactable(true);
    return store;
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE IOutputStream& operator<<(IOutputStream& out, const TStore& store)
{
    out << ToString(store.GetId()) << " (" << store.GetCompressedDataSize() / (1 << 20) << " MB) " <<
        "[" << ToString(store.MinKey()) << " .. " << ToString(store.UpperBoundKey()) << "]";
    return out;
    out << ", [";
    bool first = true;
    for (const auto& row : store.Rows()) {
        if (first) {
            first = false;
        } else {
            out << ", ";
        }
        out << row.Key;
    }
    return out << "]";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
