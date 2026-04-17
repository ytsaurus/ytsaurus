#include "min_hash_digest.h"
#include "config.h"

#include <yt/yt/core/misc/memory_usage_tracker.h>
#include <yt/yt/core/misc/serialize.h>

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

template <class TTimestampComparator>
bool TTimestampedHash<TTimestampComparator>::operator<(const TTimestampedHash& other) const
{
    return Hash == other.Hash
        ? PrimaryTimestamp == other.PrimaryTimestamp
            ? TTimestampComparator()(AuxiliaryTimestamp, other.AuxiliaryTimestamp)
            : TTimestampComparator()(PrimaryTimestamp, other.PrimaryTimestamp)
        : Hash < other.Hash;
}

template struct TTimestampedHash<std::less<ui32>>;
template struct TTimestampedHash<std::greater<ui32>>;

////////////////////////////////////////////////////////////////////////////////

template <class TTimestampComparator>
TMinHash<TTimestampComparator>::TMinHash(int capacity, TMinHashItems<TTimestampComparator> items)
    : Capacity_(capacity)
    , Items_(std::move(items))
{ }

template <class TTimestampComparator>
i64 TMinHash<TTimestampComparator>::GetWeight() const
{
    return sizeof(TMinHash<TTimestampComparator>) +
        Items_.capacity() * sizeof(TTimestampedHash<TTimestampComparator>);
}

template <class TTimestampComparator>
i64 TMinHash<TTimestampComparator>::GetSerializedSize() const
{
    // Need second ui32 for |Items_| size.
    return sizeof(ui32) * 2 +
        Items_.size() * TTimestampedHash<TTimestampComparator>::SerializedSize;
}

template <class TTimestampComparator>
void TMinHash<TTimestampComparator>::Write(char*& ptr) const
{
    WritePod(ptr, Capacity_);
    WritePod<ui32>(ptr, Items_.size());

    for (auto item : Items_) {
        WritePod(ptr, item.Hash);
        WritePod(ptr, item.PrimaryTimestamp);
        YT_VERIFY(item.AuxiliaryTimestamp == 0);
    }
}

template <class TTimestampComparator>
void TMinHash<TTimestampComparator>::Read(const char*& ptr)
{
    ReadPod(ptr, Capacity_);

    ui32 size;
    ReadPod(ptr, size);

    Items_.resize(size);
    for (auto& item : Items_) {
        ReadPod(ptr, item.Hash);
        ReadPod(ptr, item.PrimaryTimestamp);
    }
}

template <class TTimestampComparator>
TMinHash<TTimestampComparator> TMinHash<TTimestampComparator>::Merge(
    const TMinHash<TTimestampComparator>& lhs,
    const TMinHash<TTimestampComparator>& rhs)
{
    int capacity = std::min(lhs.GetCapacity(), rhs.GetCapacity());
    TMinHashItems<TTimestampComparator> items;
    items.reserve(std::min<ui32>(capacity, lhs.Items().size() + rhs.Items().size()));

    for (auto lhsIt = lhs.Items().begin(), rhsIt = rhs.Items().begin();
        ssize(items) < capacity && (lhsIt != lhs.Items().end() || rhsIt != rhs.Items().end());)
    {
        if (lhsIt == lhs.Items().end()) {
            items.push_back(*rhsIt++);
            continue;
        }

        if (rhsIt == rhs.Items().end()) {
            items.push_back(*lhsIt++);
            continue;
        }

        bool less = *lhsIt < *rhsIt;
        bool hashEqual = lhsIt->Hash == rhsIt->Hash;

        auto& takenIt = less ? lhsIt : rhsIt;
        auto& discardedIt = less ? rhsIt : lhsIt;

        auto& addedItem = items.emplace_back(*takenIt++);

        // Can leave now, otherwise should merge timestamps and increment iterator for discarded alternative.
        if (!hashEqual) {
            continue;
        }

        // If hashes are equal, we should choose best |AuxiliaryTimestamp| for added item from
        // |AuxiliaryTimestamp| of taken alternative and |PrimaryTimestamp| of discarded alternative.
        addedItem.AuxiliaryTimestamp = addedItem.AuxiliaryTimestamp == 0
            ? discardedIt->PrimaryTimestamp
            : std::min(discardedIt->PrimaryTimestamp, addedItem.AuxiliaryTimestamp, TTimestampComparator());

        ++discardedIt;
    }

    return {capacity, std::move(items)};
}

template struct TMinHash<std::less<ui32>>;
template struct TMinHash<std::greater<ui32>>;

////////////////////////////////////////////////////////////////////////////////

template <class TTimestampComparator>
TMinHashAccumulator<TTimestampComparator>::TMinHashAccumulator(int capacity)
    : Capacity_(capacity)
{
    ItemsBuffer_.reserve(Capacity_ * 2);
}

template <class TTimestampComparator>
void TMinHashAccumulator<TTimestampComparator>::Add(TFingerprint hash, ui32 timestamp)
{
    EnsureCompacted();

    ItemsBuffer_.push_back({
        .Hash = hash,
        .PrimaryTimestamp = timestamp,
    });
}

template <class TTimestampComparator>
TMinHash<TTimestampComparator> TMinHashAccumulator<TTimestampComparator>::Finish()
{
    EnsureCompacted();

    // NB(dave11ar): shrink to fit is useless, |ItemsBuffer_| will be destroyed immediately after serializing.
    return {Capacity_, std::move(ItemsBuffer_)};
}

template <class TTimestampComparator>
void TMinHashAccumulator<TTimestampComparator>::EnsureCompacted()
{
    if (ssize(ItemsBuffer_) < Capacity_ * 2) {
        return;
    }

    std::sort(ItemsBuffer_.begin(), ItemsBuffer_.end());

    auto uniqueIterator = std::unique(
        ItemsBuffer_.begin(),
        ItemsBuffer_.end(),
        [] (const auto& lhs, const auto& rhs) {
            return lhs.Hash == rhs.Hash;
        });

    ItemsBuffer_.resize(std::min<ui64>(
        Capacity_,
        uniqueIterator - ItemsBuffer_.begin()));
}

template class TMinHashAccumulator<std::less<ui32>>;
template class TMinHashAccumulator<std::greater<ui32>>;

////////////////////////////////////////////////////////////////////////////////

TMinHashDigest::TMinHashDigest(IMemoryUsageTrackerPtr memoryTracker)
    : MemoryTracker_(std::move(memoryTracker))
{ }

TMinHashDigest::~TMinHashDigest()
{
    if (MemoryTracker_) {
        MemoryTracker_->Release(GetWeight());
    }
}

void TMinHashDigest::Initialize(TSharedRef data)
{
    YT_VERIFY(!IsInitialized());

    Initialized_ = true;

    const char* ptr = data.begin();

    ui32 formatVersion;
    ReadPod(ptr, formatVersion);
    if (formatVersion != 1) {
        THROW_ERROR_EXCEPTION("Invalid min hash digest format version %v",
            formatVersion);
    }

    WriteMinHash_.Read(ptr);
    DeleteMinHash_.Read(ptr);

    TrackMemory();
}

void TMinHashDigest::Initialize(
    TWriteMinHash&& writeMinHash,
    TDeleteMinHash&& deleleMinHash)
{
    YT_VERIFY(!IsInitialized());

    Initialized_ = true;

    WriteMinHash_ = std::move(writeMinHash);
    DeleteMinHash_ = std::move(deleleMinHash);

    TrackMemory();
}

TSharedRef TMinHashDigest::BuildSerialized() const
{
    size_t allocationSize = sizeof(ui32) + WriteMinHash_.GetSerializedSize() + DeleteMinHash_.GetSerializedSize();

    auto data = TSharedMutableRef::Allocate(allocationSize);
    char* ptr = data.begin();

    WritePod(ptr, FormatVersion);

    WriteMinHash_.Write(ptr);
    DeleteMinHash_.Write(ptr);

    return data;
}

ui32 TMinHashDigest::CalculateWriteDeleteSimilarityTimestamp(const TMinHashSimilarityConfigPtr& similarityConfig) const
{
    int comparisonPrefixSize = std::min(ssize(WriteMinHash_.Items()), ssize(DeleteMinHash_.Items()));
    if (comparisonPrefixSize < similarityConfig->MinRowCount) {
        return 0;
    }

    std::vector<ui32> intersectionTimestamps;

    for (int writeIndex = 0, deleteIndex = 0; writeIndex < comparisonPrefixSize && deleteIndex < comparisonPrefixSize;) {
        auto writeItem = WriteMinHash_.Items()[writeIndex];
        auto deleteItem = DeleteMinHash_.Items()[deleteIndex];

        if (writeItem.Hash == deleteItem.Hash) {
            if (writeItem.PrimaryTimestamp < deleteItem.PrimaryTimestamp) {
                intersectionTimestamps.push_back(deleteItem.PrimaryTimestamp);
            }

            ++writeIndex;
            ++deleteIndex;

            continue;
        }

        if (writeItem.Hash < deleteItem.Hash) {
            ++writeIndex;
        } else {
            ++deleteIndex;
        }
    }

    return CalculateSufficientTimestamp(
        similarityConfig,
        std::move(intersectionTimestamps),
        comparisonPrefixSize);
}

ui32 TMinHashDigest::CalculateWritesSimilarityTimestamp(
    const TMinHashSimilarityConfigPtr& similarityConfig,
    EMinHashWriteSimilarityMode similarityMode) const
{
    int comparisonSize = ssize(WriteMinHash_.Items());
    if (comparisonSize < similarityConfig->MinRowCount) {
        return 0;
    }

    std::vector<ui32> intersectionTimestamps;
    for (auto item : WriteMinHash_.Items()) {
        // There is only one write for current hash.
        if (item.AuxiliaryTimestamp == 0) {
            continue;
        }

        intersectionTimestamps.push_back(similarityMode == EMinHashWriteSimilarityMode::Primary
            ? item.PrimaryTimestamp
            : item.AuxiliaryTimestamp);
    }

    return CalculateSufficientTimestamp(
        similarityConfig,
        std::move(intersectionTimestamps),
        comparisonSize);
}

TMinHashDigestPtr TMinHashDigest::Merge(const TMinHashDigestPtr& lhs, const TMinHashDigestPtr& rhs)
{
    YT_VERIFY(lhs->MemoryTracker_ == rhs->MemoryTracker_);

    auto result = New<TMinHashDigest>(lhs->MemoryTracker_);
    result->Initialize(
        TWriteMinHash::Merge(lhs->WriteMinHash_, rhs->WriteMinHash_),
        TDeleteMinHash::Merge(lhs->DeleteMinHash_, rhs->DeleteMinHash_));

    return result;
}

ui32 TMinHashDigest::CalculateSufficientTimestamp(
    const TMinHashSimilarityConfigPtr& similarityConfig,
    std::vector<ui32>&& intersectionTimestamps,
    int comparisonSize) const
{
    int sufficientIntersectionSize = std::ceil(comparisonSize * similarityConfig->MinSimilarity);
    if (sufficientIntersectionSize > ssize(intersectionTimestamps)) {
        return 0;
    }

    // |std::max| to protect from extremely small |MinSimilarity|, that makes |sufficientIntersectionSize| zero.
    int sufficientIntersectionIndex = std::max(0, sufficientIntersectionSize - 1);

    std::nth_element(
        intersectionTimestamps.begin(),
        intersectionTimestamps.begin() + sufficientIntersectionIndex,
        intersectionTimestamps.end());

    return intersectionTimestamps[sufficientIntersectionIndex];
}

i64 TMinHashDigest::GetWeight() const
{
    return sizeof(TMinHashDigest) + WriteMinHash_.GetWeight() + DeleteMinHash_.GetWeight();
}

void TMinHashDigest::TrackMemory()
{
    if (MemoryTracker_) {
        // NB(dave11ar): We can acquire after initialization because tablet_background is limitless.
        MemoryTracker_->Acquire(GetWeight());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
