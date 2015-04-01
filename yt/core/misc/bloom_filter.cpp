#include "stdafx.h"
#include "blob.h"
#include "bloom_filter.h"
#include "error.h"

#include <core/misc/bloom_filter.pb.h>

#include <cmath>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TBloomFilterTag { };

TBloomFilter::TBloomFilter(i64 capacity, double falsePositiveRate)
    : TBloomFilter(
        capacity,
        HashCountFromRate(falsePositiveRate),
        BitsPerItemFromRate(falsePositiveRate))
{ }

TBloomFilter::TBloomFilter(i64 capacity, int hashCount, double bitsPerItem)
    : TBloomFilter(
        TSharedRef::Allocate<TBloomFilterTag>(1ULL << Log2(capacity * bitsPerItem / 8)),
        hashCount,
        bitsPerItem)
{ }

TBloomFilter::TBloomFilter(TSharedRef data, int hashCount, double bitsPerItem)
    : HashCount_(hashCount)
    , BitsPerItem_(bitsPerItem)
    , LogSize_(Log2(data.Size()))
    , Data_(std::move(data))
{
    if (Data_.Size() != (1ULL << LogSize_)) {
        THROW_ERROR_EXCEPTION("Incorrect Bloom Filter size %v, expected a power of two",
            Data_.Size());
    }
}

TBloomFilter& TBloomFilter::operator=(TBloomFilter&& other)
{
    HashCount_ = other.HashCount_;
    BitsPerItem_ = other.BitsPerItem_;
    InsertionCount_ = other.InsertionCount_;
    LogSize_ = other.LogSize_;
    Data_ = std::move(other.Data_);
    return *this;
}

int TBloomFilter::GetVersion()
{
    return 1;
}

TSharedRef TBloomFilter::Bitmap() const
{
    return Data_.Trim(Size());
}

bool TBloomFilter::Contains(TFingerprint fingerprint) const
{
    int hash1 = static_cast<int>(fingerprint >> 32);
    int hash2 = static_cast<int>(fingerprint);

    for (int index = 0; index < HashCount_; ++index) {
        if (!IsBitSet(hash1 + index * hash2)) {
            return false;
        }
    }

    return true;
}

void TBloomFilter::Insert(TFingerprint fingerprint)
{
    YCHECK(BitsPerItem_ > 0 && Size() > 0);

    if (Contains(fingerprint)) {
        return;
    }

    int hash1 = static_cast<int>(fingerprint >> 32);
    int hash2 = static_cast<int>(fingerprint);

    for (int index = 0; index < HashCount_; ++index) {
        SetBit(hash1 + index * hash2);
    }

    ++InsertionCount_;
}

bool TBloomFilter::IsBitSet(int position) const
{
    return Data_.Begin()[BytePosition(position)] & (1 << BitPosition(position));
}

void TBloomFilter::SetBit(int position)
{
    Data_.Begin()[BytePosition(position)] |= (1 << BitPosition(position));
}

int TBloomFilter::BitPosition(int position) const
{
    return position & 7;
}

int TBloomFilter::BytePosition(int position) const
{
    return (position >> 3) & (Size() - 1);
}

i64 TBloomFilter::Size() const
{
    return 1LL << LogSize_;
}

int TBloomFilter::Log2(int size)
{
    int result = 0;
    while (size >> 1 != 0) {
        size >>= 1;
        ++result;
    }
    return result;
}

bool TBloomFilter::IsValid() const
{
    return Data_.Size() > 0 && InsertionCount_ * HashCount_ <= Size() * 8;
}

int TBloomFilter::EstimateLogSize() const
{
    i64 size = Size();
    int sizeLog = LogSize_;
    while ((size << 2) >= InsertionCount_ * BitsPerItem_) {
        size >>= 1;
        --sizeLog;
    }
    return sizeLog;
}

i64 TBloomFilter::EstimateSize() const
{
    return 1ULL << EstimateLogSize();
}

void TBloomFilter::Shrink()
{
    int sizeLog = EstimateLogSize();
    i64 size = 1LL << sizeLog;

    if (sizeLog < LogSize_) {
        auto sizeMask = size - 1;

        for (int index = size; index < Size(); ++index) {
            Data_.Begin()[index & sizeMask] |= Data_.Begin()[index];
        }

        LogSize_ = sizeLog;
    }
}

int TBloomFilter::HashCountFromRate(double falsePositiveRate)
{
    YCHECK(falsePositiveRate > 0 && falsePositiveRate <= 1);
    int hashCount = std::log2(1.0 / falsePositiveRate);
    return hashCount > 0 ? hashCount : 1;
}

double TBloomFilter::BitsPerItemFromRate(double falsePositiveRate)
{
    YCHECK(falsePositiveRate > 0 && falsePositiveRate <= 1);
    return 1.44 * std::log2(1.0 / falsePositiveRate);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TBloomFilter* protoBloomFilter, const TBloomFilter& bloomFilter)
{
    protoBloomFilter->set_version(bloomFilter.GetVersion());
    protoBloomFilter->set_hash_count(bloomFilter.GetHashCount());
    protoBloomFilter->set_bitmap(ToString(bloomFilter.Bitmap()));
}

void FromProto(TBloomFilter* bloomFilter, const NProto::TBloomFilter& protoBloomFilter)
{
    if (protoBloomFilter.version() != TBloomFilter::GetVersion()) {
        THROW_ERROR_EXCEPTION("Bloom filter version mismatch: expected %v, got %v",
            TBloomFilter::GetVersion(),
            protoBloomFilter.version());
    }

    *bloomFilter = TBloomFilter(
        TSharedRef::FromString<TBloomFilterTag>(protoBloomFilter.bitmap()),
        protoBloomFilter.hash_count());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

