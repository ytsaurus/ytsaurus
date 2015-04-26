#pragma once

#include "public.h"
#include "ref.h"
#include "property.h"
#include "protobuf_helpers.h"

#include <util/system/defaults.h>

#include <util/generic/noncopyable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TBloomFilter
    : private TNonCopyable
{
public:
    TBloomFilter() = default;
    TBloomFilter(i64 capacity, double falsePositiveRate);
    TBloomFilter(i64 capacity, int hashCount, double bitsPerItem);
    TBloomFilter(TSharedMutableRef data, int hashCount, double bitsPerItem = -1);

    TBloomFilter& operator=(TBloomFilter&& other);

    bool Contains(TFingerprint fingerprint) const;
    void Insert(TFingerprint fingerprint);

    static int GetVersion();
    i64 EstimateSize() const;
    i64 Size() const;

    bool IsValid() const;
    void Shrink();

    DEFINE_BYVAL_RO_PROPERTY(int, HashCount);
    DEFINE_BYVAL_RO_PROPERTY(double, BitsPerItem);

protected:
    int InsertionCount_ = 0;
    int LogSize_;
    TSharedMutableRef Data_;

    int BitPosition(int position) const;
    int BytePosition(int position) const;
    bool IsBitSet(int position) const;
    void SetBit(int position);
    int EstimateLogSize() const;
    TSharedRef Bitmap() const;

    static int Log2(int size);
    static int HashCountFromRate(double falsePositiveRate);
    static double BitsPerItemFromRate(double falsePositiveRate);

    friend void ToProto(NProto::TBloomFilter* protoBloomFilter, const TBloomFilter& bloomFilter);
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TBloomFilter* protoBloomFilter, const TBloomFilter& bloomFilter);
void FromProto(TBloomFilter* bloomFilter, const NProto::TBloomFilter& protoBloomFilter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

