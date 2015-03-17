#pragma once

#include "public.h"

#include <core/misc/bloom_filter.h>

#include <memory>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TKeyFilter
    : private TNonCopyable
{
public:
    TKeyFilter() = default;
    TKeyFilter(i64 capacity, double falsePositiveRate);
    explicit TKeyFilter(std::unique_ptr<TBloomFilter> bloomFilter);

    TKeyFilter& operator=(TKeyFilter&& other);

    bool Contains(const TKey& key) const;
    bool Contains(const TUnversionedValue* begin, const TUnversionedValue* end) const;
    void Insert(const TKey& key);
    void Insert(const TUnversionedValue* begin, const TUnversionedValue* end);
    i64 EstimateSize() const;
    bool IsValid() const;
    void Shrink();

protected:
    friend void ToProto(NProto::TKeyFilterExt* protoKeyFilter, const TKeyFilter& keyFilter);

    std::unique_ptr<TBloomFilter> Bloom_;
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TKeyFilterExt* protoKeyFilter, const TKeyFilter& keyFilter);
void FromProto(TKeyFilter* keyFilter, const NProto::TKeyFilterExt& protoKeyFilter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

