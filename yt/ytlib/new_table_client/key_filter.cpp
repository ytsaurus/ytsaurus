#include "stdafx.h"
#include "public.h"
#include "private.h"
#include "key_filter.h"
#include "unversioned_row.h"

#include <core/misc/protobuf_helpers.h>
#include <core/misc/farm_hash.h>

#include <core/logging/log.h>

#include <yt/ytlib/new_table_client/chunk_meta.pb.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

TKeyFilter::TKeyFilter(i64 capacity, double falsePositiveRate)
    : Bloom_(std::make_unique<TBloomFilter>(capacity, falsePositiveRate))
{ }

TKeyFilter::TKeyFilter(std::unique_ptr<TBloomFilter> bloomFilter)
    : Bloom_(std::move(bloomFilter))
{ }

TKeyFilter& TKeyFilter::operator=(TKeyFilter&& other)
{
    Bloom_ = std::move(other.Bloom_);
    return *this;
}

bool TKeyFilter::Contains(const TKey& key) const
{
    return Contains(key.Begin(), key.End());
}

bool TKeyFilter::Contains(const TUnversionedValue* begin, const TUnversionedValue* end) const
{
    return Bloom_ ? Bloom_->Contains(GetFarmFingerprint(begin, end)) : true;
}

void TKeyFilter::Insert(const TKey& key)
{
    Insert(key.Begin(), key.End());
}

void TKeyFilter::Insert(const TUnversionedValue* begin, const TUnversionedValue* end)
{
    if (Bloom_) {
        Bloom_->Insert(GetFarmFingerprint(begin, end));
    }
}

bool TKeyFilter::IsValid() const
{
    return Bloom_ ? Bloom_->IsValid() : false;
}

i64 TKeyFilter::EstimateSize() const
{
    return (Bloom_ && Bloom_->IsValid()) ? Bloom_->EstimateSize() : 0;
}

void TKeyFilter::Shrink()
{
    if (Bloom_) {
        Bloom_->Shrink();
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TKeyFilterExt* protoKeyFilter, const TKeyFilter& keyFilter)
{
    if (keyFilter.IsValid()) {
        ToProto(protoKeyFilter->mutable_bloom_filter(), *keyFilter.Bloom_);
    }
}

void FromProto(TKeyFilter* keyFilter, const NProto::TKeyFilterExt& protoKeyFilter)
{
    if (protoKeyFilter.has_bloom_filter()) {
        auto bloomFilter = std::make_unique<TBloomFilter>();
        FromProto(bloomFilter.get(), protoKeyFilter.bloom_filter());
        if (bloomFilter->IsValid()) {
            *keyFilter = TKeyFilter(std::move(bloomFilter));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

