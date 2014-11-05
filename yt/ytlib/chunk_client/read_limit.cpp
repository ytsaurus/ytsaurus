#include "stdafx.h"

#include "read_limit.h"

#include <core/ytree/node.h>
#include <core/ytree/convert.h>
#include <core/ytree/fluent.h>

namespace NYT {
namespace NChunkClient {

using namespace NYTree;
using namespace NYson;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

TReadLimit::TReadLimit()
{ }

TReadLimit::TReadLimit(const NProto::TReadLimit& protoLimit)
{
    InitCopy(protoLimit);
}

TReadLimit::TReadLimit(NProto::TReadLimit&& protoLimit)
{
    InitMove(std::move(protoLimit));
}

TReadLimit& TReadLimit::operator= (const NProto::TReadLimit& protoLimit)
{
    InitCopy(protoLimit);
    return *this;
}

TReadLimit& TReadLimit::operator= (NProto::TReadLimit&& protoLimit)
{
    InitMove(std::move(protoLimit));
    return *this;
}

const NProto::TReadLimit& TReadLimit::AsProto() const
{
    return ReadLimit_;
}

const TOwningKey& TReadLimit::GetKey() const
{
    YASSERT(HasKey());
    return Key_;
}

bool TReadLimit::HasKey() const
{
    return ReadLimit_.has_key();
}

void TReadLimit::SetKey(const TOwningKey& key)
{
    Key_ = key;
    ToProto(ReadLimit_.mutable_key(), Key_);
}

void TReadLimit::SetKey(TOwningKey&& key)
{
    swap(Key_, key);
    ToProto(ReadLimit_.mutable_key(), Key_);
}

i64 TReadLimit::GetRowIndex() const
{
    YASSERT(HasRowIndex());
    return ReadLimit_.row_index();
}

bool TReadLimit::HasRowIndex() const
{
    return ReadLimit_.has_row_index();
}

void TReadLimit::SetRowIndex(i64 rowIndex)
{
    ReadLimit_.set_row_index(rowIndex);
}

i64 TReadLimit::GetOffset() const
{
    YASSERT(HasOffset());
    return ReadLimit_.offset();
}

bool TReadLimit::HasOffset() const
{
    return ReadLimit_.has_offset();
}

void TReadLimit::SetOffset(i64 offset)
{
    ReadLimit_.set_offset(offset);
}

i64 TReadLimit::GetChunkIndex() const
{
    YASSERT(HasChunkIndex());
    return ReadLimit_.chunk_index();
}

bool TReadLimit::HasChunkIndex() const
{
    return ReadLimit_.has_chunk_index();
}

void TReadLimit::SetChunkIndex(i64 chunkIndex)
{
    ReadLimit_.set_chunk_index(chunkIndex);
}

bool TReadLimit::IsTrivial() const
{
    return NChunkClient::IsTrivial(ReadLimit_);
}

void TReadLimit::Persist(NPhoenix::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, ReadLimit_);
    Persist(context, Key_);
}

void TReadLimit::InitKey()
{
    if (ReadLimit_.has_key()) {
        FromProto(&Key_, ReadLimit_.key());
    }
}

void TReadLimit::InitCopy(const NProto::TReadLimit& readLimit)
{
    ReadLimit_.CopyFrom(readLimit);
    InitKey();
}

void TReadLimit::InitMove(NProto::TReadLimit&& readLimit)
{
    ReadLimit_.Swap(&readLimit);
    InitKey();
}

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const TReadLimit& limit)
{
    using ::ToString;

    Stroka result;
    auto append = [&] (const TStringBuf& part) {
        if (result.empty()) {
            result.append(" ");
            result.append(part);
        }
    };

    if (limit.HasKey()) {
        append("Key: ");
        append(ToString(limit.GetKey()));
    }

    if (limit.HasRowIndex()) {
        append("RowIndex: ");
        append(ToString(limit.GetRowIndex()));
    }

    if (limit.HasOffset()) {
        append("Offset: ");
        append(ToString(limit.GetOffset()));
    }

    if (limit.HasChunkIndex()) {
        append("ChunkIndex: ");
        append(ToString(limit.GetOffset()));
    }

    return result;
}

bool IsTrivial(const TReadLimit& limit)
{
    return limit.IsTrivial();
}

bool IsTrivial(const NProto::TReadLimit& limit)
{
    return
        !limit.has_row_index() &&
        !limit.has_key() &&
        !limit.has_offset() &&
        !limit.has_chunk_index();
}

void ToProto(NProto::TReadLimit* protoReadLimit, const TReadLimit& readLimit)
{
    protoReadLimit->CopyFrom(readLimit.AsProto());
}

void FromProto(TReadLimit* readLimit, const NProto::TReadLimit& protoReadLimit)
{
    *readLimit = protoReadLimit;
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TReadLimit& readLimit, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .DoIf(readLimit.HasKey(), [&] (TFluentMap fluent) {
                fluent.Item("key").Value(readLimit.GetKey());
            })
            .DoIf(readLimit.HasRowIndex(), [&] (TFluentMap fluent) {
                fluent.Item("row_index").Value(readLimit.GetRowIndex());
            })
            .DoIf(readLimit.HasOffset(), [&] (TFluentMap fluent) {
                fluent.Item("offset").Value(readLimit.GetOffset());
            })
            .DoIf(readLimit.HasChunkIndex(), [&] (TFluentMap fluent) {
                fluent.Item("chunk_index").Value(readLimit.GetChunkIndex());
            })
        .EndMap();
}

void Deserialize(TReadLimit& readLimit, INodePtr node)
{
    readLimit = TReadLimit();
    auto attributes = ConvertToAttributes(node);
    if (attributes->Contains("key")) {
        readLimit.SetKey(attributes->Get<TOwningKey>("key"));
    }
    if (attributes->Contains("row_index")) {
        readLimit.SetRowIndex(attributes->Get<i64>("row_index"));
    }
    if (attributes->Contains("offset")) {
        readLimit.SetOffset(attributes->Get<i64>("offset"));
    }
    if (attributes->Contains("chunk_index")) {
        readLimit.SetChunkIndex(attributes->Get<i64>("chunk_index"));
    }
}

////////////////////////////////////////////////////////////////////////////////

TReadRange::TReadRange()
    : LowerLimit_(TReadLimit())
    , UpperLimit_(TReadLimit())
{ }

TReadRange::TReadRange(const TReadLimit& lowerLimit, const TReadLimit& upperLimit)
    : LowerLimit_(lowerLimit)
    , UpperLimit_(upperLimit)
{ }

TReadRange::TReadRange(const NProto::TReadRange& range)
{
    InitCopy(range);
}

TReadRange::TReadRange(NProto::TReadRange&& range)
{
    InitMove(std::move(range));
}

TReadRange& TReadRange::operator= (const NProto::TReadRange& range)
{
    InitCopy(range);
    return *this;
}

TReadRange& TReadRange::operator= (NProto::TReadRange&& range)
{
    InitMove(std::move(range));
    return *this;
}

void TReadRange::InitCopy(const NProto::TReadRange& range)
{
    LowerLimit_ = range.has_lower_limit() ? TReadLimit(range.lower_limit()) : TReadLimit();
    UpperLimit_ = range.has_upper_limit() ? TReadLimit(range.upper_limit()) : TReadLimit();
}

void TReadRange::InitMove(NProto::TReadRange&& range)
{
    LowerLimit_ = std::move(range.has_lower_limit() ? TReadLimit(range.lower_limit()) : TReadLimit());
    UpperLimit_ = std::move(range.has_upper_limit() ? TReadLimit(range.upper_limit()) : TReadLimit());
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TReadRange* protoReadRange, const TReadRange& readRange)
{
    if (!readRange.LowerLimit().IsTrivial()) {
        *protoReadRange->mutable_lower_limit() = NYT::ToProto<NProto::TReadLimit>(readRange.LowerLimit());
    }
    if (!readRange.UpperLimit().IsTrivial()) {
        *protoReadRange->mutable_upper_limit() = NYT::ToProto<NProto::TReadLimit>(readRange.UpperLimit());
    }
}

void FromProto(TReadRange* readRange, const NProto::TReadRange& protoReadRange)
{
    *readRange = TReadRange(protoReadRange);
}

void Serialize(const TReadRange& readRange, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .DoIf(!readRange.LowerLimit().IsTrivial(), [&] (TFluentMap fluent) {
                fluent.Item("lower_limit").Value(readRange.LowerLimit());
            })
            .DoIf(!readRange.UpperLimit().IsTrivial(), [&] (TFluentMap fluent) {
                fluent.Item("upper_limit").Value(readRange.UpperLimit());
            })
        .EndMap();

}

void Deserialize(TReadRange& readRange, NYTree::INodePtr node)
{
    readRange = TReadRange();
    auto attributes = ConvertToAttributes(node);
    if (attributes->Contains("lower_limit")) {
        readRange.LowerLimit() = attributes->Get<TReadLimit>("lower_limit");
    }
    if (attributes->Contains("upper_limit")) {
        readRange.UpperLimit() = attributes->Get<TReadLimit>("upper_limit");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
