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
    return
        !ReadLimit_.has_chunk_index() &&
        !ReadLimit_.has_row_index() &&
        !ReadLimit_.has_key() &&
        !ReadLimit_.has_offset();
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

bool IsNontrivial(const TReadLimit& limit)
{
    return IsNontrivial(limit.AsProto());
}

bool IsTrivial(const TReadLimit& limit)
{
    return IsTrivial(limit.AsProto());
}

bool IsNontrivial(const NProto::TReadLimit& limit)
{
    return
        limit.has_row_index() ||
        limit.has_key() ||
        limit.has_chunk_index() ||
        limit.has_offset();
}

bool IsTrivial(const NProto::TReadLimit& limit)
{
    return !IsNontrivial(limit);
}

void ToProto(NProto::TReadLimit* protoReadLimit, const TReadLimit& readLimit)
{
    protoReadLimit->CopyFrom(readLimit.AsProto());
}

void FromProto(TReadLimit* readLimit, const NProto::TReadLimit& protoReadLimit)
{
    *readLimit = protoReadLimit;
}

void Serialize(const TReadLimit& readLimit, IYsonConsumer* consumer)
{
    int fieldCount = 0;
    if (readLimit.HasRowIndex())   { fieldCount += 1; }
    if (readLimit.HasKey())        { fieldCount += 1; }
    if (readLimit.HasChunkIndex()) { fieldCount += 1; }
    if (readLimit.HasOffset())     { fieldCount += 1; }

    if (fieldCount == 0) {
        THROW_ERROR_EXCEPTION("Cannot serialize empty read limit");
    }
    if (fieldCount >= 2) {
        THROW_ERROR_EXCEPTION("Cannot serialize read limit with more than one field");
    }

    consumer->OnBeginMap();
    if (readLimit.HasRowIndex()) {
        consumer->OnKeyedItem("row_index");
        consumer->OnIntegerScalar(readLimit.GetRowIndex());
    } else if (readLimit.HasChunkIndex()) {
        consumer->OnKeyedItem("chunk_index");
        consumer->OnIntegerScalar(readLimit.GetChunkIndex());
    } else if (readLimit.HasOffset()) {
        consumer->OnKeyedItem("offset");
        consumer->OnIntegerScalar(readLimit.GetOffset());
    } else if (readLimit.HasKey()) {
        consumer->OnKeyedItem("key");
        Serialize(readLimit.GetKey(), consumer);
    }
    consumer->OnEndMap();
}

void Deserialize(TReadLimit& readLimit, INodePtr node)
{
    if (node->GetType() != ENodeType::Map) {
        THROW_ERROR_EXCEPTION("Unexpected read limit token type %s",
            ~FormatEnum(node->GetType()).Quote());
    }

    auto mapNode = node->AsMap();
    if (mapNode->GetChildCount() > 1) {
        THROW_ERROR_EXCEPTION("Too many children in read limit: %d > 1",
            mapNode->GetChildCount());
    }

    if (auto child = mapNode->FindChild("row_index")) {
        if (child->GetType() != ENodeType::Integer) {
            THROW_ERROR_EXCEPTION("Unexpected row index token type %s",
                ~FormatEnum(child->GetType()).Quote());
        }
        readLimit.SetRowIndex(child->GetValue<i64>());
    } else if (auto child = mapNode->FindChild("chunk_index")) {
        if (child->GetType() != ENodeType::Integer) {
            THROW_ERROR_EXCEPTION("Unexpected chunk index token type %s",
                ~FormatEnum(child->GetType()).Quote());
        }
        readLimit.SetChunkIndex(child->GetValue<i64>());
    } else if (auto child = mapNode->FindChild("offset")) {
        if (child->GetType() != ENodeType::Integer) {
            THROW_ERROR_EXCEPTION("Unexpected chunk index token type %s",
                ~FormatEnum(child->GetType()).Quote());
        }
        readLimit.SetOffset(child->GetValue<i64>());
    } else if (auto child = mapNode->FindChild("key")) {
        TOwningKey key;
        Deserialize(key, child);
        readLimit.SetKey(std::move(key));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
