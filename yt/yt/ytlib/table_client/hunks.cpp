#include "hunks.h"

#include "cached_versioned_chunk_meta.h"

#include <yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/core/misc/chunked_memory_pool.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NYson;
using namespace NYTree;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

void ToProto(NTableClient::NProto::THunkChunkRef* protoRef, const THunkChunkRef& ref)
{
    ToProto(protoRef->mutable_chunk_id(), ref.ChunkId);
    protoRef->set_hunk_count(ref.HunkCount);
    protoRef->set_total_hunk_length(ref.TotalHunkLength);
}

void FromProto(THunkChunkRef* ref, const NTableClient::NProto::THunkChunkRef& protoRef)
{
    ref->ChunkId = FromProto<TChunkId>(protoRef.chunk_id());
    ref->HunkCount = protoRef.hunk_count();
    ref->TotalHunkLength = protoRef.total_hunk_length();
}

void Serialize(const THunkChunkRef& ref, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("chunk_id").Value(ref.ChunkId)
            .Item("hunk_count").Value(ref.HunkCount)
            .Item("total_hunk_length").Value(ref.TotalHunkLength)
        .EndMap();
}

void FormatValue(TStringBuilderBase* builder, const THunkChunkRef& ref, TStringBuf /*spec*/)
{
    builder->AppendFormat("{ChunkId: %v, HunkCount: %v, TotalHunkLength: %v}",
        ref.ChunkId,
        ref.HunkCount,
        ref.TotalHunkLength);
}

TString ToString(const THunkChunkRef& ref)
{
    return ToStringViaBuilder(ref);
}

////////////////////////////////////////////////////////////////////////////////

TRef WriteHunkValue(TChunkedMemoryPool* pool, const TInlineHunkValue& value)
{
    if (value.Payload.Size() == 0) {
        return TRef::MakeEmpty();
    }

    auto size =
        sizeof(ui8) +         // tag
        value.Payload.Size(); // payload
    auto* beginPtr = pool->AllocateUnaligned(size);
    auto* currentPtr = beginPtr;
    *currentPtr++ = static_cast<char>(EHunkValueTag::Inline);          // tag
    ::memcpy(currentPtr, value.Payload.Begin(), value.Payload.Size()); // payload
    currentPtr += value.Payload.Size();
    return TRef(beginPtr, currentPtr);
}

TRef WriteHunkValue(TChunkedMemoryPool* pool, const TLocalRefHunkValue& value)
{
    char* beginPtr =  pool->AllocateUnaligned(MaxLocalHunkRefSize);
    char* endPtr =  beginPtr + MaxLocalHunkRefSize;
    auto* currentPtr = beginPtr;
    *currentPtr++ = static_cast<char>(EHunkValueTag::LocalRef);                    // tag
    currentPtr += WriteVarUint32(currentPtr, static_cast<ui32>(value.ChunkIndex)); // chunkIndex
    currentPtr += WriteVarUint64(currentPtr, static_cast<ui64>(value.Length));     // length
    currentPtr += WriteVarUint64(currentPtr, static_cast<ui64>(value.Offset));     // offset
    pool->Free(currentPtr, endPtr);
    return TRef(beginPtr, currentPtr);
}

TRef WriteHunkValue(TChunkedMemoryPool* pool, const TGlobalRefHunkValue& value)
{
    char* beginPtr =  pool->AllocateUnaligned(MaxGlobalHunkRefSize);
    char* endPtr =  beginPtr + MaxGlobalHunkRefSize;
    auto* currentPtr = beginPtr;
    *currentPtr++ = static_cast<char>(EHunkValueTag::GlobalRef);               // tag
    ::memcpy(currentPtr, &value.ChunkId, sizeof(TChunkId));                    // chunkId
    currentPtr += sizeof(TChunkId);
    currentPtr += WriteVarUint64(currentPtr, static_cast<ui64>(value.Length)); // length
    currentPtr += WriteVarUint64(currentPtr, static_cast<ui64>(value.Offset)); // offset
    pool->Free(currentPtr, endPtr);
    return TRef(beginPtr, currentPtr);
}

THunkValue ReadHunkValue(TRef input)
{
    if (input.Size() == 0) {
        return TInlineHunkValue{
            .Payload = TRef::MakeEmpty()
        };
    }

    const char* currentPtr = input.Begin();
    switch (auto tag = *currentPtr++) {
        case static_cast<char>(EHunkValueTag::Inline):
            return TInlineHunkValue{
                .Payload = TRef(currentPtr, input.End())
            };

        case static_cast<char>(EHunkValueTag::LocalRef): {
            ui32 chunkIndex;
            ui64 length;
            ui64 offset;
            currentPtr += ReadVarUint32(currentPtr, &chunkIndex);
            currentPtr += ReadVarUint64(currentPtr, &length);
            currentPtr += ReadVarUint64(currentPtr, &offset);
            // TODO(babenko): better out-of-bounds check.
            if (currentPtr > input.End()) {
                THROW_ERROR_EXCEPTION("Malformed local ref hunk value");
            }
            return TLocalRefHunkValue{
                .ChunkIndex = static_cast<int>(chunkIndex),
                .Length = static_cast<i64>(length),
                .Offset = static_cast<i64>(offset)
            };
        }

        case static_cast<char>(EHunkValueTag::GlobalRef): {
            TChunkId chunkId;
            ui64 length;
            ui64 offset;
            ::memcpy(&chunkId, currentPtr, sizeof(TChunkId));
            currentPtr += sizeof(TChunkId);
            currentPtr += ReadVarUint64(currentPtr, &length);
            currentPtr += ReadVarUint64(currentPtr, &offset);
            // TODO(babenko): better out-of-bounds check.
            if (currentPtr > input.End()) {
                THROW_ERROR_EXCEPTION("Malformed global ref hunk value");
            }
            return TGlobalRefHunkValue{
                .ChunkId = chunkId,
                .Length = static_cast<i64>(length),
                .Offset = static_cast<i64>(offset)
            };
        }

        default:
            THROW_ERROR_EXCEPTION("Invalid hunk value tag %v", tag);
    }
}

void GlobalizeHunkValues(
    TChunkedMemoryPool* pool,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    TMutableVersionedRow row)
{
    if (!row) {
        return;
    }
    const auto& hunkChunkRefsExt = chunkMeta->HunkChunkRefsExt();
    const auto& schemaColumns = chunkMeta->GetSchema()->Columns();
    for (int index = 0; index < row.GetValueCount(); ++index) {
        auto& value = row.BeginValues()[index];
        if (!schemaColumns[value.Id].MaxInlineHunkSize()) {
            continue;
        }
        auto hunkValue = ReadHunkValue(TRef(value.Data.String, value.Length));
        if (const auto* localRefHunkValue = std::get_if<TLocalRefHunkValue>(&hunkValue)) {
            auto globalRefHunkValue = TGlobalRefHunkValue{
                .ChunkId = FromProto<TChunkId>(hunkChunkRefsExt.refs(localRefHunkValue->ChunkIndex).chunk_id()),
                .Length = localRefHunkValue->Length,
                .Offset = localRefHunkValue->Offset
            };
            auto globalRefPayload = WriteHunkValue(pool, globalRefHunkValue);
            value.Data.String = globalRefPayload.Begin();
            value.Length = globalRefPayload.Size();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
