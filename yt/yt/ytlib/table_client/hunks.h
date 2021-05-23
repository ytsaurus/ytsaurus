#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/core/misc/ref.h>
#include <yt/yt/core/misc/varint.h>

#include <yt/yt/core/yson/public.h>

#include <variant>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct THunkChunkRef
{
    NChunkClient::TChunkId ChunkId;
    i64 HunkCount = 0;
    i64 TotalHunkLength = 0;
};

void ToProto(NTableClient::NProto::THunkChunkRef* protoRef, const THunkChunkRef& ref);
void FromProto(THunkChunkRef* ref, const NTableClient::NProto::THunkChunkRef& protoRef);

void Serialize(const THunkChunkRef& ref, NYson::IYsonConsumer* consumer);

void FormatValue(TStringBuilderBase* builder, const THunkChunkRef& ref, TStringBuf spec);
TString ToString(const THunkChunkRef& ref);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EHunkValueTag, ui8,
    ((Inline)   (0))
    ((LocalRef) (1))
    ((GlobalRef)(2))
);

/*
 *  Hunk value format
 *  =================
 *
 *  Empty values are encoded as-is.
 *
 *  Non-empty values have the following layout:
 *  * tag: ui8
 *
 *  1) tag == EHunkValueTag::Inline
 *  Value payload is being stored inline.
 *  * payload: char[N]
 *
 *  2) tag == EHunkValueTag::LocalRef
 *  Value payload is moved to a hunk chunk and is referenced by index in THunkChunkRefsExt.
 *  * chunkIndex: varuint32
 *  * length: varuint64
 *  * offset: varuint64
 *
 *  3) tag == EHunkValueTag::GlobalRef
 *  Value payload is moved to a hunk chunk and is referenced by chunk id.
 *  * chunkId: TChunkId
 *  * length: varuint64
 *  * offset: varuint64
 */

struct TInlineHunkValue
{
    TRef Payload;
};

struct TLocalRefHunkValue
{
    int ChunkIndex;
    i64 Length;
    i64 Offset;
};

struct TGlobalRefHunkValue
{
    NChunkClient::TChunkId ChunkId;
    i64 Length;
    i64 Offset;
};

using THunkValue = std::variant<
    TInlineHunkValue,
    TLocalRefHunkValue,
    TGlobalRefHunkValue
>;

////////////////////////////////////////////////////////////////////////////////

constexpr auto InlineHunkRefHeaderSize =
    sizeof(ui8);       // tag

constexpr auto MaxLocalHunkRefSize =
    sizeof(ui8) +      // tag
    MaxVarUint32Size + // chunkIndex
    MaxVarUint64Size + // length
    MaxVarUint64Size;  // offset

constexpr auto MaxGlobalHunkRefSize =
    sizeof(ui8) +                    // tag
    sizeof(NChunkClient::TChunkId) + // chunkId
    MaxVarUint64Size +               // length
    MaxVarUint64Size;                // offset

////////////////////////////////////////////////////////////////////////////////

TRef WriteHunkValue(TChunkedMemoryPool* pool, const TInlineHunkValue& value);
TRef WriteHunkValue(TChunkedMemoryPool* pool, const TLocalRefHunkValue& value);
TRef WriteHunkValue(TChunkedMemoryPool* pool, const TGlobalRefHunkValue& value);

size_t GetInlineHunkValueSize(const TInlineHunkValue& value);
TRef WriteHunkValue(char* ptr, const TInlineHunkValue& value);

THunkValue ReadHunkValue(TRef input);

void GlobalizeHunkValues(
    TChunkedMemoryPool* pool,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    TMutableVersionedRow row);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
