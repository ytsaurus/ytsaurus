#include "hunks.h"

#include "cached_versioned_chunk_meta.h"
#include "chunk_meta_extensions.h"
#include "config.h"
#include "dictionary_compression_session.h"
#include "private.h"
#include "schemaless_chunk_reader.h"
#include "schemaless_multi_chunk_reader.h"
#include "versioned_chunk_writer.h"

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/chunk_fragment_reader.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/ytlib/tablet_client/proto/tablet_service.pb.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/compression/dictionary_codec.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/checksum.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/misc/variant.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NYson;
using namespace NYTree;
using namespace NApi;
using namespace NElection;
using namespace NTabletClient;

using NYT::ToProto;
using NYT::FromProto;

using NChunkClient::NProto::TDataStatistics;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

DEFINE_ENUM(ECompressionSessionStage,
    ((WaitingOnFuture)          (0))
    ((FeedingSamples)           (1))
    ((SessionIsFed)             (2))
    ((SampledRowsAreWritten)    (3))
);

TRef GetValueRef(const TUnversionedValue& value)
{
    YT_ASSERT(IsStringLikeType(value.Type));
    return TRef(value.Data.String, value.Length);
}

void SetValueRef(TUnversionedValue* value, TRef ref)
{
    YT_ASSERT(IsStringLikeType(value->Type));
    value->Data.String = ref.Begin();
    value->Length = ref.Size();
}

class TSchemafulUnversionedRowVisitor
{
public:
    TSchemafulUnversionedRowVisitor(
        const TTableSchemaPtr& schema,
        const TColumnFilter& columnFilter)
        : HunkColumnIds_(GetHunkColumnIds(schema, columnFilter))
    { }

    template <class TRow, class F>
    void ForEachHunkValue(
        TRow row,
        const F& func) const
    {
        if (!row) {
            return;
        }
        for (auto id : HunkColumnIds_) {
            auto& value = row[id];
            if (Any(value.Flags & EValueFlags::Hunk)) {
                func(&value);
            }
        }
    }

private:
    const THunkColumnIds HunkColumnIds_;

    static THunkColumnIds GetHunkColumnIds(
        const TTableSchemaPtr& schema,
        const TColumnFilter& columnFilter)
    {
        if (columnFilter.IsUniversal()) {
            return schema->GetHunkColumnIds();
        }

        THunkColumnIds hunkColumnIds;
        const auto& columnIndexes = columnFilter.GetIndexes();
        for (int i = 0; i < std::ssize(columnIndexes); ++i) {
            if (schema->Columns()[columnIndexes[i]].MaxInlineHunkSize()) {
                hunkColumnIds.push_back(i);
            }
        }

        return hunkColumnIds;
    }
};

class TSchemalessUnversionedRowVisitor
{
public:
    template <class TRow, class F>
    void ForEachHunkValue(
        TRow row,
        const F& func) const
    {
        if (!row) {
            return;
        }
        for (auto& value : row) {
            if (Any(value.Flags & EValueFlags::Hunk)) {
                func(&value);
            }
        }
    }
};

class TVersionedRowVisitor
{
public:
    template <class TRow, class F>
    void ForEachHunkValue(
        TRow row,
        const F& func) const
    {
        if (!row) {
            return;
        }
        for (auto& value : row.Values()) {
            if (Any(value.Flags & EValueFlags::Hunk)) {
                func(&value);
            }
        }
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

void ToProto(NTableClient::NProto::THunkChunkRef* protoRef, const THunkChunkRef& ref)
{
    ToProto(protoRef->mutable_chunk_id(), ref.ChunkId);
    if (ref.ErasureCodec != NErasure::ECodec::None) {
        protoRef->set_erasure_codec(ToProto<int>(ref.ErasureCodec));
    }
    protoRef->set_hunk_count(ref.HunkCount);
    protoRef->set_total_hunk_length(ref.TotalHunkLength);
    if (ref.CompressionDictionaryId) {
        ToProto(protoRef->mutable_compression_dictionary_id(), ref.CompressionDictionaryId);
    }
}

void FromProto(THunkChunkRef* ref, const NTableClient::NProto::THunkChunkRef& protoRef)
{
    ref->ChunkId = FromProto<TChunkId>(protoRef.chunk_id());
    ref->ErasureCodec = FromProto<NErasure::ECodec>(protoRef.erasure_codec());
    ref->HunkCount = protoRef.hunk_count();
    ref->TotalHunkLength = protoRef.total_hunk_length();
    ref->CompressionDictionaryId = FromProto<TChunkId>(protoRef.compression_dictionary_id());
}

void Serialize(const THunkChunkRef& ref, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("chunk_id").Value(ref.ChunkId)
            .Item("erasure_codec").Value(ref.ErasureCodec)
            .Item("hunk_count").Value(ref.HunkCount)
            .Item("total_hunk_length").Value(ref.TotalHunkLength)
            .DoIf(ref.CompressionDictionaryId != NullChunkId, [&] (TFluentMap fluentMap) {
                fluentMap.Item("compression_dictionary_id").Value(ref.CompressionDictionaryId);
            })
        .EndMap();
}

void FormatValue(TStringBuilderBase* builder, const THunkChunkRef& ref, TStringBuf /*spec*/)
{
    builder->AppendFormat("{ChunkId: %v, ",
        ref.ChunkId);
    if (ref.ErasureCodec != NErasure::ECodec::None) {
        builder->AppendFormat("ErasureCodec: %v, ",
            ref.ErasureCodec);
    }
    if (ref.CompressionDictionaryId) {
        builder->AppendFormat("CompressionDictionaryId: %v, ",
            ref.CompressionDictionaryId);
    }
    builder->AppendFormat("HunkCount: %v, TotalHunkLength: %v}",
        ref.HunkCount,
        ref.TotalHunkLength);
}

TString ToString(const THunkChunkRef& ref)
{
    return ToStringViaBuilder(ref);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NTabletClient::NProto::THunkChunksInfo* protoHunkChunkInfo,
    const THunkChunksInfo& hunkChunkInfo)
{
    ToProto(protoHunkChunkInfo->mutable_hunk_cell_id(), hunkChunkInfo.CellId);
    ToProto(protoHunkChunkInfo->mutable_hunk_tablet_id(), hunkChunkInfo.HunkTabletId);
    protoHunkChunkInfo->set_hunk_mount_revision(hunkChunkInfo.MountRevision);
    for (const auto& [_, ref] : hunkChunkInfo.HunkChunkRefs) {
        ToProto(protoHunkChunkInfo->add_hunk_chunk_refs(), ref);
    }
}

void FromProto(
    THunkChunksInfo* hunkChunkInfo,
    const NTabletClient::NProto::THunkChunksInfo& protoHunkChunkInfo)
{
    hunkChunkInfo->CellId = FromProto<TCellId>(protoHunkChunkInfo.hunk_cell_id());
    hunkChunkInfo->HunkTabletId = FromProto<TTabletId>(protoHunkChunkInfo.hunk_tablet_id());
    hunkChunkInfo->MountRevision = protoHunkChunkInfo.hunk_mount_revision();
    for (const auto& protoHunkRef : protoHunkChunkInfo.hunk_chunk_refs()) {
        auto ref = FromProto<THunkChunkRef>(protoHunkRef);
        EmplaceOrCrash(hunkChunkInfo->HunkChunkRefs, ref.ChunkId, ref);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::THunkChunkMeta* protoMeta, const THunkChunkMeta& meta)
{
    ToProto(protoMeta->mutable_chunk_id(), meta.ChunkId);
    ToProto(protoMeta->mutable_block_sizes(), meta.BlockSizes);
}

void FromProto(THunkChunkMeta* meta, const NProto::THunkChunkMeta& protoMeta)
{
    meta->ChunkId = FromProto<TChunkId>(protoMeta.chunk_id());
    meta->BlockSizes = FromProto<std::vector<i64>>(protoMeta.block_sizes());
}

////////////////////////////////////////////////////////////////////////////////

TRef WriteHunkValue(TChunkedMemoryPool* pool, const TCompressedInlineRefHunkValue& value)
{
    YT_VERIFY(value.Payload.Size() != 0);

    auto size = sizeof(ui8) + sizeof(TChunkId) + value.Payload.Size();
    auto* beginPtr =  pool->AllocateUnaligned(size);
    auto* currentPtr = beginPtr;
    WritePod(currentPtr, static_cast<char>(EHunkValueTag::CompressedInline)); // tag
    WritePod(currentPtr, value.CompressionDictionaryId);                      // chunkId
    WriteRef(currentPtr, value.Payload);                                      // payload
    YT_VERIFY(currentPtr - beginPtr == static_cast<i64>(size));
    return TRef(beginPtr, currentPtr);
}

TRef WriteHunkValue(TChunkedMemoryPool* pool, const TLocalRefHunkValue& value)
{
    char* beginPtr =  pool->AllocateUnaligned(MaxLocalHunkRefSize);
    char* endPtr =  beginPtr + MaxLocalHunkRefSize;
    auto* currentPtr = beginPtr;
    WritePod(currentPtr, static_cast<char>(EHunkValueTag::LocalRef));               // tag
    currentPtr += WriteVarUint32(currentPtr, static_cast<ui32>(value.ChunkIndex));  // chunkIndex
    currentPtr += WriteVarUint64(currentPtr, static_cast<ui64>(value.Length));      // length
    currentPtr += WriteVarUint32(currentPtr, static_cast<ui32>(value.BlockIndex));  // blockIndex
    currentPtr += WriteVarUint64(currentPtr, static_cast<ui64>(value.BlockOffset)); // blockOffset
    pool->Free(currentPtr, endPtr);
    return TRef(beginPtr, currentPtr);
}

TRef WriteHunkValue(TChunkedMemoryPool* pool, const TGlobalRefHunkValue& value)
{
    char* beginPtr =  pool->AllocateUnaligned(MaxGlobalHunkRefSize);
    char* endPtr =  beginPtr + MaxGlobalHunkRefSize;
    auto* currentPtr = beginPtr;
    WritePod(currentPtr, static_cast<char>(EHunkValueTag::GlobalRef));                  // tag
    WritePod(currentPtr, value.ChunkId);                                                // chunkId
    if (IsErasureChunkId(value.ChunkId)) {
        currentPtr += WriteVarInt32(currentPtr, static_cast<i32>(value.ErasureCodec));  // erasureCodec
    }
    currentPtr += WriteVarUint64(currentPtr, static_cast<ui64>(value.Length));          // length
    currentPtr += WriteVarUint32(currentPtr, static_cast<ui32>(value.BlockIndex));      // blockIndex
    currentPtr += WriteVarUint64(currentPtr, static_cast<ui64>(value.BlockOffset));     // blockOffset
    if (IsErasureChunkId(value.ChunkId)) {
        currentPtr += WriteVarUint64(currentPtr, static_cast<ui64>(*value.BlockSize));  // blockSize
    }
    if (IsBlobChunkId(value.ChunkId)) {
        WritePod(currentPtr, value.CompressionDictionaryId);                            // compressionDictionaryId
    }
    pool->Free(currentPtr, endPtr);
    return TRef(beginPtr, currentPtr);
}

THunkValue ReadHunkValue(TRef input)
{
    if (input.Size() == 0) {
        return TInlineHunkValue{TRef::MakeEmpty()};
    }

    const char* currentPtr = input.Begin();
    switch (auto tag = *currentPtr++) {
        case static_cast<char>(EHunkValueTag::Inline):
            return TInlineHunkValue{TRef(currentPtr, input.End())};

        case static_cast<char>(EHunkValueTag::CompressedInline): {
            TChunkId dictionaryId;
            ReadPod(currentPtr, dictionaryId);
            return TCompressedInlineRefHunkValue{
                .CompressionDictionaryId = dictionaryId,
                .Payload = TRef(currentPtr, input.End()),
            };
        }

        case static_cast<char>(EHunkValueTag::LocalRef): {
            ui32 chunkIndex;
            ui64 length;
            ui32 blockIndex;
            ui64 blockOffset;
            currentPtr += ReadVarUint32(currentPtr, &chunkIndex);
            currentPtr += ReadVarUint64(currentPtr, &length);
            currentPtr += ReadVarUint32(currentPtr, &blockIndex);
            currentPtr += ReadVarUint64(currentPtr, &blockOffset);
            // TODO(babenko): better out-of-bounds check.
            if (currentPtr > input.End()) {
                THROW_ERROR_EXCEPTION("Malformed local ref hunk value");
            }
            return TLocalRefHunkValue{
                .ChunkIndex = static_cast<int>(chunkIndex),
                .BlockIndex = static_cast<int>(blockIndex),
                .BlockOffset = static_cast<i64>(blockOffset),
                .Length = static_cast<i64>(length)
            };
        }

        case static_cast<char>(EHunkValueTag::GlobalRef): {
            TChunkId chunkId;
            i32 erasureCodec = static_cast<i32>(NErasure::ECodec::None);
            ui64 length;
            ui32 blockIndex;
            ui64 blockOffset;
            ui64 blockSize = 0;
            TChunkId compressionDictionaryId;
            ReadPod(currentPtr, chunkId);
            bool isErasure = IsErasureChunkId(chunkId);
            if (isErasure) {
                currentPtr += ReadVarInt32(currentPtr, &erasureCodec);
            }
            currentPtr += ReadVarUint64(currentPtr, &length);
            currentPtr += ReadVarUint32(currentPtr, &blockIndex);
            currentPtr += ReadVarUint64(currentPtr, &blockOffset);
            if (isErasure) {
                currentPtr += ReadVarUint64(currentPtr, &blockSize);
            }
            if (IsBlobChunkId(chunkId)) {
                // COMPAT(akozhikhov): We need to check for that bounds now because in case of a data node of old version
                // data node lookup may produce payload that does not contain compressionDictionaryId.
                // Which is fine in case of disabled hunk value compression (i.e. null compressionDictionaryId).
                if (currentPtr < input.End()) {
                    ReadPod(currentPtr, compressionDictionaryId);
                }
            }
            // TODO(babenko): better out-of-bounds check.
            if (currentPtr > input.End()) {
                THROW_ERROR_EXCEPTION("Malformed global ref hunk value");
            }
            return TGlobalRefHunkValue{
                .ChunkId = chunkId,
                .ErasureCodec = static_cast<NErasure::ECodec>(erasureCodec),
                .BlockIndex = static_cast<int>(blockIndex),
                .BlockOffset = static_cast<i64>(blockOffset),
                .BlockSize = isErasure ? std::make_optional<i64>(static_cast<i64>(blockSize)) : std::nullopt,
                .Length = static_cast<i64>(length),
                .CompressionDictionaryId = compressionDictionaryId,
            };
        }

        default:
            THROW_ERROR_EXCEPTION("Invalid hunk value tag %v",
                static_cast<ui8>(tag));
    }
}

bool TryDecodeInlineHunkValue(TUnversionedValue* value)
{
    auto hunkValue = ReadHunkValue(GetValueRef(*value));
    YT_VERIFY(!std::holds_alternative<TCompressedInlineRefHunkValue>(hunkValue));
    const auto* inlineHunkValue = std::get_if<TInlineHunkValue>(&hunkValue);
    if (!inlineHunkValue) {
        return false;
    }
    SetValueRef(value, inlineHunkValue->Payload);
    value->Flags &= ~EValueFlags::Hunk;
    return true;
}

namespace {

void DoGlobalizeHunkValue(
    TChunkedMemoryPool* pool,
    const std::vector<THunkChunkRef>& hunkChunkRefs,
    const std::vector<THunkChunkMeta>& hunkChunkMetas,
    TUnversionedValue* value,
    NChunkClient::TChunkId compressionDictionaryId)
{
    Visit(
        ReadHunkValue(TRef(value->Data.String, value->Length)),
        [&] (const TInlineHunkValue& inlineHunkValue) {
            if (compressionDictionaryId) {
                TCompressedInlineRefHunkValue compressedInlineRefHunkValue{
                    .CompressionDictionaryId = compressionDictionaryId,
                    .Payload = inlineHunkValue.Payload,
                };
                auto hunkValuePayload = WriteHunkValue(pool, compressedInlineRefHunkValue);
                SetValueRef(value, hunkValuePayload);
            }
        },
        [&] (const TCompressedInlineRefHunkValue& /*compressedInlineRefHunkValue*/) {
            THROW_ERROR_EXCEPTION("Unexpected compressed inline hunk value");
        },
        [&] (const TLocalRefHunkValue& localRefHunkValue) {
            const auto& hunkChunkRef = hunkChunkRefs[localRefHunkValue.ChunkIndex];

            std::optional<i64> blockSize;
            if (IsErasureChunkId(hunkChunkRef.ChunkId)) {
                const auto& hunkChunkMeta = hunkChunkMetas[localRefHunkValue.ChunkIndex];
                YT_VERIFY(hunkChunkMeta.ChunkId == hunkChunkRef.ChunkId);
                blockSize = hunkChunkMeta.BlockSizes[localRefHunkValue.BlockIndex];
            }

            TGlobalRefHunkValue globalRefHunkValue{
                .ChunkId = hunkChunkRef.ChunkId,
                .ErasureCodec = hunkChunkRef.ErasureCodec,
                .BlockIndex = localRefHunkValue.BlockIndex,
                .BlockOffset = localRefHunkValue.BlockOffset,
                .BlockSize = blockSize,
                .Length = localRefHunkValue.Length,
                .CompressionDictionaryId = hunkChunkRef.CompressionDictionaryId,
            };

            auto globalRefPayload = WriteHunkValue(pool, globalRefHunkValue);
            SetValueRef(value, globalRefPayload);
        },
        [&] (const TGlobalRefHunkValue& globalRefHunkValue) {
            if (IsBlobChunkId(globalRefHunkValue.ChunkId)) {
                THROW_ERROR_EXCEPTION("Unexpected global hunk reference to blob hunk chunk");
            };
        });
}

} // namespace

void GlobalizeHunkValues(
    TChunkedMemoryPool* pool,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    TMutableVersionedRow row)
{
    if (!row) {
        return;
    }

    const auto& hunkChunkRefs = chunkMeta->HunkChunkRefs();
    const auto& hunkChunkMetas = chunkMeta->HunkChunkMetas();
    auto compressionDictionaryId = FromProto<TChunkId>(chunkMeta->Misc().compression_dictionary_id());

    for (auto& value : row.Values()) {
        if (None(value.Flags & EValueFlags::Hunk)) {
            continue;
        }

        DoGlobalizeHunkValue(
            pool,
            hunkChunkRefs,
            hunkChunkMetas,
            &value,
            compressionDictionaryId);
    }
}

void GlobalizeHunkValueAndSetHunkFlag(
    TChunkedMemoryPool* pool,
    const std::vector<THunkChunkRef>& hunkChunkRefs,
    const std::vector<THunkChunkMeta>& hunkChunkMetas,
    TUnversionedValue* value)
{
    value->Flags |= EValueFlags::Hunk;
    DoGlobalizeHunkValue(
        pool,
        hunkChunkRefs,
        hunkChunkMetas,
        value,
        /*compressionDictionaryId*/ NullChunkId);
}

void GlobalizeHunkValuesAndSetHunkFlag(
    TChunkedMemoryPool* pool,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    bool* columnHunkFlags,
    TMutableVersionedRow row)
{
    if (!row) {
        return;
    }

    const auto& hunkChunkRefs = chunkMeta->HunkChunkRefs();
    const auto& hunkChunkMetas = chunkMeta->HunkChunkMetas();
    auto compressionDictionaryId = FromProto<TChunkId>(chunkMeta->Misc().compression_dictionary_id());

    for (auto& value : row.Values()) {
        if (!columnHunkFlags[value.Id]) {
            continue;
        }

        if (value.Type == EValueType::Null) {
            continue;
        }

        value.Flags |= EValueFlags::Hunk;

        DoGlobalizeHunkValue(
            pool,
            hunkChunkRefs,
            hunkChunkMetas,
            &value,
            compressionDictionaryId);
    }
}

////////////////////////////////////////////////////////////////////////////////

class THunkChunkStatisticsBase
    : public virtual IHunkChunkStatisticsBase
{
public:
    THunkChunkStatisticsBase(
        bool enableHunkColumnarProfiling,
        const TTableSchemaPtr& schema)
    {
        if (enableHunkColumnarProfiling) {
            ColumnIdToStatistics_.emplace();
            for (auto id : schema->GetHunkColumnIds()) {
                EmplaceOrCrash(*ColumnIdToStatistics_, id, TAtomicColumnarStatistics{});
            }
        }
    }

    bool HasColumnarStatistics() const override
    {
        return ColumnIdToStatistics_.has_value();
    }

    TColumnarHunkChunkStatistics GetColumnarStatistics(int columnId) const override
    {
        const auto& statistics = GetOrCrash(*ColumnIdToStatistics_, columnId);

        return {
            .InlineValueCount = statistics.InlineValueCount.load(std::memory_order::relaxed),
            .RefValueCount = statistics.RefValueCount.load(std::memory_order::relaxed),
            .InlineValueWeight = statistics.InlineValueWeight.load(std::memory_order::relaxed),
            .RefValueWeight = statistics.RefValueWeight.load(std::memory_order::relaxed)
        };
    }

    void UpdateColumnarStatistics(
        int columnId,
        const TColumnarHunkChunkStatistics& newStatistics) override
    {
        auto& statistics = GetOrCrash(*ColumnIdToStatistics_, columnId);

        statistics.InlineValueCount += newStatistics.InlineValueCount;
        statistics.RefValueCount += newStatistics.RefValueCount;
        statistics.InlineValueWeight += newStatistics.InlineValueWeight;
        statistics.RefValueWeight += newStatistics.RefValueWeight;
    }

private:
    struct TAtomicColumnarStatistics
    {
        TAtomicColumnarStatistics()
        { }

        TAtomicColumnarStatistics(TAtomicColumnarStatistics&& other)
            : InlineValueCount(other.InlineValueCount.load(std::memory_order::relaxed))
            , RefValueCount(other.RefValueCount.load(std::memory_order::relaxed))
            , InlineValueWeight(other.InlineValueWeight.load(std::memory_order::relaxed))
            , RefValueWeight(other.RefValueWeight.load(std::memory_order::relaxed))
        { }

        std::atomic<i64> InlineValueCount = 0;
        std::atomic<i64> RefValueCount = 0;

        std::atomic<i64> InlineValueWeight = 0;
        std::atomic<i64> RefValueWeight = 0;
    };

    std::optional<THashMap<int, TAtomicColumnarStatistics>> ColumnIdToStatistics_;
};

class TColumnarStatisticsThunk
{
public:
    void UpdateStatistics(int columnId, const TInlineHunkValue& hunkValue)
    {
        auto* statistics = GetOrCreateStatistics(columnId);

        ++statistics->InlineValueCount;
        statistics->InlineValueWeight += hunkValue.Payload.Size();
    }

    void UpdateStatistics(int columnId, const TCompressedInlineRefHunkValue& hunkValue)
    {
        auto* statistics = GetOrCreateStatistics(columnId);

        ++statistics->InlineValueCount;

        auto* codec = NCompression::GetDictionaryCompressionCodec();
        statistics->InlineValueWeight += codec->GetFrameInfo(hunkValue.Payload).ContentSize;
    }

    void UpdateStatistics(int columnId, const TLocalRefHunkValue& hunkValue)
    {
        auto* statistics = GetOrCreateStatistics(columnId);

        ++statistics->RefValueCount;
        statistics->RefValueWeight += hunkValue.Length;
    }

    void UpdateStatistics(int columnId, const TGlobalRefHunkValue& hunkValue)
    {
        auto* statistics = GetOrCreateStatistics(columnId);

        ++statistics->RefValueCount;
        statistics->RefValueWeight += hunkValue.Length;
    }

    void MergeTo(const IHunkChunkReaderStatisticsPtr& statistics) const
    {
        YT_VERIFY(statistics);
        for (const auto& [columnId, columnarStatistics] : ColumnIdToStatistics_) {
            statistics->UpdateColumnarStatistics(columnId, columnarStatistics);
        }
    }

    void MergeTo(const IHunkChunkWriterStatisticsPtr& statistics) const
    {
        YT_VERIFY(statistics);
        for (const auto& [columnId, columnarStatistics] : ColumnIdToStatistics_) {
            statistics->UpdateColumnarStatistics(columnId, columnarStatistics);
        }
    }

private:
    THashMap<int, TColumnarHunkChunkStatistics> ColumnIdToStatistics_;


    TColumnarHunkChunkStatistics* GetOrCreateStatistics(int columnId)
    {
        auto it = ColumnIdToStatistics_.find(columnId);
        if (it == ColumnIdToStatistics_.end()) {
            it = EmplaceOrCrash(ColumnIdToStatistics_, columnId, TColumnarHunkChunkStatistics{});
        }
        return &it->second;
    }
};

class THunkChunkReaderStatistics
    : public THunkChunkStatisticsBase
    , public IHunkChunkReaderStatistics
{
public:
    using THunkChunkStatisticsBase::THunkChunkStatisticsBase;

    const TChunkReaderStatisticsPtr& GetChunkReaderStatistics() const override
    {
        return ChunkReaderStatistics_;
    }

    std::atomic<i64>& DataWeight() override
    {
        return DataWeight_;
    }

    std::atomic<i64>& DroppedDataWeight() override
    {
        return DroppedDataWeight_;
    }

    std::atomic<int>& InlineValueCount() override
    {
        return InlineValueCount_;
    }

    std::atomic<int>& RefValueCount() override
    {
        return RefValueCount_;
    }

    std::atomic<int>& BackendReadRequestCount() override
    {
        return BackendReadRequestCount_;
    }

    std::atomic<int>& BackendHedgingReadRequestCount() override
    {
        return BackendHedgingReadRequestCount_;
    }

    std::atomic<int>& BackendProbingRequestCount() override
    {
        return BackendProbingRequestCount_;
    }

private:
    const TChunkReaderStatisticsPtr ChunkReaderStatistics_ = New<TChunkReaderStatistics>();

    std::atomic<i64> DataWeight_ = 0;
    std::atomic<i64> DroppedDataWeight_ = 0;

    std::atomic<int> ChunkCount_ = 0;

    std::atomic<int> InlineValueCount_ = 0;
    std::atomic<int> RefValueCount_ = 0;

    std::atomic<int> BackendReadRequestCount_ = 0;
    std::atomic<int> BackendHedgingReadRequestCount_ = 0;
    std::atomic<int> BackendProbingRequestCount_ = 0;
};

IHunkChunkReaderStatisticsPtr CreateHunkChunkReaderStatistics(
    bool enableHunkColumnarProfiling,
    const TTableSchemaPtr& schema)
{
    if (!schema->HasHunkColumns()) {
        return nullptr;
    }

    return New<THunkChunkReaderStatistics>(
        enableHunkColumnarProfiling,
        schema);
}

class THunkChunkWriterStatistics
    : public THunkChunkStatisticsBase
    , public IHunkChunkWriterStatistics
{
public:
    using THunkChunkStatisticsBase::THunkChunkStatisticsBase;
};

DEFINE_REFCOUNTED_TYPE(THunkChunkWriterStatistics)

IHunkChunkWriterStatisticsPtr CreateHunkChunkWriterStatistics(
    bool enableHunkColumnarProfiling,
    const TTableSchemaPtr& schema)
{
    // NB: No need to create object if #enableHunkColumnarProfiling is false.
    if (!schema->HasHunkColumns() || !enableHunkColumnarProfiling) {
        return nullptr;
    }

    return New<THunkChunkWriterStatistics>(
        enableHunkColumnarProfiling,
        schema);
}

////////////////////////////////////////////////////////////////////////////////

THunkChunkStatisticsCountersBase::THunkChunkStatisticsCountersBase(
    const NProfiling::TProfiler& profiler,
    const TTableSchemaPtr& schema)
{
    for (auto id : schema->GetHunkColumnIds()) {
        auto columnProfiler = profiler.WithTag("column", schema->Columns()[id].Name());
        YT_VERIFY(ColumnIdToCounters_.emplace(
            id,
            TColumnarHunkChunkStatisticsCounters{
                .InlineValueCount = columnProfiler.Counter("/inline_value_count"),
                .RefValueCount = columnProfiler.Counter("/ref_value_count"),
                .InlineValueWeight = columnProfiler.Counter("/inline_value_weight"),
                .RefValueWeight = columnProfiler.Counter("/ref_value_weight")
            })
            .second);
    }
}

template <class IStatisticsPtr>
void THunkChunkStatisticsCountersBase::IncrementColumnar(const IStatisticsPtr& statistics)
{
    if (!statistics->HasColumnarStatistics()) {
        return;
    }

    for (auto& [columnId, counters] : ColumnIdToCounters_) {
        auto columnarStatistics = statistics->GetColumnarStatistics(columnId);
        counters.InlineValueCount.Increment(columnarStatistics.InlineValueCount);
        counters.RefValueCount.Increment(columnarStatistics.RefValueCount);
        counters.InlineValueWeight.Increment(columnarStatistics.InlineValueWeight);
        counters.RefValueWeight.Increment(columnarStatistics.RefValueWeight);
    }
}

THunkChunkReaderCounters::THunkChunkReaderCounters(
    const NProfiling::TProfiler& profiler,
    const TTableSchemaPtr& schema)
    : THunkChunkStatisticsCountersBase(profiler, schema)
    , DataWeight_(profiler.Counter("/data_weight"))
    , DroppedDataWeight_(profiler.Counter("/dropped_data_weight"))
    , InlineValueCount_(profiler.Counter("/inline_value_count"))
    , RefValueCount_(profiler.Counter("/ref_value_count"))
    , BackendReadRequestCount_(profiler.Counter("/backend_read_request_count"))
    , BackendHedgingReadRequestCount_(profiler.Counter("/backend_hedging_read_request_count"))
    , BackendProbingRequestCount_(profiler.Counter("/backend_probing_request_count"))
    , ChunkReaderStatisticsCounters_(profiler.WithPrefix("/chunk_reader_statistics"))
{ }

void THunkChunkReaderCounters::Increment(
    const IHunkChunkReaderStatisticsPtr& statistics,
    bool failed)
{
    if (!statistics) {
        return;
    }

    DataWeight_.Increment(statistics->DataWeight());
    DroppedDataWeight_.Increment(statistics->DroppedDataWeight());

    InlineValueCount_.Increment(statistics->InlineValueCount());
    RefValueCount_.Increment(statistics->RefValueCount());

    BackendReadRequestCount_.Increment(statistics->BackendReadRequestCount());
    BackendHedgingReadRequestCount_.Increment(statistics->BackendHedgingReadRequestCount());
    BackendProbingRequestCount_.Increment(statistics->BackendProbingRequestCount());

    ChunkReaderStatisticsCounters_.Increment(statistics->GetChunkReaderStatistics(), failed);

    IncrementColumnar(statistics);
}

THunkChunkWriterCounters::THunkChunkWriterCounters(
    const NProfiling::TProfiler& profiler,
    const TTableSchemaPtr& schema)
    : THunkChunkStatisticsCountersBase(profiler, schema)
    , HasHunkColumns_(schema->HasHunkColumns())
    , ChunkWriterCounters_(profiler)
{ }

void THunkChunkWriterCounters::Increment(
    const IHunkChunkWriterStatisticsPtr& statistics,
    const NChunkClient::NProto::TDataStatistics& dataStatistics,
    const TCodecStatistics& codecStatistics,
    int replicationFactor)
{
    if (!HasHunkColumns_) {
        return;
    }

    ChunkWriterCounters_.Increment(
        dataStatistics,
        codecStatistics,
        replicationFactor);

    if (statistics) {
        IncrementColumnar(statistics);
    }
}

////////////////////////////////////////////////////////////////////////////////

class THunkEncodingVersionedWriter
    : public IVersionedChunkWriter
{
public:
    THunkEncodingVersionedWriter(
        IVersionedChunkWriterPtr underlying,
        TTableSchemaPtr schema,
        IHunkChunkPayloadWriterPtr hunkChunkPayloadWriter,
        IHunkChunkWriterStatisticsPtr hunkChunkWriterStatistics,
        TFuture<IDictionaryCompressionSessionPtr> compressionSessionFuture)
        : Underlying_(std::move(underlying))
        , Schema_(std::move(schema))
        , HunkChunkPayloadWriter_(std::move(hunkChunkPayloadWriter))
        , HunkChunkWriterStatistics_(std::move(hunkChunkWriterStatistics))
        , CompressionContext_(compressionSessionFuture
            ? std::make_optional<TCompressionContext>(compressionSessionFuture)
            : std::nullopt)
    { }

    bool Write(TRange<TVersionedRow> rows) override
    {
        std::optional<TColumnarStatisticsThunk> columnarStatisticsThunk;
        if (HunkChunkWriterStatistics_ && HunkChunkWriterStatistics_->HasColumnarStatistics()) {
            columnarStatisticsThunk.emplace();
        }

        if (CompressionContext_) {
            switch (CompressionContext_->SessionStage) {
                case ECompressionSessionStage::WaitingOnFuture:
                    if (CompressionContext_->SessionFuture.IsSet()) {
                        CompressionContext_->OnSessionFutureSet();
                        if (CompressionContext_->SessionStage == ECompressionSessionStage::WaitingOnFuture) {
                            // NB: Future is set with error. User will get it from ready event future.
                            return false;
                        }
                    }
                    // No break intentionally.

                case ECompressionSessionStage::FeedingSamples:
                    CompressionContext_->FeedSamples(rows);
                    rows = {};
                    if (CompressionContext_->SessionStage == ECompressionSessionStage::WaitingOnFuture) {
                        return false;
                    }
                    if (CompressionContext_->SessionStage == ECompressionSessionStage::FeedingSamples) {
                        return true;
                    }
                    break;

                case ECompressionSessionStage::SessionIsFed:
                    break;

                case ECompressionSessionStage::SampledRowsAreWritten:
                    CompressionContext_->SampledRowBuffer = {};
                    CompressionContext_->SampledRows.clear();
                    break;

                default:
                    YT_ABORT();
            }
        }

        ScratchRowBuffer_->Clear();
        ScratchRows_.clear();
        ScratchRows_.reserve(rows.Size());

        bool ready = true;

        if (CompressionContext_) {
            if (CompressionContext_->SessionStage == ECompressionSessionStage::SessionIsFed) {
                for (auto row : CompressionContext_->SampledRows) {
                    ScratchRows_.push_back(row);
                    DataWeight_ += NTableClient::GetDataWeight(row);
                    ProcessScratchRow(row, &ready, columnarStatisticsThunk);
                }
                CompressionContext_->SessionStage = ECompressionSessionStage::SampledRowsAreWritten;
            } else {
                YT_VERIFY(CompressionContext_->SessionStage == ECompressionSessionStage::SampledRowsAreWritten);
            }
        }

        for (auto row : rows) {
            auto scratchRow = ScratchRowBuffer_->CaptureRow(row, /*captureValues*/ false);
            ScratchRows_.push_back(scratchRow);
            DataWeight_ += NTableClient::GetDataWeight(scratchRow);
            ProcessScratchRow(scratchRow, &ready, columnarStatisticsThunk);
        }

        if (columnarStatisticsThunk) {
            columnarStatisticsThunk->MergeTo(HunkChunkWriterStatistics_);
        }

        ready &= Underlying_->Write(MakeRange(ScratchRows_));
        return ready;
    }

    TFuture<void> GetReadyEvent() override
    {
        if (CompressionContext_ &&
            CompressionContext_->SessionStage == ECompressionSessionStage::WaitingOnFuture)
        {
            return CompressionContext_->SessionFuture.AsVoid();
        }

        std::vector<TFuture<void>> futures;
        futures.push_back(Underlying_->GetReadyEvent());
        if (HunkChunkPayloadWriter_) {
            futures.push_back(HunkChunkPayloadWriter_->GetReadyEvent());
        }
        return AllSucceeded(std::move(futures));
    }

    TFuture<void> Close() override
    {
        if (CompressionContext_) {
            switch (CompressionContext_->SessionStage) {
                case ECompressionSessionStage::WaitingOnFuture:
                    return CompressionContext_->SessionFuture.Apply(BIND([
                        this,
                        this_ = MakeStrong(this)
                    ] (const IDictionaryCompressionSessionPtr& /*compressionSession*/) {
                        CompressionContext_->OnSessionFutureSet();
                        YT_VERIFY(CompressionContext_->Session);
                        YT_VERIFY(CompressionContext_->SessionStage != ECompressionSessionStage::WaitingOnFuture);
                        return Close();
                    })
                        .AsyncVia(GetCurrentInvoker()));

                case ECompressionSessionStage::FeedingSamples:
                case ECompressionSessionStage::SessionIsFed:
                    YT_VERIFY(ScratchRows_.empty());
                    CompressionContext_->SessionStage = ECompressionSessionStage::SessionIsFed;
                    // NB: Before closing we need to pass CompressionContext_->SampledRows to the underlying writer.
                    Write(/*rows*/ {});
                    break;

                case ECompressionSessionStage::SampledRowsAreWritten:
                    break;

                default:
                    YT_ABORT();
            }
        }

        auto newDictionaryId = CompressionContext_
            ? CompressionContext_->Session->GetCompressionDictionaryId()
            : NullChunkId;

        if (HunkChunkPayloadWriterChunkIndex_) {
            HunkChunkPayloadWriter_->OnParentReaderFinished(newDictionaryId);

            HunkChunkRefs_[*HunkChunkPayloadWriterChunkIndex_] = THunkChunkRef{
                .ChunkId = HunkChunkPayloadWriter_->GetChunkId(),
                .ErasureCodec = HunkChunkPayloadWriter_->GetErasureCodecId(),
                .HunkCount = HunkCount_,
                .TotalHunkLength = TotalHunkLength_,
            };

            if (newDictionaryId) {
                HunkChunkRefs_[*HunkChunkPayloadWriterChunkIndex_].CompressionDictionaryId = newDictionaryId;
            }

            HunkChunkMetas_[*HunkChunkPayloadWriterChunkIndex_] = HunkChunkPayloadWriter_->GetHunkChunkMeta();
        }

        std::vector<TChunkId> dictionaryIds;
        if (newDictionaryId) {
            // NB: This is for the case when all (compressed) values are inlined and there is no new hunk chunk.
            dictionaryIds.push_back(newDictionaryId);
        }
        for (const auto& ref : HunkChunkRefs_) {
            if (ref.CompressionDictionaryId) {
                dictionaryIds.push_back(ref.CompressionDictionaryId);
            }
        }

        if (!dictionaryIds.empty()) {
            SortUnique(dictionaryIds);
            for (auto dictionaryId : dictionaryIds) {
                HunkChunkRefs_.push_back(THunkChunkRef{
                    .ChunkId = dictionaryId,
                });
            }
        }

        Underlying_->GetMeta()->RegisterFinalizer(
            [
                dataWeight = DataWeight_,
                newDictionaryId,
                weakUnderlying = MakeWeak(Underlying_),
                hunkChunkPayloadWriter = HunkChunkPayloadWriter_,
                hunkChunkRefs = std::move(HunkChunkRefs_),
                hunkChunkMetas = std::move(HunkChunkMetas_),
                dictionaryIds = std::move(dictionaryIds)
            ] (TDeferredChunkMeta* meta) mutable {
                if (newDictionaryId) {
                    auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(meta->extensions());
                    // NB: This value reflects compression dictionary id of inline hunk values.
                    ToProto(miscExt.mutable_compression_dictionary_id(), newDictionaryId);
                    // NB: Data weight is generally set within underlying chunk writer, however as it is unaware
                    // of the size of uncompressed inline hunk values we have to perform such override here.
                    miscExt.set_data_weight(dataWeight);
                    SetProtoExtension(meta->mutable_extensions(), miscExt);

                    auto chunkFeatures = FromProto<EChunkFeatures>(meta->features());
                    chunkFeatures |= EChunkFeatures::CompressedHunkValues;
                    meta->set_features(ToProto<ui64>(chunkFeatures));
                }

                if (hunkChunkRefs.empty()) {
                    return;
                }

                auto underlying = weakUnderlying.Lock();
                if (!underlying) {
                    return;
                }

                YT_LOG_DEBUG("Hunk chunk references written (StoreId: %v, HunkChunkRefs: %v, "
                    "NewDictionaryId: %v, DictionaryIds: %v)",
                    underlying->GetChunkId(),
                    hunkChunkRefs,
                    newDictionaryId,
                    dictionaryIds);

                NTableClient::NProto::THunkChunkRefsExt hunkChunkRefsExt;
                ToProto(hunkChunkRefsExt.mutable_refs(), hunkChunkRefs);
                SetProtoExtension(meta->mutable_extensions(), hunkChunkRefsExt);

                NTableClient::NProto::THunkChunkMetasExt hunkChunkMetasExt;
                ToProto(hunkChunkMetasExt.mutable_metas(), hunkChunkMetas);
                SetProtoExtension(meta->mutable_extensions(), hunkChunkMetasExt);
            });

        return Underlying_->Close();
    }

    i64 GetRowCount() const override
    {
        auto result = Underlying_->GetRowCount();
        if (CompressionContext_ &&
            CompressionContext_->SessionStage != ECompressionSessionStage::SampledRowsAreWritten)
        {
            result += std::ssize(CompressionContext_->SampledRows);
        }
        return result;
    }

    i64 GetMetaSize() const override
    {
        // NB: This can be called on unclosed writer.
        // However staging data (i.e. CompressionContext_->SampledRows) is negligible here.
        return Underlying_->GetMetaSize();
    }

    i64 GetCompressedDataSize() const override
    {
        auto result = Underlying_->GetCompressedDataSize();
        if (CompressionContext_ &&
            CompressionContext_->SessionStage != ECompressionSessionStage::SampledRowsAreWritten)
        {
            result += CompressionContext_->SampledRowBuffer->GetSize();
        }
        return result;
    }

    i64 GetDataWeight() const override
    {
        auto result = Underlying_->GetDataWeight();
        if (CompressionContext_ &&
            CompressionContext_->SessionStage != ECompressionSessionStage::SampledRowsAreWritten)
        {
            result += CompressionContext_->SampledRowBuffer->GetSize();
        }
        return result;
    }

    bool IsCloseDemanded() const override
    {
        return Underlying_->IsCloseDemanded();
    }

    TDeferredChunkMetaPtr GetMeta() const override
    {
        YT_VERIFY(!CompressionContext_ ||
            CompressionContext_->SessionStage == ECompressionSessionStage::SampledRowsAreWritten);
        return Underlying_->GetMeta();
    }

    TChunkId GetChunkId() const override
    {
        return Underlying_->GetChunkId();
    }

    TDataStatistics GetDataStatistics() const override
    {
        return Underlying_->GetDataStatistics();
    }

    TCodecStatistics GetCompressionStatistics() const override
    {
        auto statistics = Underlying_->GetCompressionStatistics();
        if (CompressionContext_ &&
            CompressionContext_->SessionStage != ECompressionSessionStage::WaitingOnFuture)
        {
            statistics.AppendToValueDictionaryCompression(CompressionContext_->Session->GetCompressionTime());
        }
        return statistics;
    }

private:
    const IVersionedChunkWriterPtr Underlying_;
    const TTableSchemaPtr Schema_;
    const IHunkChunkPayloadWriterPtr HunkChunkPayloadWriter_;
    const IHunkChunkWriterStatisticsPtr HunkChunkWriterStatistics_;

    struct TCompressionContext
    {
        struct TCompressionSamplesRowBufferTag
        { };

        TFuture<IDictionaryCompressionSessionPtr> SessionFuture;
        IDictionaryCompressionSessionPtr Session;

        std::vector<TMutableVersionedRow> SampledRows;
        TRowBufferPtr SampledRowBuffer = New<TRowBuffer>(TCompressionSamplesRowBufferTag());

        ECompressionSessionStage SessionStage = ECompressionSessionStage::WaitingOnFuture;


        TCompressionContext(TFuture<IDictionaryCompressionSessionPtr> sessionFuture)
            : SessionFuture(std::move(sessionFuture))
        {
            YT_VERIFY(SessionFuture);
        }

        void FeedSamples(TRange<TVersionedRow> rows)
        {
            for (auto row : rows) {
                // NB: Capture values so these rows might stay for multiple Write calls.
                auto sampledRow = SampledRowBuffer->CaptureRow(row, /*captureValues*/ true);
                SampledRows.push_back(sampledRow);

                MaybeFeedSample(sampledRow);
            }
        }

        void OnSessionFutureSet()
        {
            YT_VERIFY(SessionFuture.IsSet());
            auto sessionOrError = SessionFuture.Get();
            if (sessionOrError.IsOK()) {
                SessionStage = ECompressionSessionStage::FeedingSamples;
                Session = std::move(sessionOrError.Value());
                for (auto row : SampledRows) {
                    MaybeFeedSample(row);
                }
            }
        }

        void MaybeFeedSample(TMutableVersionedRow row)
        {
            if (SessionStage == ECompressionSessionStage::FeedingSamples &&
                !Session->FeedSample(row, SampledRowBuffer->GetPool()))
            {
                SessionStage = ECompressionSessionStage::SessionIsFed;
            }
        }
    };

    std::optional<TCompressionContext> CompressionContext_;

    struct TScratchRowBufferTag
    { };

    const TRowBufferPtr ScratchRowBuffer_ = New<TRowBuffer>(TScratchRowBufferTag());
    std::vector<TVersionedRow> ScratchRows_;

    i64 HunkCount_ = 0;
    i64 DataWeight_ = 0;
    i64 TotalHunkLength_ = 0;

    using TChunkIdToIndex = THashMap<TChunkId, int>;
    TChunkIdToIndex ChunkIdToIndex_;
    std::vector<THunkChunkRef> HunkChunkRefs_;
    std::vector<THunkChunkMeta> HunkChunkMetas_;

    std::optional<int> HunkChunkPayloadWriterChunkIndex_;


    int RegisterHunkRef(const TGlobalRefHunkValue& globalRefHunkValue)
    {
        int chunkIndex;
        TChunkIdToIndex::insert_ctx context;
        auto it = ChunkIdToIndex_.find(globalRefHunkValue.ChunkId, context);
        if (it == ChunkIdToIndex_.end()) {
            chunkIndex = std::ssize(HunkChunkRefs_);
            auto& ref = HunkChunkRefs_.emplace_back();
            ref.ChunkId = globalRefHunkValue.ChunkId;
            ref.ErasureCodec = globalRefHunkValue.ErasureCodec;
            ChunkIdToIndex_.emplace_direct(context, globalRefHunkValue.ChunkId, chunkIndex);

            auto& meta = HunkChunkMetas_.emplace_back();
            meta.ChunkId = globalRefHunkValue.ChunkId;
        } else {
            chunkIndex = it->second;
        }

        auto& ref = HunkChunkRefs_[chunkIndex];
        ref.HunkCount += 1;
        ref.TotalHunkLength += globalRefHunkValue.Length;

        if (IsErasureChunkId(globalRefHunkValue.ChunkId)) {
            auto& meta = HunkChunkMetas_[chunkIndex];
            auto& blockSizes = meta.BlockSizes;
            auto blockIndex = globalRefHunkValue.BlockIndex;
            if (std::ssize(blockSizes) <= blockIndex) {
                blockSizes.resize(blockIndex + 1);
            }
            blockSizes[blockIndex] = *globalRefHunkValue.BlockSize;
        }

        if (!ref.CompressionDictionaryId) {
            ref.CompressionDictionaryId = globalRefHunkValue.CompressionDictionaryId;
        }
        YT_VERIFY(ref.CompressionDictionaryId == globalRefHunkValue.CompressionDictionaryId);

        return chunkIndex;
    }

    int GetHunkChunkPayloadWriterChunkIndex()
    {
        if (!HunkChunkPayloadWriterChunkIndex_) {
            HunkChunkPayloadWriterChunkIndex_ = std::ssize(HunkChunkRefs_);
            HunkChunkRefs_.emplace_back(); // to be filled on close
            HunkChunkMetas_.emplace_back(); // to be filled on close
        }
        return *HunkChunkPayloadWriterChunkIndex_;
    }

    void ProcessScratchRow(
        TMutableVersionedRow scratchRow,
        bool* ready,
        std::optional<TColumnarStatisticsThunk>& columnarStatisticsThunk)
    {
        auto* pool = ScratchRowBuffer_->GetPool();

        std::optional<std::vector<int>> valueDataWeights;
        if (CompressionContext_) {
            YT_VERIFY(CompressionContext_->Session);
            valueDataWeights.emplace();
            for (auto& value : scratchRow.Values()) {
                valueDataWeights->push_back(value.Type != EValueType::Null && IsStringLikeType(value.Type)
                    ? value.Length
                    : 0);
            }
            CompressionContext_->Session->CompressValuesInRow(&scratchRow, pool);
        }

        for (int valueIndex = 0; valueIndex < std::ssize(scratchRow.Values()); ++valueIndex) {
            auto& value = scratchRow.Values()[valueIndex];

            if (value.Type == EValueType::Null) {
                continue;
            }

            auto maxInlineHunkSize = Schema_->Columns()[value.Id].MaxInlineHunkSize();
            if (!maxInlineHunkSize) {
                continue;
            }

            auto handleInlineHunkValue = [&] (const TInlineHunkValue& inlineHunkValue) {
                auto payloadLength = std::ssize(inlineHunkValue.Payload);
                if (payloadLength < *maxInlineHunkSize) {
                    // Leave as is.
                    if (columnarStatisticsThunk) {
                        columnarStatisticsThunk->UpdateStatistics(value.Id, inlineHunkValue);
                    }
                    return;
                }

                HunkCount_ += 1;
                TotalHunkLength_ += payloadLength;

                auto [blockIndex, blockOffset, hunkWriterReady] = HunkChunkPayloadWriter_->WriteHunk(
                    inlineHunkValue.Payload,
                    valueDataWeights ? (*valueDataWeights)[valueIndex] : payloadLength);
                *ready &= hunkWriterReady;

                TLocalRefHunkValue localRefHunkValue{
                    .ChunkIndex = GetHunkChunkPayloadWriterChunkIndex(),
                    .BlockIndex = blockIndex,
                    .BlockOffset = blockOffset,
                    .Length = payloadLength,
                };
                if (columnarStatisticsThunk) {
                    columnarStatisticsThunk->UpdateStatistics(value.Id, localRefHunkValue);
                }
                auto localizedPayload = WriteHunkValue(pool, localRefHunkValue);
                SetValueRef(&value, localizedPayload);
                value.Flags |= EValueFlags::Hunk;
            };

            auto valueRef = GetValueRef(value);
            if (Any(value.Flags & EValueFlags::Hunk)) {
                Visit(
                    ReadHunkValue(valueRef),
                    handleInlineHunkValue,
                    [&] (const TCompressedInlineRefHunkValue& /*compressedInlineRefHunkValue*/) {
                        THROW_ERROR_EXCEPTION("Unexpected compressed inline hunk value");
                    },
                    [&] (const TLocalRefHunkValue& /*localRefHunkValue*/) {
                        THROW_ERROR_EXCEPTION("Unexpected local hunk reference");
                    },
                    [&] (const TGlobalRefHunkValue& globalRefHunkValue) {
                        TLocalRefHunkValue localRefHunkValue{
                            .ChunkIndex = RegisterHunkRef(globalRefHunkValue),
                            .BlockIndex = globalRefHunkValue.BlockIndex,
                            .BlockOffset = globalRefHunkValue.BlockOffset,
                            .Length = globalRefHunkValue.Length,
                        };
                        if (columnarStatisticsThunk) {
                            columnarStatisticsThunk->UpdateStatistics(value.Id, localRefHunkValue);
                        }
                        auto localizedPayload = WriteHunkValue(pool, localRefHunkValue);
                        SetValueRef(&value, localizedPayload);
                        // NB: Strictly speaking, this is redundant.
                        value.Flags |= EValueFlags::Hunk;
                    });
            } else {
                handleInlineHunkValue(TInlineHunkValue{valueRef});
            }
        }
    }
};

IVersionedChunkWriterPtr CreateHunkEncodingVersionedWriter(
    IVersionedChunkWriterPtr underlying,
    TTableSchemaPtr schema,
    IHunkChunkPayloadWriterPtr hunkChunkPayloadWriter,
    IHunkChunkWriterStatisticsPtr hunkChunkWriterStatistics,
    IDictionaryCompressionFactoryPtr dictionaryCompressionFactory,
    const TClientChunkReadOptions& chunkReadOptions)
{
    if (!schema->HasHunkColumns()) {
        return underlying;
    }
    return New<THunkEncodingVersionedWriter>(
        std::move(underlying),
        std::move(schema),
        std::move(hunkChunkPayloadWriter),
        std::move(hunkChunkWriterStatistics),
        dictionaryCompressionFactory->MaybeCreateDictionaryCompressionSession(chunkReadOptions));
}

////////////////////////////////////////////////////////////////////////////////

TSharedRef GetAndValidateHunkPayload(TSharedRef fragment, const IChunkFragmentReader::TChunkFragmentRequest& request)
{
    YT_VERIFY(fragment.Size() >= sizeof(THunkPayloadHeader));
    auto* header = reinterpret_cast<const THunkPayloadHeader*>(fragment.Begin());
    auto payload = fragment.Slice(sizeof(THunkPayloadHeader), fragment.Size());
    auto actualChecksum = GetChecksum(payload);
    if (actualChecksum != header->Checksum) {
        THROW_ERROR_EXCEPTION("Hunk fragment checksum mismatch")
            << TErrorAttribute("chunk_id", request.ChunkId)
            << TErrorAttribute("block_index", request.BlockIndex)
            << TErrorAttribute("block_offset", request.BlockOffset)
            << TErrorAttribute("length", request.Length)
            << TErrorAttribute("expected_checksum", header->Checksum)
            << TErrorAttribute("actual_checksum", actualChecksum)
            << TErrorAttribute("recalculated_checksum", GetChecksum(payload));
    }
    return payload;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

TFuture<TSharedRange<TUnversionedValue*>> DecodeHunks(
    IChunkFragmentReaderPtr chunkFragmentReader,
    IDictionaryCompressionFactoryPtr dictionaryCompressionFactory,
    TClientChunkReadOptions options,
    TSharedRange<TUnversionedValue*> values)
{
    std::optional<TColumnarStatisticsThunk> columnarStatisticsThunk;
    if (options.HunkChunkReaderStatistics &&
        options.HunkChunkReaderStatistics->HasColumnarStatistics())
    {
        columnarStatisticsThunk.emplace();
    }

    auto setValuePayload = [] (TUnversionedValue* value, TRef payload) {
        SetValueRef(value, payload);
        value->Flags &= ~EValueFlags::Hunk;
    };

    int inlineHunkValueCount = 0;
    std::vector<IChunkFragmentReader::TChunkFragmentRequest> requests;
    std::vector<TUnversionedValue*> requestedValues;

    std::vector<TUnversionedValue*> compressedValues;
    std::vector<TChunkId> compressionDictionaryIds;

    for (auto* value : values) {
        Visit(
            ReadHunkValue(GetValueRef(*value)),
            [&] (const TInlineHunkValue& inlineHunkValue) {
                if (columnarStatisticsThunk) {
                    columnarStatisticsThunk->UpdateStatistics(value->Id, inlineHunkValue);
                }
                setValuePayload(value, inlineHunkValue.Payload);
                ++inlineHunkValueCount;
            },
            [&] (const TCompressedInlineRefHunkValue& compressedInlineRefHunkValue) {
                if (columnarStatisticsThunk) {
                    columnarStatisticsThunk->UpdateStatistics(value->Id, compressedInlineRefHunkValue);
                }
                YT_VERIFY(compressedInlineRefHunkValue.CompressionDictionaryId);
                setValuePayload(value, compressedInlineRefHunkValue.Payload);
                compressedValues.push_back(value);
                compressionDictionaryIds.push_back(compressedInlineRefHunkValue.CompressionDictionaryId);
                ++inlineHunkValueCount;
            },
            [&] (const TLocalRefHunkValue& /*localRefHunkValue*/) {
                THROW_ERROR_EXCEPTION("Unexpected local hunk reference");
            },
            [&] (const TGlobalRefHunkValue& globalRefHunkValue) {
                if (columnarStatisticsThunk) {
                    columnarStatisticsThunk->UpdateStatistics(value->Id, globalRefHunkValue);
                }
                requests.push_back({
                    .ChunkId = globalRefHunkValue.ChunkId,
                    .ErasureCodec = globalRefHunkValue.ErasureCodec,
                    .Length = static_cast<i64>(sizeof(THunkPayloadHeader)) + globalRefHunkValue.Length,
                    .BlockIndex = globalRefHunkValue.BlockIndex,
                    .BlockOffset = globalRefHunkValue.BlockOffset,
                    .BlockSize = globalRefHunkValue.BlockSize,
                });
                requestedValues.push_back(value);

                if (globalRefHunkValue.CompressionDictionaryId) {
                    compressedValues.push_back(value);
                    compressionDictionaryIds.push_back(globalRefHunkValue.CompressionDictionaryId);
                }
            });
    }

    auto hunkChunkReaderStatistics = options.HunkChunkReaderStatistics;
    if (hunkChunkReaderStatistics) {
        options.ChunkReaderStatistics = hunkChunkReaderStatistics->GetChunkReaderStatistics();
    }
    if (columnarStatisticsThunk) {
        columnarStatisticsThunk->MergeTo(hunkChunkReaderStatistics);
    }

    auto fragmentsFuture = chunkFragmentReader->ReadFragments(options, requests);
    return fragmentsFuture.ApplyUnique(BIND([
            =,
            requests = std::move(requests),
            values = std::move(values),
            requestedValues = std::move(requestedValues),
            compressedValues = std::move(compressedValues),
            compressionDictionaryIds = std::move(compressionDictionaryIds),
            hunkChunkReaderStatistics = std::move(hunkChunkReaderStatistics),
            options = std::move(options),
            dictionaryCompressionFactory = std::move(dictionaryCompressionFactory)
        ] (IChunkFragmentReader::TReadFragmentsResponse&& response) mutable {
            YT_VERIFY(response.Fragments.size() == requestedValues.size());

            i64 dataWeight = 0;
            for (int index = 0; index < std::ssize(response.Fragments); ++index) {
                const auto& fragment = response.Fragments[index];
                dataWeight += fragment.Size();
                auto payload = GetAndValidateHunkPayload(fragment, requests[index]);
                setValuePayload(requestedValues[index], payload);
            }

            if (hunkChunkReaderStatistics) {
                // NB: Chunk fragment reader does not update any hunk chunk reader statistics.
                hunkChunkReaderStatistics->DataWeight() += dataWeight;
                hunkChunkReaderStatistics->InlineValueCount() += inlineHunkValueCount;
                hunkChunkReaderStatistics->RefValueCount() += std::ssize(requestedValues);
                hunkChunkReaderStatistics->BackendReadRequestCount() += response.BackendReadRequestCount;
                hunkChunkReaderStatistics->BackendHedgingReadRequestCount() += response.BackendHedgingReadRequestCount;
                hunkChunkReaderStatistics->BackendProbingRequestCount() += response.BackendProbingRequestCount;
            }

            auto result = MakeSharedRange(values, values, std::move(response.Fragments));

            if (!compressedValues.empty()) {
                YT_VERIFY(dictionaryCompressionFactory);
                return dictionaryCompressionFactory->CreateDictionaryDecompressionSession()->DecompressValues(
                    std::move(compressedValues),
                    std::move(compressionDictionaryIds),
                    std::move(options))
                    .ApplyUnique(BIND([
                        result = std::move(result)
                    ] (std::vector<TSharedRef>&& decompressionResults) {
                        return MakeSharedRange(result, result, std::move(decompressionResults));
                    }));
            }

            return MakeFuture(std::move(result));
        }));
}

template <class TRow, class TRowVisitor>
TSharedRange<TUnversionedValue*> CollectHunkValues(
    TSharedRange<TRow> rows,
    const TRowVisitor& rowVisitor)
{
    std::vector<TUnversionedValue*> values;
    for (auto row : rows) {
        rowVisitor.ForEachHunkValue(
            row,
            [&] (TUnversionedValue* value) {
                values.push_back(value);
            });
    }
    return MakeSharedRange(std::move(values), std::move(rows));
}

std::optional<i64> UniversalHunkValueChecker(const TUnversionedValue& value)
{
    YT_ASSERT(Any(value.Flags & EValueFlags::Hunk));
    return Visit(
        ReadHunkValue(GetValueRef(value)),
        [&] (const TInlineHunkValue& /*inlineHunkValue*/) -> std::optional<i64> {
            return 0;
        },
        [&] (const TCompressedInlineRefHunkValue& /*compressedInlineRefHunkValue*/) -> std::optional<i64> {
            return 0;
        },
        [&] (const TLocalRefHunkValue& /*localRefHunkValue*/) -> std::optional<i64> {
            THROW_ERROR_EXCEPTION("Unexpected local hunk reference");
        },
        [&] (const TGlobalRefHunkValue& globalRefHunkValue) -> std::optional<i64> {
            return globalRefHunkValue.Length;
        });
}

} // namespace

template <class TRow, class TRowVisitor>
TFuture<TSharedRange<TRow>> DecodeHunksInRows(
    IChunkFragmentReaderPtr chunkFragmentReader,
    IDictionaryCompressionFactoryPtr dictionaryCompressionFactory,
    TClientChunkReadOptions options,
    TSharedRange<TRow> rows,
    const TRowVisitor& rowVisitor)
{
    return
        DecodeHunks(
            std::move(chunkFragmentReader),
            std::move(dictionaryCompressionFactory),
            std::move(options),
            CollectHunkValues(rows, rowVisitor))
        .ApplyUnique(BIND(
            [rows = std::move(rows)]
            (TSharedRange<TUnversionedValue*>&& sharedValues)
        {
            return MakeSharedRange(rows, rows, std::move(sharedValues));
        }));
}

TFuture<TSharedRange<TMutableUnversionedRow>> DecodeHunksInSchemafulUnversionedRows(
    const TTableSchemaPtr& schema,
    const TColumnFilter& columnFilter,
    IChunkFragmentReaderPtr chunkFragmentReader,
    IDictionaryCompressionFactoryPtr dictionaryCompressionFactory,
    TClientChunkReadOptions options,
    TSharedRange<TMutableUnversionedRow> rows)
{
    return DecodeHunksInRows(
        std::move(chunkFragmentReader),
        std::move(dictionaryCompressionFactory),
        std::move(options),
        std::move(rows),
        TSchemafulUnversionedRowVisitor(schema, columnFilter));
}

TFuture<TSharedRange<TMutableVersionedRow>> DecodeHunksInVersionedRows(
    IChunkFragmentReaderPtr chunkFragmentReader,
    IDictionaryCompressionFactoryPtr dictionaryCompressionFactory,
    TClientChunkReadOptions options,
    TSharedRange<TMutableVersionedRow> rows)
{
    return DecodeHunksInRows(
        std::move(chunkFragmentReader),
        std::move(dictionaryCompressionFactory),
        std::move(options),
        std::move(rows),
        TVersionedRowVisitor());
}

////////////////////////////////////////////////////////////////////////////////

template <
    class IReader,
    class TImmutableRow,
    class TMutableRow
>
class TBatchHunkReader
    : public IReader
{
public:
    TBatchHunkReader(
        TBatchHunkReaderConfigPtr config,
        TIntrusivePtr<IReader> underlying,
        IChunkFragmentReaderPtr chunkFragmentReader,
        IDictionaryCompressionFactoryPtr dictionaryCompressionFactory,
        TClientChunkReadOptions options)
        : Config_(std::move(config))
        , Underlying_(std::move(underlying))
        , ChunkFragmentReader_(std::move(chunkFragmentReader))
        , DictionaryCompressionFactory_(std::move(dictionaryCompressionFactory))
        , Options_(std::move(options))
        , Logger(TableClientLogger.WithTag("ReadSessionId: %v",
            Options_.ReadSessionId))
    { }

    TDataStatistics GetDataStatistics() const override
    {
        return Underlying_->GetDataStatistics();
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        // TODO(akozhikhov): Account hunk values decompression here.
        return Underlying_->GetDecompressionStatistics();
    }

    bool IsFetchingCompleted() const override
    {
        return Underlying_->IsFetchingCompleted();
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return Underlying_->GetFailedChunkIds();
    }

    TFuture<void> GetReadyEvent() const override
    {
        return ReadyEvent_;
    }

protected:
    const TBatchHunkReaderConfigPtr Config_;
    const TIntrusivePtr<IReader> Underlying_;
    const IChunkFragmentReaderPtr ChunkFragmentReader_;
    const IDictionaryCompressionFactoryPtr DictionaryCompressionFactory_;
    const TClientChunkReadOptions Options_;

    const NLogging::TLogger Logger;

    TFuture<void> ReadyEvent_ = VoidFuture;

    using IRowBatchPtr = typename TRowBatchTrait<TImmutableRow>::IRowBatchPtr;

    IRowBatchPtr UnderlyingRowBatch_;

    TSharedRange<TImmutableRow> EncodedRows_;
    int CurrentEncodedRowIndex_ = 0;

    bool HunksDecoded_ = false;
    std::vector<TMutableRow> DecodableRows_;
    TSharedRange<TUnversionedValue*> DecodedHunkValues_;

    struct TRowBufferTag
    { };
    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TRowBufferTag());


    template <class TRowVisitor, class THunkValueChecker>
    IRowBatchPtr DoRead(
        const TRowBatchReadOptions& options,
        const TRowVisitor& rowVisitor,
        const THunkValueChecker& valueChecker)
    {
        if (std::exchange(HunksDecoded_, false)) {
            return MakeBatch(MakeSharedRange(
                std::move(DecodableRows_),
                std::move(DecodedHunkValues_),
                MakeStrong(this)));
        }

        if (CurrentEncodedRowIndex_ >= std::ssize(EncodedRows_)) {
            UnderlyingRowBatch_ = Underlying_->Read(options);
            if (!UnderlyingRowBatch_) {
                return nullptr;
            }

            if (UnderlyingRowBatch_->IsEmpty()) {
                ReadyEvent_ = Underlying_->GetReadyEvent();
                return UnderlyingRowBatch_;
            }

            EncodedRows_ = UnderlyingRowBatch_->MaterializeRows();
            CurrentEncodedRowIndex_ = 0;

            YT_LOG_DEBUG("Hunk-encoded rows materialized (RowCount: %v)",
                EncodedRows_.size());
        }

        RowBuffer_->Clear();
        DecodableRows_.clear();

        int hunkCount = 0;
        i64 totalHunkLength = 0;
        std::vector<TUnversionedValue*> values;
        auto startRowIndex = CurrentEncodedRowIndex_;
        while (CurrentEncodedRowIndex_ < std::ssize(EncodedRows_) &&
               hunkCount < Config_->MaxHunkCountPerRead &&
               totalHunkLength < Config_->MaxTotalHunkLengthPerRead)
        {
            auto row = EncodedRows_[CurrentEncodedRowIndex_++];
            auto mutableRow = RowBuffer_->CaptureRow(row, /*captureValues*/ false);
            DecodableRows_.push_back(mutableRow);
            rowVisitor.ForEachHunkValue(
                mutableRow,
                [&] (TUnversionedValue* value) {
                    if (auto hunkLength = valueChecker(*value)) {
                        values.push_back(value);
                        hunkCount += 1;
                        totalHunkLength += *hunkLength;
                    }
                });
        }
        auto endRowIndex = CurrentEncodedRowIndex_;

        if (values.empty()) {
            return MakeBatch(MakeSharedRange(std::move(DecodableRows_), MakeStrong(this)));
        }

        YT_LOG_DEBUG("Fetching hunks in row slice "
            "(StartRowIndex: %v, EndRowIndex: %v, HunkCount: %v, TotalHunkLength: %v)",
            startRowIndex,
            endRowIndex,
            hunkCount,
            totalHunkLength);

        ReadyEvent_ =
            DecodeHunks(
                ChunkFragmentReader_,
                DictionaryCompressionFactory_,
                Options_,
                MakeSharedRange(std::move(values), DecodableRows_))
            .ApplyUnique(
                BIND(&TBatchHunkReader::OnHunksRead, MakeStrong(this)));

        return CreateEmptyRowBatch<TImmutableRow>();
    }

private:
    static IRowBatchPtr MakeBatch(TSharedRange<TMutableRow> mutableRows)
    {
        auto range = MakeRange<TImmutableRow>(mutableRows.Begin(), mutableRows.Size());
        return CreateBatchFromRows(MakeSharedRange(
            range,
            std::move(mutableRows.ReleaseHolder())));
    }

    void OnHunksRead(TSharedRange<TUnversionedValue*>&& hunkValues)
    {
        DecodedHunkValues_ = std::move(hunkValues);
        YT_VERIFY(!std::exchange(HunksDecoded_, true));
    }
};

////////////////////////////////////////////////////////////////////////////////

class THunkDecodingSchemafulUnversionedReader
    : public TBatchHunkReader<ISchemafulUnversionedReader, TUnversionedRow, TMutableUnversionedRow>
{
public:
    THunkDecodingSchemafulUnversionedReader(
        const TTableSchemaPtr& schema,
        const TColumnFilter& columnFilter,
        TBatchHunkReaderConfigPtr config,
        ISchemafulUnversionedReaderPtr underlying,
        IChunkFragmentReaderPtr chunkFragmentReader,
        IDictionaryCompressionFactoryPtr dictionaryCompressionFactory,
        TClientChunkReadOptions options)
        : TBatchHunkReader(
            std::move(config),
            std::move(underlying),
            std::move(chunkFragmentReader),
            std::move(dictionaryCompressionFactory),
            std::move(options))
        , RowVisitor_(schema, columnFilter)
    { }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        return this->DoRead(
            options,
            RowVisitor_,
            UniversalHunkValueChecker);
    }

private:
    const TSchemafulUnversionedRowVisitor RowVisitor_;
};

ISchemafulUnversionedReaderPtr CreateHunkDecodingSchemafulReader(
    const TTableSchemaPtr& schema,
    const TColumnFilter& columnFilter,
    TBatchHunkReaderConfigPtr config,
    ISchemafulUnversionedReaderPtr underlying,
    IChunkFragmentReaderPtr chunkFragmentReader,
    IDictionaryCompressionFactoryPtr dictionaryCompressionFactory,
    TClientChunkReadOptions options)
{
    if (!schema || !schema->HasHunkColumns()) {
        return underlying;
    }

    return New<THunkDecodingSchemafulUnversionedReader>(
        schema,
        columnFilter,
        std::move(config),
        std::move(underlying),
        std::move(chunkFragmentReader),
        std::move(dictionaryCompressionFactory),
        std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

class THunkInliningVersionedReader
    : public TBatchHunkReader<IVersionedReader, TVersionedRow, TMutableVersionedRow>
{
public:
    THunkInliningVersionedReader(
        TBatchHunkReaderConfigPtr config,
        IVersionedReaderPtr underlying,
        IChunkFragmentReaderPtr chunkFragmentReader,
        IDictionaryCompressionFactoryPtr dictionaryCompressionFactory,
        TTableSchemaPtr schema,
        THashSet<TChunkId> hunkChunkIdsToForceInline,
        TClientChunkReadOptions options)
        : TBatchHunkReader(
            std::move(config),
            std::move(underlying),
            std::move(chunkFragmentReader),
            std::move(dictionaryCompressionFactory),
            std::move(options))
        , Schema_(std::move(schema))
        , HunkChunkIdsToForceInline_(std::move(hunkChunkIdsToForceInline))
    { }

    TFuture<void> Open() override
    {
        return this->Underlying_->Open();
    }

    IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        i64 droppedDataWeight = 0;
        auto batch = this->DoRead(
            options,
            TVersionedRowVisitor(),
            [&] (const TUnversionedValue& value) {
                return Visit(
                    ReadHunkValue(GetValueRef(value)),
                    [&] (const TInlineHunkValue& /*inlineHunkValue*/) -> std::optional<i64> {
                        return 0;
                    },
                    [&] (const TCompressedInlineRefHunkValue& /*compressedInlineRefHunkValue*/) -> std::optional<i64> {
                        return 0;
                    },
                    [&] (const TLocalRefHunkValue& /*localRefHunkValue*/) -> std::optional<i64> {
                        THROW_ERROR_EXCEPTION("Unexpected local hunk reference");
                    },
                    [&] (const TGlobalRefHunkValue& globalRefHunkValue) -> std::optional<i64> {
                        const auto& columnSchema = Schema_->Columns()[value.Id];
                        if (globalRefHunkValue.Length <= *columnSchema.MaxInlineHunkSize() ||
                            HunkChunkIdsToForceInline_.contains(globalRefHunkValue.ChunkId))
                        {
                            return globalRefHunkValue.Length;
                        } else {
                            droppedDataWeight += globalRefHunkValue.Length;
                            return {};
                        }
                    });
            });

        if (Options_.HunkChunkReaderStatistics) {
            Options_.HunkChunkReaderStatistics->DroppedDataWeight() += droppedDataWeight;
        }

        return batch;
    }

private:
    const TTableSchemaPtr Schema_;
    const THashSet<TChunkId> HunkChunkIdsToForceInline_;
};

IVersionedReaderPtr CreateHunkInliningVersionedReader(
    TBatchHunkReaderConfigPtr config,
    IVersionedReaderPtr underlying,
    IChunkFragmentReaderPtr chunkFragmentReader,
    IDictionaryCompressionFactoryPtr dictionaryCompressionFactory,
    TTableSchemaPtr schema,
    THashSet<TChunkId> hunkChunkIdsToForceInline,
    TClientChunkReadOptions options)
{
    if (!schema->HasHunkColumns()) {
        return underlying;
    }

    return New<THunkInliningVersionedReader>(
        std::move(config),
        std::move(underlying),
        std::move(chunkFragmentReader),
        std::move(dictionaryCompressionFactory),
        std::move(schema),
        std::move(hunkChunkIdsToForceInline),
        std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

template <class IReader>
class THunkDecodingSchemalessUnversionedReaderBase
    : public TBatchHunkReader<IReader, TUnversionedRow, TMutableUnversionedRow>
{
public:
    using TBatchHunkReader<IReader, TUnversionedRow, TMutableUnversionedRow>::TBatchHunkReader;

    const TNameTablePtr& GetNameTable() const override
    {
        return this->Underlying_->GetNameTable();
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        return this->DoRead(
            options,
            TSchemalessUnversionedRowVisitor(),
            UniversalHunkValueChecker);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class IReader>
class THunkDecodingSchemalessChunkReader
    : public THunkDecodingSchemalessUnversionedReaderBase<IReader>
{
public:
    using THunkDecodingSchemalessUnversionedReaderBase<IReader>::
        THunkDecodingSchemalessUnversionedReaderBase;

    //! ITimingReader implementation.
    TTimingStatistics GetTimingStatistics() const override
    {
        return this->Underlying_->GetTimingStatistics();
    }

    //! ISchemalessChunkReader implementation.
    i64 GetTableRowIndex() const override
    {
        return this->Underlying_->GetTableRowIndex();
    }

    TInterruptDescriptor GetInterruptDescriptor(
        TRange<TUnversionedRow> unreadRows) const override
    {
        std::vector<TUnversionedRow> underlyingUnreadRows;
        auto addRows = [&] (auto firstIt, auto lastIt) {
            underlyingUnreadRows.insert(underlyingUnreadRows.end(), firstIt, lastIt);
        };

        // Fetched but not decodable rows.
        addRows(this->EncodedRows_.begin() + this->CurrentEncodedRowIndex_, this->EncodedRows_.end());

        // Decodable rows.
        addRows(this->DecodableRows_.begin(), this->DecodableRows_.end());

        // Unread rows.
        addRows(unreadRows.begin(), unreadRows.end());

        return this->Underlying_->GetInterruptDescriptor(MakeRange(underlyingUnreadRows));
    }

    const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
    {
        return this->Underlying_->GetCurrentReaderDescriptor();
    }
};

ISchemalessChunkReaderPtr CreateHunkDecodingSchemalessChunkReader(
    TBatchHunkReaderConfigPtr config,
    ISchemalessChunkReaderPtr underlying,
    IChunkFragmentReaderPtr chunkFragmentReader,
    IDictionaryCompressionFactoryPtr dictionaryCompressionFactory,
    TTableSchemaPtr schema,
    TClientChunkReadOptions options)
{
    YT_VERIFY(!options.HunkChunkReaderStatistics);

    if (!schema || !schema->HasHunkColumns()) {
        return underlying;
    }

    return New<THunkDecodingSchemalessChunkReader<ISchemalessChunkReader>>(
        std::move(config),
        std::move(underlying),
        std::move(chunkFragmentReader),
        std::move(dictionaryCompressionFactory),
        std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

template <class IReader>
class THunkDecodingSchemalessMultiChunkReader
    : public THunkDecodingSchemalessChunkReader<IReader>
{
public:
    using THunkDecodingSchemalessChunkReader<IReader>::
        THunkDecodingSchemalessChunkReader;

    i64 GetSessionRowIndex() const override
    {
        return this->Underlying_->GetSessionRowIndex();
    }

    i64 GetTotalRowCount() const override
    {
        return this->Underlying_->GetTotalRowCount();
    }

    virtual void Interrupt() override
    {
        this->Underlying_->Interrupt();
    }

    void SkipCurrentReader() override
    {
        this->Underlying_->SkipCurrentReader();
    }
};

ISchemalessMultiChunkReaderPtr CreateHunkDecodingSchemalessMultiChunkReader(
    TBatchHunkReaderConfigPtr config,
    ISchemalessMultiChunkReaderPtr underlying,
    IChunkFragmentReaderPtr chunkFragmentReader,
    IDictionaryCompressionFactoryPtr dictionaryCompressionFactory,
    TTableSchemaPtr schema,
    TClientChunkReadOptions options)
{
    YT_VERIFY(!options.HunkChunkReaderStatistics);

    if (!schema || !schema->HasHunkColumns()) {
        return underlying;
    }

    return New<THunkDecodingSchemalessMultiChunkReader<ISchemalessMultiChunkReader>>(
        std::move(config),
        std::move(underlying),
        std::move(chunkFragmentReader),
        std::move(dictionaryCompressionFactory),
        std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

class THunkChunkPayloadWriter
    : public IHunkChunkPayloadWriter
{
public:
    THunkChunkPayloadWriter(
        const TWorkloadDescriptor& workloadDescriptor,
        THunkChunkPayloadWriterConfigPtr config,
        IChunkWriterPtr underlying)
        : WorkloadDescriptor_(workloadDescriptor)
        , Config_(std::move(config))
        , Underlying_(std::move(underlying))
    {
        Buffer_.Reserve(static_cast<i64>(Config_->DesiredBlockSize * BufferReserveFactor));

        if (auto codecId = Underlying_->GetErasureCodecId(); codecId != NErasure::ECodec::None) {
            DataPartCount_ = NErasure::GetCodec(codecId)->GetDataPartCount();
        }
    }

    TFuture<void> Open() override
    {
        return Underlying_->Open();
    }

    std::tuple<int, i64, bool> WriteHunk(TRef payload, i64 dataWeight) override
    {
        auto guard = Guard(Lock_);

        auto [blockIndex, blockOffset, dataSize] = AppendPayloadToBuffer(payload);

        bool ready = true;
        if (std::ssize(Buffer_) >= Config_->DesiredBlockSize) {
            ready = FlushBuffer();
        }

        HunkCount_ += 1;
        DataWeight_ += dataWeight;
        TotalHunkLength_ += std::ssize(payload);
        DataSize_ += dataSize;
        HasHunks_ = true;

        return {blockIndex, blockOffset, ready};
    }

    bool HasHunks() const override
    {
        return HasHunks_;
    }

    TFuture<void> GetReadyEvent() override
    {
        return Underlying_->GetReadyEvent();
    }

    TFuture<void> Close() override
    {
        auto guard = Guard(Lock_);

        FlushBuffer();

        if (!HasHunks_) {
            return Underlying_->Cancel();
        }

        Meta_->set_type(ToProto<int>(EChunkType::Hunk));
        Meta_->set_format(ToProto<int>(EChunkFormat::HunkDefault));

        {
            NChunkClient::NProto::TMiscExt ext;
            ext.set_compression_codec(ToProto<int>(NCompression::ECodec::None));
            ext.set_data_weight(DataWeight_);
            ext.set_uncompressed_data_size(DataSize_);
            ext.set_compressed_data_size(DataSize_);
            if (CompressionDictionaryId_) {
                ToProto(ext.mutable_compression_dictionary_id(), CompressionDictionaryId_);
            }
            ext.set_creation_time(TInstant::Now().GetValue());
            SetProtoExtension(Meta_->mutable_extensions(), ext);
        }

        {
            NTableClient::NProto::THunkChunkMiscExt ext;
            ext.set_hunk_count(HunkCount_);
            ext.set_total_hunk_length(TotalHunkLength_);
            SetProtoExtension(Meta_->mutable_extensions(), ext);
        }

        return Underlying_->Close(WorkloadDescriptor_, Meta_);
    }

    TDeferredChunkMetaPtr GetMeta() const override
    {
        return Meta_;
    }

    TChunkId GetChunkId() const override
    {
        return Underlying_->GetChunkId();
    }

    NErasure::ECodec GetErasureCodecId() const override
    {
        return Underlying_->GetErasureCodecId();
    }

    const TDataStatistics& GetDataStatistics() const override
    {
        return Underlying_->GetDataStatistics();
    }

    void OnParentReaderFinished(TChunkId compressionDictionaryId) override
    {
        auto guard = Guard(Lock_);

        CompressionDictionaryId_ = compressionDictionaryId;

        FlushBuffer();
    }

    THunkChunkMeta GetHunkChunkMeta() const override
    {
        auto guard = Guard(Lock_);

        return THunkChunkMeta{
            .ChunkId = GetChunkId(),
            .BlockSizes = BlockSizes_
        };
    }

private:
    const TWorkloadDescriptor WorkloadDescriptor_;
    const THunkChunkPayloadWriterConfigPtr Config_;
    const IChunkWriterPtr Underlying_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    int DataPartCount_ = 1;

    int BlockIndex_ = 0;
    i64 BlockOffset_ = 0;
    i64 MaxHunkSizeInBlock_ = 0;
    i64 HunkCount_ = 0;

    // Accounts uncompressed hunks.
    i64 DataWeight_ = 0;
    // Accounts compressed hunks.
    i64 TotalHunkLength_ = 0;
    // Accounts compressed hunks with header.
    i64 DataSize_ = 0;

    bool HasHunks_ = false;

    std::vector<i64> BlockSizes_;

    const TDeferredChunkMetaPtr Meta_ = New<TDeferredChunkMeta>();

    struct TBufferTag
    { };

    struct TBlockTag
    { };

    static constexpr auto BufferReserveFactor = 1.2;
    TBlob Buffer_{GetRefCountedTypeCookie<TBufferTag>()};

    TChunkId CompressionDictionaryId_;


    char* BeginWriteToBuffer(i64 writeSize)
    {
        auto oldSize = Buffer_.Size();
        Buffer_.Resize(oldSize + writeSize, /*initializeStorage*/ false);
        return Buffer_.Begin() + oldSize;
    }

    //! Returns |(blockIndex, blockOffset, dataSize)|.
    std::tuple<int, i64, i64> AppendPayloadToBuffer(TRef payload)
    {
        VERIFY_SPINLOCK_AFFINITY(Lock_);

        auto dataSize = sizeof(THunkPayloadHeader) + payload.Size();
        auto* ptr = BeginWriteToBuffer(dataSize);

        // Write header.
        auto* header = reinterpret_cast<THunkPayloadHeader*>(ptr);
        header->Checksum = GetChecksum(payload);
        ptr += sizeof(THunkPayloadHeader);

        // Write payload.
        ::memcpy(ptr, payload.Begin(), payload.Size());

        auto offset = BlockOffset_;
        BlockOffset_ += dataSize;
        MaxHunkSizeInBlock_ = std::max<i64>(MaxHunkSizeInBlock_, dataSize);
        return {BlockIndex_, offset, dataSize};
    }

    bool FlushBuffer()
    {
        VERIFY_SPINLOCK_AFFINITY(Lock_);

        if (Buffer_.IsEmpty()) {
            return true;
        }

        // If block size is at least maximum hunk size times data part count,
        // each hunk is located in at most two parts which is good for reading.
        auto minBlockSize = MaxHunkSizeInBlock_ * DataPartCount_;
        if (std::ssize(Buffer_) < minBlockSize) {
            Buffer_.Resize(minBlockSize);
        }
        auto block = TSharedRef::MakeCopy<TBlockTag>(Buffer_.ToRef());

        Buffer_.Clear();
        ++BlockIndex_;
        BlockOffset_ = 0;
        MaxHunkSizeInBlock_ = 0;
        BlockSizes_.push_back(block.Size());

        return Underlying_->WriteBlock(WorkloadDescriptor_, TBlock(std::move(block)));
    }
};

IHunkChunkPayloadWriterPtr CreateHunkChunkPayloadWriter(
    const TWorkloadDescriptor& workloadDescriptor,
    THunkChunkPayloadWriterConfigPtr config,
    IChunkWriterPtr underlying)
{
    return New<THunkChunkPayloadWriter>(
        workloadDescriptor,
        std::move(config),
        std::move(underlying));
}

////////////////////////////////////////////////////////////////////////////////

void DecodeInlineHunkInUnversionedValue(TUnversionedValue* value)
{
    if (Any(value->Flags & EValueFlags::Hunk)) {
        YT_VERIFY(IsStringLikeType(value->Type));
        auto hunkValue = ReadHunkValue(GetValueRef(*value));
        YT_VERIFY(std::holds_alternative<TInlineHunkValue>(hunkValue));
        value->Data.String++;
        value->Length--;
        value->Flags &= ~EValueFlags::Hunk;
    }
}

////////////////////////////////////////////////////////////////////////////////

bool ValidateHunkValue(
    const TUnversionedValue& value,
    const TTableSchemaPtr& schema)
{
    if (!IsStringLikeType(value.Type)) {
        return false;
    }

    auto maxInlineHunkSize = schema->Columns()[value.Id].MaxInlineHunkSize();
    if (!maxInlineHunkSize) {
        return false;
    }

    auto payload = GetValueRef(value);
    auto payloadLength = std::ssize(payload);
    if (payloadLength < *maxInlineHunkSize) {
        // Leave as is.
        return false;
    }

    return true;
}

std::vector<TRef> ExtractHunks(
    TUnversionedRow row,
    TTableSchemaPtr schema)
{
    std::vector<TRef> payloads;
    for (const auto& value : row.Elements()) {
        if (!ValidateHunkValue(value, schema)) {
            continue;
        }

        auto payload = GetValueRef(value);
        payloads.push_back(payload);
    }

    return payloads;
}

void ReplaceHunks(
    TMutableUnversionedRow row,
    const TTableSchemaPtr& schema,
    const std::vector<THunkDescriptor>& descriptors,
    TChunkedMemoryPool* pool)
{
    int descriptorIndex = 0;
    for (auto& value : row.Elements()) {
        if (!ValidateHunkValue(value, schema)) {
            continue;
        }

        YT_VERIFY(descriptorIndex < std::ssize(descriptors));
        const auto& descriptor = descriptors[descriptorIndex];

        auto payloadSize = static_cast<i64>(descriptor.Length - sizeof(THunkPayloadHeader));
        // TODO(akozhikhov): Support compressed hunk values here?
        TGlobalRefHunkValue globalRefHunkValue{
            .ChunkId = descriptor.ChunkId,
            .ErasureCodec = descriptor.ErasureCodec,
            .BlockIndex = descriptor.BlockIndex,
            .BlockOffset = descriptor.BlockOffset,
            .BlockSize = descriptor.BlockSize,
            .Length = payloadSize,
        };
        auto globalRefPayload = WriteHunkValue(pool, globalRefHunkValue);
        SetValueRef(&value, globalRefPayload);
        value.Flags |= EValueFlags::Hunk;

        ++descriptorIndex;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
