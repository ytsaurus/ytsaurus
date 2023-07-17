#include "hunks.h"

#include "cached_versioned_chunk_meta.h"
#include "chunk_meta_extensions.h"
#include "config.h"
#include "private.h"
#include "schemaless_chunk_reader.h"
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
        for (auto* value = row.BeginValues(); value != row.EndValues(); ++value) {
            if (Any(value->Flags & EValueFlags::Hunk)) {
                func(value);
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
}

void FromProto(THunkChunkRef* ref, const NTableClient::NProto::THunkChunkRef& protoRef)
{
    ref->ChunkId = FromProto<TChunkId>(protoRef.chunk_id());
    ref->ErasureCodec = FromProto<NErasure::ECodec>(protoRef.erasure_codec());
    ref->HunkCount = protoRef.hunk_count();
    ref->TotalHunkLength = protoRef.total_hunk_length();
}

void Serialize(const THunkChunkRef& ref, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("chunk_id").Value(ref.ChunkId)
            .Item("erasure_codec").Value(ref.ErasureCodec)
            .Item("hunk_count").Value(ref.HunkCount)
            .Item("total_hunk_length").Value(ref.TotalHunkLength)
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
    builder->AppendFormat("HunkCount: %v, TotalHunkLength: %v}",
        ref.HunkCount,
        ref.TotalHunkLength);
}

TString ToString(const THunkChunkRef& ref)
{
    return ToStringViaBuilder(ref);
}

void THunkChunkRef::Save(TStreamSaveContext& context) const
{
    using NYT::Save;

    Save(context, ChunkId);
    Save(context, ErasureCodec);
    Save(context, HunkCount);
    Save(context, TotalHunkLength);
}

void THunkChunkRef::Load(TStreamLoadContext& context)
{
    using NYT::Load;

    Load(context, ChunkId);
    Load(context, ErasureCodec);
    Load(context, HunkCount);
    Load(context, TotalHunkLength);
}

////////////////////////////////////////////////////////////////////////////////

void THunkChunksInfo::Save(TStreamSaveContext& context) const
{
    using NYT::Save;

    Save(context, CellId);
    Save(context, HunkTabletId);
    Save(context, MountRevision);
    Save(context, HunkChunkRefs);
}

void THunkChunksInfo::Load(TStreamLoadContext& context)
{
    using NYT::Load;

    Load(context, CellId);
    Load(context, HunkTabletId);
    Load(context, MountRevision);
    Load(context, HunkChunkRefs);
}

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

TRef WriteHunkValue(TChunkedMemoryPool* pool, const TInlineHunkValue& value)
{
    if (value.Payload.Size() == 0) {
        return TRef::MakeEmpty();
    }

    auto size =
        sizeof(ui8) +         // tag
        value.Payload.Size(); // payload
    return WriteHunkValue(pool->AllocateUnaligned(size), value);
}

TRef WriteHunkValue(TChunkedMemoryPool* pool, const TLocalRefHunkValue& value)
{
    char* beginPtr =  pool->AllocateUnaligned(MaxLocalHunkRefSize);
    char* endPtr =  beginPtr + MaxLocalHunkRefSize;
    auto* currentPtr = beginPtr;
    WritePod(currentPtr, static_cast<char>(EHunkValueTag::LocalRef)); // tag
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
    WritePod(currentPtr, static_cast<char>(EHunkValueTag::GlobalRef));   // tag
    WritePod(currentPtr, value.ChunkId);                                 // chunkId
    if (IsErasureChunkId(value.ChunkId)) {
        currentPtr += WriteVarInt32(currentPtr, static_cast<i32>(value.ErasureCodec)); // erasureCodec
    }
    currentPtr += WriteVarUint64(currentPtr, static_cast<ui64>(value.Length));         // length
    currentPtr += WriteVarUint32(currentPtr, static_cast<ui32>(value.BlockIndex));     // blockIndex
    currentPtr += WriteVarUint64(currentPtr, static_cast<ui64>(value.BlockOffset));    // blockOffset
    if (IsErasureChunkId(value.ChunkId)) {
        currentPtr += WriteVarUint64(currentPtr, static_cast<ui64>(*value.BlockSize));  // blockSize
    }
    pool->Free(currentPtr, endPtr);
    return TRef(beginPtr, currentPtr);
}

size_t GetInlineHunkValueSize(const TInlineHunkValue& value)
{
    return InlineHunkHeaderSize + value.Payload.Size();
}

TRef WriteHunkValue(char* ptr, const TInlineHunkValue& value)
{
    auto* beginPtr = ptr;
    auto* currentPtr = ptr;
    WritePod(currentPtr, static_cast<char>(EHunkValueTag::Inline)); // tag
    WriteRef(currentPtr, value.Payload);                            // payload
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
            };
        }

        default:
            THROW_ERROR_EXCEPTION("Invalid hunk value tag %v",
                static_cast<ui8>(tag));
    }
}

void DoGlobalizeHunkValue(
    TChunkedMemoryPool* pool,
    const NTableClient::NProto::THunkChunkRefsExt& hunkChunkRefsExt,
    const NTableClient::NProto::THunkChunkMetasExt& hunkChunkMetasExt,
    TUnversionedValue* value)
{
    auto hunkValue = ReadHunkValue(TRef(value->Data.String, value->Length));
    if (const auto* localRefHunkValue = std::get_if<TLocalRefHunkValue>(&hunkValue)) {
        const auto& hunkChunkRef = hunkChunkRefsExt.refs(localRefHunkValue->ChunkIndex);
        auto chunkId = FromProto<TChunkId>(hunkChunkRef.chunk_id());

        std::optional<i64> blockSize;
        if (IsErasureChunkId(chunkId)) {
            const auto& hunkChunkMeta = hunkChunkMetasExt.metas(localRefHunkValue->ChunkIndex);
            YT_VERIFY(FromProto<TChunkId>(hunkChunkMeta.chunk_id()) == chunkId);
            blockSize = hunkChunkMeta.block_sizes(localRefHunkValue->BlockIndex);
        }

        auto globalRefHunkValue = TGlobalRefHunkValue{
            .ChunkId = chunkId,
            .ErasureCodec = FromProto<NErasure::ECodec>(hunkChunkRef.erasure_codec()),
            .BlockIndex = localRefHunkValue->BlockIndex,
            .BlockOffset = localRefHunkValue->BlockOffset,
            .BlockSize = blockSize,
            .Length = localRefHunkValue->Length,
        };
        auto globalRefPayload = WriteHunkValue(pool, globalRefHunkValue);
        value->Data.String = globalRefPayload.Begin();
        value->Length = globalRefPayload.Size();
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
    const auto& hunkChunkMetasExt = chunkMeta->HunkChunkMetasExt();
    for (int index = 0; index < row.GetValueCount(); ++index) {
        auto& value = row.BeginValues()[index];
        if (None(value.Flags & EValueFlags::Hunk)) {
            continue;
        }

        DoGlobalizeHunkValue(
            pool,
            hunkChunkRefsExt,
            hunkChunkMetasExt,
            &value);
    }
}

void GlobalizeHunkValues(
    TChunkedMemoryPool* pool,
    const TColumnarChunkMetaPtr& chunkMeta,
    TMutableUnversionedRow row)
{
    if (!row) {
        return;
    }

    const auto& hunkChunkRefsExt = chunkMeta->HunkChunkRefsExt();
    const auto& hunkChunkMetasExt = chunkMeta->HunkChunkMetasExt();

    for (int index = 0; index < static_cast<int>(row.GetCount()); ++index) {
        auto& value = row.Begin()[index];
        if (None(value.Flags & EValueFlags::Hunk)) {
            continue;
        }

        DoGlobalizeHunkValue(
            pool,
            hunkChunkRefsExt,
            hunkChunkMetasExt,
            &value);
    }
}

void GlobalizeHunkValueAndSetHunkFlag(
    TChunkedMemoryPool* pool,
    const NTableClient::NProto::THunkChunkRefsExt& hunkChunkRefsExt,
    const NTableClient::NProto::THunkChunkMetasExt& hunkChunkMetasExt,
    TUnversionedValue* value)
{
    value->Flags |= EValueFlags::Hunk;
    DoGlobalizeHunkValue(
        pool,
        hunkChunkRefsExt,
        hunkChunkMetasExt,
        value);
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

    const auto& hunkChunkRefsExt = chunkMeta->HunkChunkRefsExt();
    const auto& hunkChunkMetasExt = chunkMeta->HunkChunkMetasExt();

    for (int index = 0; index < row.GetValueCount(); ++index) {
        auto& value = row.BeginValues()[index];

        if (!columnHunkFlags[value.Id]) {
            continue;
        }

        if (value.Type == EValueType::Null) {
            continue;
        }

        value.Flags |= EValueFlags::Hunk;

        DoGlobalizeHunkValue(
            pool,
            hunkChunkRefsExt,
            hunkChunkMetasExt,
            &value);
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
        IHunkChunkWriterStatisticsPtr hunkChunkWriterStatistics)
        : Underlying_(std::move(underlying))
        , Schema_(std::move(schema))
        , HunkChunkPayloadWriter_(std::move(hunkChunkPayloadWriter))
        , HunkChunkWriterStatistics_(std::move(hunkChunkWriterStatistics))
    { }

    bool Write(TRange<TVersionedRow> rows) override
    {
        std::optional<TColumnarStatisticsThunk> columnarStatisticsThunk;
        if (HunkChunkWriterStatistics_ && HunkChunkWriterStatistics_->HasColumnarStatistics()) {
            columnarStatisticsThunk.emplace();
        }

        ScratchRowBuffer_->Clear();
        ScratchRows_.clear();
        ScratchRows_.reserve(rows.Size());

        auto* pool = ScratchRowBuffer_->GetPool();

        bool ready = true;

        for (auto row : rows) {
            auto scratchRow = ScratchRowBuffer_->CaptureRow(row, false);
            ScratchRows_.push_back(scratchRow);

            for (int index = 0; index < scratchRow.GetValueCount(); ++index) {
                auto& value = scratchRow.BeginValues()[index];
                if (value.Type == EValueType::Null) {
                    continue;
                }

                auto maxInlineHunkSize = Schema_->Columns()[value.Id].MaxInlineHunkSize();
                if (!maxInlineHunkSize) {
                    continue;
                }

                auto handleInlineHunkValue = [&] (const TInlineHunkValue& inlineHunkValue) {
                    auto payloadLength = static_cast<i64>(inlineHunkValue.Payload.Size());
                    if (payloadLength < *maxInlineHunkSize) {
                        // Leave as is.
                        if (columnarStatisticsThunk) {
                            columnarStatisticsThunk->UpdateStatistics(value.Id, inlineHunkValue);
                        }
                        return;
                    }

                    HunkCount_ += 1;
                    TotalHunkLength_ += payloadLength;

                    auto [blockIndex, blockOffset, hunkWriterReady] = HunkChunkPayloadWriter_->WriteHunk(inlineHunkValue.Payload);
                    ready &= hunkWriterReady;

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

        if (columnarStatisticsThunk) {
            columnarStatisticsThunk->MergeTo(HunkChunkWriterStatistics_);
        }

        ready &= Underlying_->Write(MakeRange(ScratchRows_));
        return ready;
    }

    TFuture<void> GetReadyEvent() override
    {
        std::vector<TFuture<void>> futures;
        futures.push_back(Underlying_->GetReadyEvent());
        if (HunkChunkPayloadWriter_) {
            futures.push_back(HunkChunkPayloadWriter_->GetReadyEvent());
        }
        return AllSucceeded(std::move(futures));
    }

    TFuture<void> Close() override
    {
        if (HunkChunkPayloadWriterChunkIndex_) {
            HunkChunkPayloadWriter_->OnParentReaderFinished();

            HunkChunkRefs_[*HunkChunkPayloadWriterChunkIndex_] = THunkChunkRef{
                .ChunkId = HunkChunkPayloadWriter_->GetChunkId(),
                .ErasureCodec = HunkChunkPayloadWriter_->GetErasureCodecId(),
                .HunkCount = HunkCount_,
                .TotalHunkLength = TotalHunkLength_
            };

            HunkChunkMetas_[*HunkChunkPayloadWriterChunkIndex_] = HunkChunkPayloadWriter_->GetHunkChunkMeta();
        }

        Underlying_->GetMeta()->RegisterFinalizer(
            [
                weakUnderlying = MakeWeak(Underlying_),
                hunkChunkPayloadWriter = HunkChunkPayloadWriter_,
                hunkChunkRefs = std::move(HunkChunkRefs_),
                hunkChunkMetas = std::move(HunkChunkMetas_)
            ] (TDeferredChunkMeta* meta) mutable {
                if (hunkChunkRefs.empty()) {
                    return;
                }

                auto underlying = weakUnderlying.Lock();
                YT_VERIFY(underlying);

                YT_LOG_DEBUG("Hunk chunk references written (StoreId: %v, HunkChunkRefs: %v)",
                    underlying->GetChunkId(),
                    hunkChunkRefs);

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
        return Underlying_->GetRowCount();
    }

    i64 GetMetaSize() const override
    {
        return Underlying_->GetMetaSize();
    }

    i64 GetCompressedDataSize() const override
    {
        return Underlying_->GetCompressedDataSize();
    }

    i64 GetDataWeight() const override
    {
        return Underlying_->IsCloseDemanded();
    }

    bool IsCloseDemanded() const override
    {
        return Underlying_->IsCloseDemanded();
    }

    TDeferredChunkMetaPtr GetMeta() const override
    {
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
        return Underlying_->GetCompressionStatistics();
    }

private:
    const IVersionedChunkWriterPtr Underlying_;
    const TTableSchemaPtr Schema_;
    const IHunkChunkPayloadWriterPtr HunkChunkPayloadWriter_;
    const IHunkChunkWriterStatisticsPtr HunkChunkWriterStatistics_;

    struct TScratchRowBufferTag
    { };

    const TRowBufferPtr ScratchRowBuffer_ = New<TRowBuffer>(TScratchRowBufferTag());
    std::vector<TVersionedRow> ScratchRows_;

    i64 HunkCount_ = 0;
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
};

IVersionedChunkWriterPtr CreateHunkEncodingVersionedWriter(
    IVersionedChunkWriterPtr underlying,
    TTableSchemaPtr schema,
    IHunkChunkPayloadWriterPtr hunkChunkPayloadWriter,
    IHunkChunkWriterStatisticsPtr hunkChunkWriterStatistics)
{
    if (!schema->HasHunkColumns()) {
        return underlying;
    }
    return New<THunkEncodingVersionedWriter>(
        std::move(underlying),
        std::move(schema),
        std::move(hunkChunkPayloadWriter),
        std::move(hunkChunkWriterStatistics));
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
        if (value->Type == EValueType::Any) {
            *value = TryDecodeUnversionedAnyValue(*value);
        }
    };

    int inlineHunkValueCount = 0;
    std::vector<IChunkFragmentReader::TChunkFragmentRequest> requests;
    std::vector<TUnversionedValue*> requestedValues;
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
                    .BlockSize = globalRefHunkValue.BlockSize
                });
                requestedValues.push_back(value);
            });
    }

    auto hunkChunkReaderStatistics = options.HunkChunkReaderStatistics;
    if (hunkChunkReaderStatistics) {
        options.ChunkReaderStatistics = hunkChunkReaderStatistics->GetChunkReaderStatistics();
    }
    if (columnarStatisticsThunk) {
        columnarStatisticsThunk->MergeTo(hunkChunkReaderStatistics);
    }

    auto fragmentsFuture = chunkFragmentReader->ReadFragments(std::move(options), requests);
    return fragmentsFuture.ApplyUnique(BIND([
            =,
            requests = std::move(requests),
            values = std::move(values),
            requestedValues = std::move(requestedValues),
            hunkChunkReaderStatistics = std::move(hunkChunkReaderStatistics)
        ] (IChunkFragmentReader::TReadFragmentsResponse&& response) {
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

            return MakeSharedRange(values, values, std::move(response.Fragments));
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
    TClientChunkReadOptions options,
    TSharedRange<TRow> rows,
    const TRowVisitor& rowVisitor)
{
    return
        DecodeHunks(
            std::move(chunkFragmentReader),
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
    TClientChunkReadOptions options,
    TSharedRange<TMutableUnversionedRow> rows)
{
    return DecodeHunksInRows(
        std::move(chunkFragmentReader),
        std::move(options),
        std::move(rows),
        TSchemafulUnversionedRowVisitor(schema, columnFilter));
}

TFuture<TSharedRange<TMutableVersionedRow>> DecodeHunksInVersionedRows(
    IChunkFragmentReaderPtr chunkFragmentReader,
    TClientChunkReadOptions options,
    TSharedRange<TMutableVersionedRow> rows)
{
    return DecodeHunksInRows(
        std::move(chunkFragmentReader),
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
        TClientChunkReadOptions options)
        : Config_(std::move(config))
        , Underlying_(std::move(underlying))
        , ChunkFragmentReader_(std::move(chunkFragmentReader))
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

        YT_LOG_DEBUG("Fetching hunks in row slice (StartRowIndex: %v, EndRowIndex: %v, HunkCount: %v, TotalHunkLength: %v)",
            startRowIndex,
            endRowIndex,
            hunkCount,
            totalHunkLength);

        if (values.empty()) {
            return MakeBatch(MakeSharedRange(std::move(DecodableRows_), MakeStrong(this)));
        }

        ReadyEvent_ =
            DecodeHunks(
                ChunkFragmentReader_,
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
        TClientChunkReadOptions options)
        : TBatchHunkReader(
            std::move(config),
            std::move(underlying),
            std::move(chunkFragmentReader),
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
        TTableSchemaPtr schema,
        THashSet<TChunkId> hunkChunkIdsToForceInline,
        TClientChunkReadOptions options)
        : TBatchHunkReader(
            std::move(config),
            std::move(underlying),
            std::move(chunkFragmentReader),
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

class THunkDecodingSchemalessUnversionedReader
    : public THunkDecodingSchemalessUnversionedReaderBase<ISchemalessUnversionedReader>
{
public:
    using THunkDecodingSchemalessUnversionedReaderBase<ISchemalessUnversionedReader>::
        THunkDecodingSchemalessUnversionedReaderBase;
};

ISchemalessUnversionedReaderPtr CreateHunkDecodingSchemalessReader(
    TBatchHunkReaderConfigPtr config,
    ISchemalessUnversionedReaderPtr underlying,
    IChunkFragmentReaderPtr chunkFragmentReader,
    TTableSchemaPtr schema,
    TClientChunkReadOptions options)
{
    YT_VERIFY(!options.HunkChunkReaderStatistics);

    if (!schema || !schema->HasHunkColumns()) {
        return underlying;
    }

    return New<THunkDecodingSchemalessUnversionedReader>(
        std::move(config),
        std::move(underlying),
        std::move(chunkFragmentReader),
        std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

class THunkDecodingSchemalessChunkReader
    : public THunkDecodingSchemalessUnversionedReaderBase<ISchemalessChunkReader>
{
public:
    using THunkDecodingSchemalessUnversionedReaderBase<ISchemalessChunkReader>::
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
        addRows(EncodedRows_.begin() + CurrentEncodedRowIndex_, EncodedRows_.end());

        // Decodable rows.
        addRows(DecodableRows_.begin(), DecodableRows_.end());

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
    TTableSchemaPtr schema,
    TClientChunkReadOptions options)
{
    YT_VERIFY(!options.HunkChunkReaderStatistics);

    if (!schema || !schema->HasHunkColumns()) {
        return underlying;
    }

    return New<THunkDecodingSchemalessChunkReader>(
        std::move(config),
        std::move(underlying),
        std::move(chunkFragmentReader),
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

    std::tuple<int, i64, bool> WriteHunk(TRef payload) override
    {
        auto guard = Guard(Lock_);

        auto [blockIndex, blockOffset, dataSize] = AppendPayloadToBuffer(payload);

        bool ready = true;
        if (std::ssize(Buffer_) >= Config_->DesiredBlockSize) {
            ready = FlushBuffer();
        }

        HunkCount_ += 1;
        TotalHunkLength_ += std::ssize(payload);
        TotalDataSize_ += dataSize;
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
            ext.set_data_weight(TotalHunkLength_);
            ext.set_uncompressed_data_size(TotalDataSize_);
            ext.set_compressed_data_size(TotalDataSize_);
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

    void OnParentReaderFinished() override
    {
        auto guard = Guard(Lock_);

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
    i64 TotalHunkLength_ = 0;
    i64 TotalDataSize_ = 0;

    bool HasHunks_ = false;

    std::vector<i64> BlockSizes_;

    const TDeferredChunkMetaPtr Meta_ = New<TDeferredChunkMeta>();

    struct TBufferTag
    { };

    struct TBlockTag
    { };

    static constexpr auto BufferReserveFactor = 1.2;
    TBlob Buffer_{GetRefCountedTypeCookie<TBufferTag>()};


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

        // TODO(alesandra-zh): support erasure.
        YT_VERIFY(!IsErasureChunkId(descriptor.ChunkId));
        YT_VERIFY(descriptor.ErasureCodec == NErasure::ECodec::None);

        auto payloadSize = static_cast<i64>(descriptor.Length - sizeof(THunkPayloadHeader));
        auto globalRefHunkValue = TGlobalRefHunkValue{
            .ChunkId = descriptor.ChunkId,
            .ErasureCodec = descriptor.ErasureCodec,
            .BlockIndex = descriptor.BlockIndex,
            .BlockOffset = descriptor.BlockOffset,
            .BlockSize = descriptor.BlockSize,
            .Length = payloadSize
        };
        auto globalRefPayload = WriteHunkValue(pool, globalRefHunkValue);
        value.Data.String = globalRefPayload.Begin();
        value.Length = globalRefPayload.Size();
        value.Flags |= EValueFlags::Hunk;

        ++descriptorIndex;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
