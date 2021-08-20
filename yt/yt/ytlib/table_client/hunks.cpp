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

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/core/misc/chunked_memory_pool.h>
#include <yt/yt/core/misc/variant.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NYson;
using namespace NYTree;

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
    return WriteHunkValue(pool->AllocateUnaligned(size), value);
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

size_t GetInlineHunkValueSize(const TInlineHunkValue& value)
{
    return InlineHunkHeaderSize + value.Payload.Size();
}

TRef WriteHunkValue(char* ptr, const TInlineHunkValue& value)
{
    auto beginPtr = ptr;
    auto currentPtr = ptr;
    *currentPtr++ = static_cast<char>(EHunkValueTag::Inline);          // tag
    ::memcpy(currentPtr, value.Payload.Begin(), value.Payload.Size()); // payload
    currentPtr += value.Payload.Size();
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
            THROW_ERROR_EXCEPTION("Invalid hunk value tag %v",
                static_cast<ui8>(tag));
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
    for (int index = 0; index < row.GetValueCount(); ++index) {
        auto& value = row.BeginValues()[index];
        if (None(value.Flags & EValueFlags::Hunk)) {
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
                YT_VERIFY(ColumnIdToStatistics_->emplace(id, TAtomicColumnarStatistics{})
                    .second);
            }
        }
    }

    virtual bool HasColumnarStatistics() const override
    {
        return ColumnIdToStatistics_.has_value();
    }

    virtual TColumnarHunkChunkStatistics GetColumnarStatistics(int columnId) const override
    {
        const auto& statistics = GetOrCrash(*ColumnIdToStatistics_, columnId);

        return {
            .InlineValueCount = statistics.InlineValueCount.load(std::memory_order_relaxed),
            .RefValueCount = statistics.RefValueCount.load(std::memory_order_relaxed),
            .InlineValueWeight = statistics.InlineValueWeight.load(std::memory_order_relaxed),
            .RefValueWeight = statistics.RefValueWeight.load(std::memory_order_relaxed)
        };
    }

    virtual void UpdateColumnarStatistics(
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
            : InlineValueCount(other.InlineValueCount.load(std::memory_order_relaxed))
            , RefValueCount(other.RefValueCount.load(std::memory_order_relaxed))
            , InlineValueWeight(other.InlineValueWeight.load(std::memory_order_relaxed))
            , RefValueWeight(other.RefValueWeight.load(std::memory_order_relaxed))
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
            it = ColumnIdToStatistics_.emplace(
                columnId,
                TColumnarHunkChunkStatistics{})
                .first;
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

    virtual const TChunkReaderStatisticsPtr& GetChunkReaderStatistics() const override
    {
        return ChunkReaderStatistics_;
    }

    virtual std::atomic<i64>& DataWeight() override
    {
        return DataWeight_;
    }

    virtual std::atomic<i64>& SkippedDataWeight() override
    {
        return SkippedDataWeight_;
    }

    virtual std::atomic<int>& InlineValueCount() override
    {
        return InlineValueCount_;
    }

    virtual std::atomic<int>& RefValueCount() override
    {
        return RefValueCount_;
    }

    virtual std::atomic<int>& BackendRequestCount() override
    {
        return BackendRequestCount_;
    }

private:
    const TChunkReaderStatisticsPtr ChunkReaderStatistics_ = New<TChunkReaderStatistics>();

    std::atomic<i64> DataWeight_ = 0;
    std::atomic<i64> SkippedDataWeight_ = 0;

    std::atomic<int> InlineValueCount_ = 0;
    std::atomic<int> RefValueCount_ = 0;

    std::atomic<int> BackendRequestCount_ = 0;
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
    // NB: No need to create object if |enableHunkColumnarProfiling| is false.
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
    , SkippedDataWeight_(profiler.Counter("/skipped_data_weight"))
    , InlineValueCount_(profiler.Counter("/inline_value_count"))
    , RefValueCount_(profiler.Counter("/ref_value_count"))
    , BackendRequestCount_(profiler.Counter("/backend_request_count"))
    , ChunkReaderStatisticsCounters_(profiler.WithPrefix("/chunk_reader_statistics"))
{ }

void THunkChunkReaderCounters::Increment(const IHunkChunkReaderStatisticsPtr& statistics)
{
    if (!statistics) {
        return;
    }

    DataWeight_.Increment(statistics->DataWeight());
    SkippedDataWeight_.Increment(statistics->SkippedDataWeight());

    InlineValueCount_.Increment(statistics->InlineValueCount());
    RefValueCount_.Increment(statistics->RefValueCount());

    BackendRequestCount_.Increment(statistics->BackendRequestCount());

    ChunkReaderStatisticsCounters_.Increment(statistics->GetChunkReaderStatistics());

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

    virtual bool Write(TRange<TVersionedRow> rows) override
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

                    auto [offset, hunkWriterReady] = HunkChunkPayloadWriter_->WriteHunk(inlineHunkValue.Payload);
                    ready &= hunkWriterReady;

                    TLocalRefHunkValue localRefHunkValue{
                        .ChunkIndex = GetHunkChunkPayloadWriterChunkIndex(),
                        .Length = payloadLength,
                        .Offset = offset
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
                                .ChunkIndex = RegisterHunkRef(globalRefHunkValue.ChunkId, globalRefHunkValue.Length),
                                .Length = globalRefHunkValue.Length,
                                .Offset = globalRefHunkValue.Offset
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

    virtual TFuture<void> GetReadyEvent() override
    {
        std::vector<TFuture<void>> futures;
        futures.push_back(Underlying_->GetReadyEvent());
        if (HunkChunkPayloadWriter_) {
            futures.push_back(HunkChunkPayloadWriter_->GetReadyEvent());
        }
        return AllSucceeded(std::move(futures));
    }

    virtual TFuture<void> Close() override
    {
        Underlying_->GetMeta()->RegisterFinalizer(
            [
                weakUnderlying = MakeWeak(Underlying_),
                hunkChunkPayloadWriter = HunkChunkPayloadWriter_,
                hunkChunkPayloadWriterChunkIndex = HunkChunkPayloadWriterChunkIndex_,
                hunkChunkRefs = std::move(HunkChunkRefs_),
                hunkCount = HunkCount_,
                totalHunkLength = TotalHunkLength_
            ] (TDeferredChunkMeta* meta) mutable {
                if (hunkChunkRefs.empty()) {
                    return;
                }

                auto underlying = weakUnderlying.Lock();
                YT_VERIFY(underlying);

                if (hunkChunkPayloadWriterChunkIndex) {
                    hunkChunkRefs[*hunkChunkPayloadWriterChunkIndex] = THunkChunkRef{
                        .ChunkId = hunkChunkPayloadWriter->GetChunkId(),
                        .HunkCount = hunkCount,
                        .TotalHunkLength = totalHunkLength
                    };
                }

                YT_LOG_DEBUG("Hunk chunk references written (StoreId: %v, HunkChunkRefs: %v)",
                    underlying->GetChunkId(),
                    hunkChunkRefs);

                NTableClient::NProto::THunkChunkRefsExt hunkChunkRefsExt;
                ToProto(hunkChunkRefsExt.mutable_refs(), hunkChunkRefs);
                SetProtoExtension(meta->mutable_extensions(), hunkChunkRefsExt);
            });

        auto openFuture = HunkChunkPayloadWriterChunkIndex_ ? HunkChunkPayloadWriter_->GetOpenFuture() : VoidFuture;
        return openFuture.Apply(BIND(&IVersionedMultiChunkWriter::Close, Underlying_));
    }

    virtual i64 GetRowCount() const override
    {
        return Underlying_->GetRowCount();
    }

    virtual i64 GetMetaSize() const override
    {
        return Underlying_->GetMetaSize();
    }

    virtual i64 GetCompressedDataSize() const override
    {
        return Underlying_->GetCompressedDataSize();
    }

    virtual i64 GetDataWeight() const override
    {
        return Underlying_->IsCloseDemanded();
    }

    virtual bool IsCloseDemanded() const override
    {
        return Underlying_->IsCloseDemanded();
    }

    virtual TDeferredChunkMetaPtr GetMeta() const override
    {
        return Underlying_->GetMeta();
    }

    virtual TChunkId GetChunkId() const override
    {
        return Underlying_->GetChunkId();
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        return Underlying_->GetDataStatistics();
    }

    virtual TCodecStatistics GetCompressionStatistics() const override
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

    std::optional<int> HunkChunkPayloadWriterChunkIndex_;


    int RegisterHunkRef(TChunkId chunkId, i64 length)
    {
        int chunkIndex;
        TChunkIdToIndex::insert_ctx context;
        auto it = ChunkIdToIndex_.find(chunkId, context);
        if (it == ChunkIdToIndex_.end()) {
            chunkIndex = std::ssize(HunkChunkRefs_);
            HunkChunkRefs_.emplace_back().ChunkId = chunkId;
            ChunkIdToIndex_.emplace_direct(context, chunkId, chunkIndex);
        } else {
            chunkIndex = it->second;
        }

        auto& ref = HunkChunkRefs_[chunkIndex];
        ref.HunkCount += 1;
        ref.TotalHunkLength += length;

        return chunkIndex;
    }

    int GetHunkChunkPayloadWriterChunkIndex()
    {
        if (!HunkChunkPayloadWriterChunkIndex_) {
            HunkChunkPayloadWriterChunkIndex_ = std::ssize(HunkChunkRefs_);
            HunkChunkRefs_.emplace_back(); // to be filled on close
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
    };

    i64 dataWeight = 0;
    int inlineHunkValueCount = 0;
    int refHunkValueCount = 0;
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
                    .Offset = globalRefHunkValue.Offset,
                    .Length = globalRefHunkValue.Length
                });
                requestedValues.push_back(value);
                dataWeight += globalRefHunkValue.Length;
                ++refHunkValueCount;
            });
    }

    auto hunkChunkReaderStatistics = options.HunkChunkReaderStatistics;
    if (hunkChunkReaderStatistics) {
        // NB: Chunk fragment reader does not update any hunk chunk reader statistics.
        options.ChunkReaderStatistics = hunkChunkReaderStatistics->GetChunkReaderStatistics();
        hunkChunkReaderStatistics->DataWeight() += dataWeight;
        hunkChunkReaderStatistics->InlineValueCount() += inlineHunkValueCount;
        hunkChunkReaderStatistics->RefValueCount() += refHunkValueCount;
    }

    if (columnarStatisticsThunk) {
        columnarStatisticsThunk->MergeTo(hunkChunkReaderStatistics);
    }

    return chunkFragmentReader
        ->ReadFragments(std::move(options), std::move(requests))
        .ApplyUnique(BIND([
            =,
            values = std::move(values),
            requestedValues = std::move(requestedValues),
            hunkChunkReaderStatistics = std::move(hunkChunkReaderStatistics)
        ] (IChunkFragmentReader::TReadFragmentsResponse&& response) {
            YT_VERIFY(response.Fragments.size() == requestedValues.size());
            for (int index = 0; index < std::ssize(response.Fragments); ++index) {
                setValuePayload(requestedValues[index], response.Fragments[index]);
            }
            if (hunkChunkReaderStatistics) {
                hunkChunkReaderStatistics->BackendRequestCount() += response.BackendRequestCount;
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

    virtual TDataStatistics GetDataStatistics() const override
    {
        return Underlying_->GetDataStatistics();
    }

    virtual TCodecStatistics GetDecompressionStatistics() const override
    {
        return Underlying_->GetDecompressionStatistics();
    }

    virtual bool IsFetchingCompleted() const override
    {
        return Underlying_->IsFetchingCompleted();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return Underlying_->GetFailedChunkIds();
    }

    virtual TFuture<void> GetReadyEvent() const override
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

    TSharedRange<TMutableRow> DecodableRows_;

    IRowBatchPtr ReadyRowBatch_;

    struct TRowBufferTag
    { };
    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TRowBufferTag());


    template <class TRowVisitor, class THunkValueChecker>
    IRowBatchPtr DoRead(
        const TRowBatchReadOptions& options,
        const TRowVisitor& rowVisitor,
        const THunkValueChecker& valueChecker)
    {
        if (ReadyRowBatch_) {
            DecodableRows_ = {};
            return std::move(ReadyRowBatch_);
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

        int hunkCount = 0;
        i64 totalHunkLength = 0;
        std::vector<TMutableRow> mutableRows;
        std::vector<TUnversionedValue*> values;

        auto startRowIndex = CurrentEncodedRowIndex_;
        while (CurrentEncodedRowIndex_ < std::ssize(EncodedRows_) &&
               hunkCount < Config_->MaxHunkCountPerRead &&
               totalHunkLength < Config_->MaxTotalHunkLengthPerRead)
        {
            auto row = EncodedRows_[CurrentEncodedRowIndex_++];
            auto mutableRow = RowBuffer_->CaptureRow(row, /*captureValues*/ false);
            mutableRows.push_back(mutableRow);
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

        auto sharedMutableRows = MakeSharedRange(std::move(mutableRows), MakeStrong(this));

        YT_LOG_DEBUG("Fetching hunks in row slice (StartRowIndex: %v, EndRowIndex: %v, HunkCount: %v, TotalHunkLength: %v)",
            startRowIndex,
            endRowIndex,
            hunkCount,
            totalHunkLength);

        if (values.empty()) {
            return MakeBatch(std::move(sharedMutableRows));
        }

        DecodableRows_ = std::move(sharedMutableRows);

        ReadyEvent_ =
            DecodeHunks(
                ChunkFragmentReader_,
                Options_,
                MakeSharedRange(std::move(values), DecodableRows_))
            .ApplyUnique(
                BIND(&TBatchHunkReader::OnHunksRead,
                    MakeStrong(this),
                    DecodableRows_));

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

    void OnHunksRead(
        const TSharedRange<TMutableRow>& sharedMutableRows,
        TSharedRange<TUnversionedValue*>&& sharedValues)
    {
        ReadyRowBatch_ = MakeBatch(MakeSharedRange(
            sharedMutableRows,
            sharedMutableRows,
            std::move(sharedValues)));
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

    virtual IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
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

    virtual TFuture<void> Open() override
    {
        return this->Underlying_->Open();
    }

    virtual IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        i64 skippedDataWeight = 0;
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
                            skippedDataWeight += globalRefHunkValue.Length;
                            return {};
                        }
                    });
            });

        if (Options_.HunkChunkReaderStatistics) {
            Options_.HunkChunkReaderStatistics->SkippedDataWeight() += skippedDataWeight;
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

    virtual const TNameTablePtr& GetNameTable() const override
    {
        return this->Underlying_->GetNameTable();
    }

    virtual IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
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
    virtual TTimingStatistics GetTimingStatistics() const override
    {
        return this->Underlying_->GetTimingStatistics();
    }

    //! ISchemalessChunkReader implementation.
    virtual i64 GetTableRowIndex() const override
    {
        return this->Underlying_->GetTableRowIndex();
    }

    virtual TInterruptDescriptor GetInterruptDescriptor(
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

    virtual const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
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
        THunkChunkPayloadWriterConfigPtr config,
        IChunkWriterPtr underlying)
        : Config_(std::move(config))
        , Underlying_(std::move(underlying))
        , Buffer_(TBufferTag())
    {
        Buffer_.Reserve(static_cast<i64>(Config_->DesiredBlockSize * BufferReserveFactor));
    }

    virtual std::tuple<i64, bool> WriteHunk(TRef payload) override
    {
        if (!OpenFuture_) {
            OpenFuture_ = Underlying_->Open();
        }

        auto offset = AppendPayloadToBuffer(payload);

        bool ready = true;
        if (!OpenFuture_.IsSet()) {
            ready = false;
        } else if (std::ssize(Buffer_) >= Config_->DesiredBlockSize) {
            ready = FlushBuffer();
        }

        HunkCount_ += 1;
        TotalHunkLength_ += std::ssize(payload);

        return {offset, ready};
    }

    virtual bool HasHunks() const override
    {
        return OpenFuture_.operator bool();
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        if (!OpenFuture_) {
            return VoidFuture;
        }
        if (!OpenFuture_.IsSet()) {
            return OpenFuture_;
        }
        return Underlying_->GetReadyEvent();
    }

    virtual TFuture<void> GetOpenFuture() override
    {
        YT_VERIFY(OpenFuture_);
        return OpenFuture_;
    }

    virtual TFuture<void> Close() override
    {
        if (!OpenFuture_) {
            return VoidFuture;
        }

        return OpenFuture_
            .Apply(BIND([=, this_ = MakeStrong(this)] {
                return FlushBuffer() ? VoidFuture : Underlying_->GetReadyEvent();
            }))
            .Apply(BIND([=, this_ = MakeStrong(this)] {
                Meta_->set_type(ToProto<int>(EChunkType::Hunk));
                Meta_->set_format(ToProto<int>(EChunkFormat::HunkDefault));

                {
                    NChunkClient::NProto::TMiscExt ext;
                    ext.set_compression_codec(ToProto<int>(NCompression::ECodec::None));
                    ext.set_data_weight(TotalHunkLength_);
                    ext.set_uncompressed_data_size(TotalHunkLength_);
                    ext.set_compressed_data_size(TotalHunkLength_);
                    SetProtoExtension(Meta_->mutable_extensions(), ext);
                }

                {
                    NTableClient::NProto::THunkChunkMiscExt ext;
                    ext.set_hunk_count(HunkCount_);
                    ext.set_total_hunk_length(TotalHunkLength_);
                    SetProtoExtension(Meta_->mutable_extensions(), ext);
                }

                return Underlying_->Close(Meta_);
            }));
    }

    virtual TDeferredChunkMetaPtr GetMeta() const override
    {
        return Meta_;
    }

    virtual TChunkId GetChunkId() const override
    {
        return Underlying_->GetChunkId();
    }

    virtual const TDataStatistics& GetDataStatistics() const override
    {
        return Underlying_->GetDataStatistics();
    }

private:
    const THunkChunkPayloadWriterConfigPtr Config_;
    const IChunkWriterPtr Underlying_;

    TFuture<void> OpenFuture_;

    i64 UncompressedDataSize_ = 0;
    i64 HunkCount_ = 0;
    i64 TotalHunkLength_ = 0;

    const TDeferredChunkMetaPtr Meta_ = New<TDeferredChunkMeta>();

    struct TBufferTag
    { };

    struct TBlockTag
    { };

    static constexpr auto BufferReserveFactor = 1.2;
    TBlob Buffer_;


    char* BeginWriteToBuffer(i64 writeSize)
    {
        auto oldSize = Buffer_.Size();
        Buffer_.Resize(oldSize + writeSize, false);
        return Buffer_.Begin() + oldSize;
    }

    i64 AppendPayloadToBuffer(TRef payload)
    {
        auto offset = UncompressedDataSize_;
        auto* ptr = BeginWriteToBuffer(payload.Size());
        ::memcpy(ptr, payload.Begin(), payload.Size());
        UncompressedDataSize_ += payload.Size();
        return offset;
    }

    bool FlushBuffer()
    {
        YT_VERIFY(OpenFuture_.IsSet());
        if (Buffer_.IsEmpty()) {
            return true;
        }
        auto block = TSharedRef::MakeCopy<TBlockTag>(Buffer_.ToRef());
        Buffer_.Clear();
        return Underlying_->WriteBlock(TBlock(std::move(block)));
    }
};

IHunkChunkPayloadWriterPtr CreateHunkChunkPayloadWriter(
    THunkChunkPayloadWriterConfigPtr config,
    IChunkWriterPtr underlying)
{
    return New<THunkChunkPayloadWriter>(
        std::move(config),
        std::move(underlying));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
