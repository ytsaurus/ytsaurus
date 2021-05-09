#include "hunks.h"

#include "config.h"
#include "private.h"

#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/chunk_fragment_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/hunks.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_writer.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/core/misc/variant.h>

namespace NYT::NTabletNode {

using namespace NTableClient;
using namespace NChunkClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

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

TInlineHunkValue ReadInlineHunkValue(TRef input)
{
    if (input.Empty()) {
        return TInlineHunkValue{
            .Payload = TRef::MakeEmpty()
        };
    }

    YT_ASSERT(input.Begin()[0] == static_cast<char>(EHunkValueTag::Inline));
    return TInlineHunkValue{
        .Payload = input.Slice(1, input.Size())
    };
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class THunkChunkPayloadWriter
    : public IHunkChunkPayloadWriter
{
public:
    THunkChunkPayloadWriter(
        THunkChunkPayloadWriterConfigPtr config,
        IChunkWriterPtr underlying,
        int chunkIndex)
        : Config_(std::move(config))
        , Underlying_(std::move(underlying))
        , ChunkIndex_(chunkIndex)
        , Buffer_(TBufferTag())
    {
        Buffer_.Reserve(static_cast<i64>(Config_->DesiredBlockSize * BufferReserveFactor));
    }

    virtual TFuture<void> Open() override
    {
        return Underlying_->Open();
    }

    virtual std::tuple<TLocalRefHunkValue, bool> WriteHunk(TRef payload) override
    {
        HunkChunkRef_.HunkCount += 1;
        HunkChunkRef_.TotalHunkLength += payload.Size();

        if (std::ssize(payload) >= Config_->PayloadSectorAlignmentLengthThreshold) {
            AppendPaddingToBuffer(static_cast<i64>(GetSectorPadding(Buffer_.Size())));
        }

        auto offset = AppendPayloadToBuffer(payload);

        bool ready = true;
        if (static_cast<i64>(Buffer_.Size()) >= Config_->DesiredBlockSize) {
            ready = FlushBuffer();
        }

        return {
            TLocalRefHunkValue{
                .ChunkIndex = ChunkIndex_,
                .Length = static_cast<i64>(payload.Size()),
                .Offset = offset
            },
            ready
        };
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return Underlying_->GetReadyEvent();
    }

    virtual TFuture<void> Close() override
    {
        auto ready = FlushBuffer();
        auto future = ready ? VoidFuture : Underlying_->GetReadyEvent();
        return future.Apply(BIND([=, this_ = MakeStrong(this)] {
            Meta_->set_type(ToProto<int>(EChunkType::Hunk));
            Meta_->set_format(ToProto<int>(EHunkChunkFormat::Default));

            NChunkClient::NProto::TMiscExt miscExt;
            miscExt.set_compression_codec(ToProto<int>(NCompression::ECodec::None));
            miscExt.set_uncompressed_data_size(TotalSize_);
            miscExt.set_compressed_data_size(TotalSize_);
            SetProtoExtension(Meta_->mutable_extensions(), miscExt);

            return Underlying_->Close(Meta_);
        }));
    }

    virtual THunkChunkRef GetHunkChunkRef() const override
    {
        auto result = HunkChunkRef_;
        result.ChunkId = Underlying_->GetChunkId();
        return result;
    }

    virtual TDeferredChunkMetaPtr GetMeta() const override
    {
        return Meta_;
    }

private:
    const THunkChunkPayloadWriterConfigPtr Config_;
    const IChunkWriterPtr Underlying_;
    const int ChunkIndex_;

    // NB: ChunkId is not filled.
    THunkChunkRef HunkChunkRef_;
    i64 TotalSize_ = 0;

    const TDeferredChunkMetaPtr Meta_ = New<TDeferredChunkMeta>();

    struct TBufferTag
    { };

    struct TBlockTag
    { };

    static constexpr auto BufferReserveFactor = 1.2;
    TBlob Buffer_;

    static size_t GetSectorPadding(size_t size)
    {
        return size % NIO::SectorSize == 0 ? 0 : NIO::SectorSize - (size % NIO::SectorSize);
    }

    char* BeginWriteToBuffer(i64 writeSize)
    {
        auto oldSize = Buffer_.Size();
        Buffer_.Resize(oldSize + writeSize, false);
        return Buffer_.Begin() + oldSize;
    }

    void AppendPaddingToBuffer(i64 size)
    {
        if (size == 0) {
            return;
        }
        auto* ptr = BeginWriteToBuffer(size);
        ::memset(ptr, 0, size);
        TotalSize_ += size;
    }

    i64 AppendPayloadToBuffer(TRef payload)
    {
        auto offset = TotalSize_;
        auto* ptr = BeginWriteToBuffer(payload.Size());
        ::memcpy(ptr, payload.Begin(), payload.Size());
        TotalSize_ += payload.Size();
        return offset;
    }

    bool FlushBuffer()
    {
        if (Buffer_.IsEmpty()) {
            return true;
        }
        AppendPaddingToBuffer(static_cast<i64>(GetSectorPadding(Buffer_.Size())));
        auto block = TSharedRef::MakeCopy<TBlockTag>(Buffer_.ToRef());
        Buffer_.Clear();
        return Underlying_->WriteBlock(TBlock(std::move(block)));
    }
};

IHunkChunkPayloadWriterPtr CreateHunkChunkPayloadWriter(
    THunkChunkPayloadWriterConfigPtr config,
    IChunkWriterPtr underlying,
    int chunkIndex)
{
    return New<THunkChunkPayloadWriter>(
        std::move(config),
        std::move(underlying),
        chunkIndex);
}

////////////////////////////////////////////////////////////////////////////////

class THunkRefLocalizingVersionedWriterAdapter
    : public IVersionedChunkWriter
{
public:
    THunkRefLocalizingVersionedWriterAdapter(
        IVersionedChunkWriterPtr underlying,
        TTableSchemaPtr schema)
        : Underlying_(std::move(underlying))
        , Schema_(std::move(schema))
    { }

    virtual bool Write(TRange<TVersionedRow> rows) override
    {
        RowBuffer_->Clear();
        LocalizedRows_.clear();
        LocalizedRows_.reserve(rows.Size());

        auto* pool = RowBuffer_->GetPool();

        for (auto row : rows) {
            auto localizedRow = RowBuffer_->CaptureRow(row, false);
            LocalizedRows_.push_back(localizedRow);

            for (int index = 0; index < localizedRow.GetValueCount(); ++index) {
                auto& value = localizedRow.BeginValues()[index];
                if (!Schema_->Columns()[value.Id].MaxInlineHunkSize()) {
                    continue;
                }

                auto hunkValue = ReadHunkValue(GetValueRef(value));
                Visit(
                    hunkValue,
                    [&] (const TInlineHunkValue& /*inlineHunkValue*/) {
                        // Leave as is.
                    },
                    [&] (const TLocalRefHunkValue& /*localRefHunkValue*/) {
                        THROW_ERROR_EXCEPTION("Unexpected local hunk reference");
                    },
                    [&] (const TGlobalRefHunkValue& globalRefHunkValue) {
                        auto localizedPayload = WriteHunkValue(
                            pool,
                            TLocalRefHunkValue{
                                .ChunkIndex = RegisterHunkRef(globalRefHunkValue.ChunkId, globalRefHunkValue.Length),
                                .Length = globalRefHunkValue.Length,
                                .Offset = globalRefHunkValue.Offset
                            });
                        SetValueRef(&value, localizedPayload);
                    });
            }
        }

        return Underlying_->Write(MakeRange(LocalizedRows_));
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return Underlying_->GetReadyEvent();
    }

    virtual TFuture<void> Close() override
    {
        Underlying_->GetMeta()->RegisterFinalizer(
            [
                weakUnderlying = MakeWeak(Underlying_),
                hunkChunkRefs = std::move(HunkChunkRefs_)
            ] (TDeferredChunkMeta* meta) {
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
            });
        return Underlying_->Close();
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

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return Underlying_->GetDataStatistics();
    }

    virtual TCodecStatistics GetCompressionStatistics() const override
    {
        return Underlying_->GetCompressionStatistics();
    }

private:
    const IVersionedChunkWriterPtr Underlying_;
    const IChunkWriterPtr ChunkWriter_;
    const TTableSchemaPtr Schema_;

    struct TRowBufferTag
    { };

    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>();
    std::vector<TVersionedRow> LocalizedRows_;

    using TChunkIdToIndex = THashMap<TChunkId, int>;
    TChunkIdToIndex ChunkIdToIndex_;
    std::vector<THunkChunkRef> HunkChunkRefs_;


    int RegisterHunkRef(TChunkId chunkId, i64 length)
    {
        int chunkIndex;
        TChunkIdToIndex::insert_ctx context;
        auto it = ChunkIdToIndex_.find(chunkId, context);
        if (it == ChunkIdToIndex_.end()) {
            chunkIndex = static_cast<int>(HunkChunkRefs_.size());
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
};

IVersionedChunkWriterPtr CreateHunkRefLocalizingVersionedWriterAdapter(
    IVersionedChunkWriterPtr underlying,
    TTableSchemaPtr schema)
{
    return New<THunkRefLocalizingVersionedWriterAdapter>(
        std::move(underlying),
        std::move(schema));
}

////////////////////////////////////////////////////////////////////////////////

TVersionedRow EncodeHunkValues(
    TVersionedRow row,
    const TTableSchema& schema,
    const TRowBufferPtr& rowBuffer,
    const IHunkChunkPayloadWriterPtr& hunkChunkPayloadWriter)
{
    auto encodedRow = rowBuffer->AllocateVersioned(
        row.GetKeyCount(),
        row.GetValueCount(),
        row.GetWriteTimestampCount(),
        row.GetDeleteTimestampCount());

    ::memcpy(encodedRow.BeginKeys(), row.BeginKeys(), sizeof (TUnversionedValue) * row.GetKeyCount());
    ::memcpy(encodedRow.BeginWriteTimestamps(), row.BeginWriteTimestamps(), sizeof (TTimestamp) * row.GetWriteTimestampCount());
    ::memcpy(encodedRow.BeginDeleteTimestamps(), row.BeginDeleteTimestamps(), sizeof (TTimestamp) * row.GetDeleteTimestampCount());

    auto* pool = rowBuffer->GetPool();

    for (int index = 0; index < row.GetValueCount(); ++index) {
        auto value = row.BeginValues()[index];
        const auto& columnSchema = schema.Columns()[value.Id];
        if (auto maxInlineHunkSize = columnSchema.MaxInlineHunkSize()) {
            auto inputValueRef = GetValueRef(value);
            auto inputValue = ReadInlineHunkValue(inputValueRef);
            TRef encodedValueRef;
            if (value.Length > *maxInlineHunkSize) {
                auto [refHunkValue, ready] = hunkChunkPayloadWriter->WriteHunk(inputValue.Payload);
                if (!ready) {
                    WaitFor(hunkChunkPayloadWriter->GetReadyEvent())
                        .ThrowOnError();
                }
                encodedValueRef = WriteHunkValue(pool, refHunkValue);
            } else {
                encodedValueRef = inputValueRef;
            }
            SetValueRef(&value, encodedValueRef);
        }
        encodedRow.BeginValues()[index] = value;
    }

    return encodedRow;
}

////////////////////////////////////////////////////////////////////////////////

template <class TRow>
class THunkPayloadReader::TSessionBase
    : public TRefCounted
{
protected:
    const NChunkClient::IChunkFragmentReaderPtr Reader_;
    const NTableClient::TTableSchemaPtr Schema_;
    const TSharedRange<TRow> Rows_;
    const TClientChunkReadOptions Options_;

    std::vector<IChunkFragmentReader::TChunkFragmentRequest> Requests_;
    std::vector<TUnversionedValue*> HunkValues_;

    TSessionBase(
        IChunkFragmentReaderPtr chunkFragmentReader,
        TTableSchemaPtr schema,
        TSharedRange<TRow> rows,
        TClientChunkReadOptions options)
        : Reader_(std::move(chunkFragmentReader))
        , Schema_(std::move(schema))
        , Rows_(std::move(rows))
        , Options_(std::move(options))
    { }

    void ProcessHunkValue(TUnversionedValue* value)
    {
        auto hunkValue = ReadHunkValue(GetValueRef(*value));
        Visit(
            hunkValue,
            [&] (const TInlineHunkValue& inlineHunkValue) {
                SetValueRef(value, inlineHunkValue.Payload);
            },
            [&] (const TLocalRefHunkValue& /*localRefHunkValue*/) {
                THROW_ERROR_EXCEPTION("Unexpected local hunk reference");
            },
            [&] (const TGlobalRefHunkValue& globalRefHunkValue) {
                HunkValues_.push_back(value);
                Requests_.push_back({
                    globalRefHunkValue.ChunkId,
                    globalRefHunkValue.Offset,
                    globalRefHunkValue.Length
                });
            });
    }

    TFuture<TSharedRange<TRow>> RequestFragments()
    {
        if (Requests_.empty()) {
            return {};
        }
        return Reader_
            ->ReadFragments(Options_, std::move(Requests_))
            .Apply(
                BIND(&TSessionBase::OnFragmentsRead, MakeStrong(this)));
    }

private:
    TSharedRange<TRow> OnFragmentsRead(const std::vector<TSharedRef>& fragments)
    {
        YT_VERIFY(fragments.size() == HunkValues_.size());
        for (int index = 0; index < static_cast<int>(fragments.size()); ++index) {
            SetValueRef(HunkValues_[index], fragments[index]);
        }
        return MakeSharedRange(Rows_, Rows_, fragments);
    }
};

class THunkPayloadReader::TUnversionedSession
    : public TSessionBase<TMutableUnversionedRow>
{
public:
    using TSessionBase::TSessionBase;

    TFuture<TSharedRange<TMutableUnversionedRow>> Run()
    {
        auto hunkColumnIds = Schema_->GetHunkColumnIds();
        for (auto row : Rows_) {
            if (row) {
                for (auto id : hunkColumnIds) {
                    ProcessHunkValue(&row[id]);
                }
            }
        }
        return RequestFragments();
    }
};

class THunkPayloadReader::TVersionedSession
    : public TSessionBase<TMutableVersionedRow>
{
public:
    using TSessionBase::TSessionBase;

    TFuture<TSharedRange<TMutableVersionedRow>> Run()
    {
        std::vector<IChunkFragmentReader::TChunkFragmentRequest> requests;
        for (auto row : Rows_) {
            if (row) {
                for (int index = 0; index < row.GetValueCount(); ++index) {
                    auto& value = row.BeginValues()[index];
                    if (Schema_->Columns()[value.Id].MaxInlineHunkSize()) {
                        ProcessHunkValue(&value);
                    }
                }
            }
        }
        return RequestFragments();
    }
};

THunkPayloadReader::THunkPayloadReader(
    IChunkFragmentReaderPtr chunkFragmentReader,
    TTableSchemaPtr schema)
    : ChunkFragmentReader_(std::move(chunkFragmentReader))
    , Schema_(std::move(schema))
{ }

template <class TSession, class TRow>
TFuture<TSharedRange<TRow>> THunkPayloadReader::DoRead(
    TSharedRange<TRow> rows,
    TClientChunkReadOptions options)
{
    return New<TSession>(
        ChunkFragmentReader_,
        Schema_,
        std::move(rows),
        std::move(options))
        ->Run();
}

TFuture<TSharedRange<TMutableUnversionedRow>> THunkPayloadReader::Read(
    TSharedRange<TMutableUnversionedRow> rows,
    TClientChunkReadOptions options)
{
    return DoRead<TUnversionedSession, TMutableUnversionedRow>(std::move(rows), std::move(options));
}

TFuture<TSharedRange<TMutableVersionedRow>> THunkPayloadReader::Read(
    TSharedRange<TMutableVersionedRow> rows,
    TClientChunkReadOptions options)
{
    return DoRead<TVersionedSession, TMutableVersionedRow>(std::move(rows), std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

class THunkResolvingSchemafulUnversionedReader
    : public ISchemafulUnversionedReader
{
public:
    THunkResolvingSchemafulUnversionedReader(
        ISchemafulUnversionedReaderPtr underlying,
        IChunkFragmentReaderPtr chunkFragmentReader,
        TTableSchemaPtr schema,
        TClientChunkReadOptions options)
        : Underlying_(std::move(underlying))
        , Schema_(std::move(schema))
        , Options_(std::move(options))
        , HunkPayloadReader_(std::move(chunkFragmentReader), Schema_)
    { }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        // TODO(babenko): hunk statistics
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

    virtual IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        if (ReadyRowBatch_) {
            return std::move(ReadyRowBatch_);
        }

        UnderlyingRowBatch_ = Underlying_->Read(options);
        if (!UnderlyingRowBatch_) {
            return UnderlyingRowBatch_;
        }

        if (UnderlyingRowBatch_->IsEmpty()) {
            ReadyEvent_ = Underlying_->GetReadyEvent();
            return UnderlyingRowBatch_;
        }

        UnderlyingRows_ = UnderlyingRowBatch_->MaterializeRows();

        RowBuffer_->Clear();
        ResolvedRows_.clear();
        ResolvedRows_.reserve(UnderlyingRows_.Size());

        for (auto row : UnderlyingRows_) {
            ResolvedRows_.push_back(RowBuffer_->CaptureRow(row, /*captureValues*/ false));
        }

        auto sharedResolvedRows = MakeSharedRange(MakeRange(ResolvedRows_), MakeStrong(this));
        auto hunkPayloadReaderFuture = HunkPayloadReader_.Read(sharedResolvedRows, Options_);
        if (!hunkPayloadReaderFuture) {
            return MakeResultBatch(sharedResolvedRows);
        }

        ReadyEvent_ = hunkPayloadReaderFuture.Apply(
            BIND(&THunkResolvingSchemafulUnversionedReader::OnHunksRead, MakeStrong(this)));
        return CreateEmptyUnversionedRowBatch();
    }

private:
    const ISchemafulUnversionedReaderPtr Underlying_;
    const TTableSchemaPtr Schema_;
    const TClientChunkReadOptions Options_;

    THunkPayloadReader HunkPayloadReader_;

    TFuture<void> ReadyEvent_ = VoidFuture;

    IUnversionedRowBatchPtr UnderlyingRowBatch_;
    TSharedRange<TUnversionedRow> UnderlyingRows_;
    IUnversionedRowBatchPtr ReadyRowBatch_;

    struct TRowBufferTag
    { };
    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TRowBufferTag());

    std::vector<TMutableUnversionedRow> ResolvedRows_;


    static IUnversionedRowBatchPtr MakeResultBatch(const TSharedRange<TMutableUnversionedRow>& resolvedMutableRows)
    {
        auto resolvedImmutableRows = MakeSharedRange(
            MakeRange<TUnversionedRow>(resolvedMutableRows.Begin(), resolvedMutableRows.Size()),
            resolvedMutableRows);
        return CreateBatchFromRows(std::move(resolvedImmutableRows));
    }

    void OnHunksRead(const TSharedRange<TMutableUnversionedRow>& resolvedMutableRows)
    {
        ReadyRowBatch_ = MakeResultBatch(resolvedMutableRows);
    }
};

ISchemafulUnversionedReaderPtr CreateHunkResolvingSchemafulReader(
    ISchemafulUnversionedReaderPtr underlying,
    IChunkFragmentReaderPtr chunkFragmentReader,
    TTableSchemaPtr schema,
    TClientChunkReadOptions options)
{
    if (!schema->HasHunkColumns()) {
        return underlying;
    }
    return New<THunkResolvingSchemafulUnversionedReader>(
        std::move(underlying),
        std::move(chunkFragmentReader),
        std::move(schema),
        std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
