#include "hunks.h"

#include "config.h"
#include "private.h"

#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/chunk_fragment_reader.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/hunks.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_writer.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

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

} // namespace

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

        if (std::ssize(payload) >= Config_->PayloadSectorAlignmentLengthThreshold) {
            AppendPaddingToBuffer(static_cast<i64>(GetSectorPadding(Buffer_.Size())));
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
                Meta_->set_format(ToProto<int>(EHunkChunkFormat::Default));

                {
                    NChunkClient::NProto::TMiscExt ext;
                    ext.set_compression_codec(ToProto<int>(NCompression::ECodec::None));
                    ext.set_uncompressed_data_size(UncompressedDataSize_);
                    ext.set_compressed_data_size(UncompressedDataSize_);
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
        UncompressedDataSize_ += size;
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
        AppendPaddingToBuffer(static_cast<i64>(GetSectorPadding(Buffer_.Size())));
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

class THunkEncodingVersionedWriter
    : public IVersionedChunkWriter
{
public:
    THunkEncodingVersionedWriter(
        IVersionedChunkWriterPtr underlying,
        TTableSchemaPtr schema,
        IHunkChunkPayloadWriterPtr hunkChunkPayloadWriter)
        : Underlying_(std::move(underlying))
        , Schema_(std::move(schema))
        , HunkChunkPayloadWriter_(std::move(hunkChunkPayloadWriter))
    { }

    virtual bool Write(TRange<TVersionedRow> rows) override
    {
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

                auto hunkValue = ReadHunkValue(GetValueRef(value));
                Visit(
                    hunkValue,
                    [&] (const TInlineHunkValue& inlineHunkValue) {
                        auto payloadLength = static_cast<i64>(inlineHunkValue.Payload.Size());
                        if (payloadLength < *maxInlineHunkSize) {
                            // Leave as is.
                            return;
                        }

                        HunkCount_ += 1;
                        TotalHunkLength_ += payloadLength;

                        auto [offset, hunkWriterReady] = HunkChunkPayloadWriter_->WriteHunk(inlineHunkValue.Payload);
                        ready &= hunkWriterReady;

                        auto localizedPayload = WriteHunkValue(
                            pool,
                            TLocalRefHunkValue{
                                .ChunkIndex = GetHunkChunkPayloadWriterChunkIndex(),
                                .Length = payloadLength,
                                .Offset = offset
                            });
                        SetValueRef(&value, localizedPayload);
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
    const TTableSchemaPtr Schema_;
    const IHunkChunkPayloadWriterPtr HunkChunkPayloadWriter_;

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
    IHunkChunkPayloadWriterPtr hunkChunkPayloadWriter)
{
    if (!schema->HasHunkColumns()) {
        return underlying;
    }
    return New<THunkEncodingVersionedWriter>(
        std::move(underlying),
        std::move(schema),
        std::move(hunkChunkPayloadWriter));
}

////////////////////////////////////////////////////////////////////////////////

class THunkPayloadReader
{
public:
    THunkPayloadReader(
        IChunkFragmentReaderPtr chunkFragmentReader,
        TTableSchemaPtr schema,
        TClientChunkReadOptions options);

    TFuture<TSharedRange<TMutableUnversionedRow>> ReadAndDecode(
        TSharedRange<TMutableUnversionedRow> rows);
    TFuture<TSharedRange<TMutableVersionedRow>> ReadAndDecode(
        TSharedRange<TMutableVersionedRow> rows);
    TFuture<TSharedRange<TMutableVersionedRow>> ReadAndInline(
        TSharedRange<TMutableVersionedRow> rows,
        const THashSet<TChunkId>& hunkChunkIdsToForceInline);

private:
    template <class TRow>
    class TSessionBase;
    template <class TRow>
    class TDecodeSessionBase;
    class TUnversionedDecodeSession;
    class TVersionedDecodeSession;
    class TVersionedInlineSession;

    const IChunkFragmentReaderPtr ChunkFragmentReader_;
    const TTableSchemaPtr Schema_;
    const TClientChunkReadOptions Options_;

    template <class TSession, class TRow, class... TArgs>
    TFuture<TSharedRange<TRow>> DoRead(TSharedRange<TRow> rows, TArgs&&... args);
};

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

    void RegisterRequest(
        TUnversionedValue* value,
        const TGlobalRefHunkValue& globalRefHunkValue)
    {
        HunkValues_.push_back(value);
        Requests_.push_back({
            globalRefHunkValue.ChunkId,
            globalRefHunkValue.Offset,
            globalRefHunkValue.Length
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

    virtual TSharedRange<TRow> OnFragmentsRead(const std::vector<TSharedRef>& fragments) = 0;
};

template <class TRow>
class THunkPayloadReader::TDecodeSessionBase
    : public TSessionBase<TRow>
{
protected:
    using TSessionBase<TRow>::TSessionBase;

    void ProcessHunkValue(TUnversionedValue* value)
    {
        if (value->Type == EValueType::Null) {
            return;
        }

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
                this->RegisterRequest(value, globalRefHunkValue);
            });
    }

    virtual TSharedRange<TRow> OnFragmentsRead(const std::vector<TSharedRef>& fragments) override
    {
        YT_VERIFY(fragments.size() == this->HunkValues_.size());
        for (int index = 0; index < static_cast<int>(fragments.size()); ++index) {
            SetValueRef(this->HunkValues_[index], fragments[index]);
        }
        return MakeSharedRange(this->Rows_, this->Rows_, fragments);
    }
};

class THunkPayloadReader::TUnversionedDecodeSession
    : public TDecodeSessionBase<TMutableUnversionedRow>
{
public:
    using TDecodeSessionBase<TMutableUnversionedRow>::TDecodeSessionBase;

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

class THunkPayloadReader::TVersionedDecodeSession
    : public TDecodeSessionBase<TMutableVersionedRow>
{
public:
    using TDecodeSessionBase::TDecodeSessionBase;

    TFuture<TSharedRange<TMutableVersionedRow>> Run()
    {
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

class THunkPayloadReader::TVersionedInlineSession
    : public TSessionBase<TMutableVersionedRow>
{
public:
    using TSessionBase<TMutableVersionedRow>::TSessionBase;

    TFuture<TSharedRange<TMutableVersionedRow>> Run(const THashSet<TChunkId>& hunkChunkIdsToForceInline)
    {
        for (auto row : Rows_) {
            if (row) {
                for (int index = 0; index < row.GetValueCount(); ++index) {
                    auto& value = row.BeginValues()[index];
                    if (auto maxInlineHunkSize = Schema_->Columns()[value.Id].MaxInlineHunkSize()) {
                        ProcessHunkValue(&value, *maxInlineHunkSize, hunkChunkIdsToForceInline);
                    }
                }
            }
        }
        return RequestFragments();
    }

private:
    void ProcessHunkValue(
        TUnversionedValue* value,
        i64 maxInlineHunkSize,
        const THashSet<TChunkId>& hunkChunkIdsToForceInline)
    {
        if (value->Type == EValueType::Null) {
            return;
        }

        auto hunkValue = ReadHunkValue(GetValueRef(*value));
        const auto* globalRefHunkValue = std::get_if<TGlobalRefHunkValue>(&hunkValue);
        if (!globalRefHunkValue) {
            return;
        }

        if (globalRefHunkValue->Length <= maxInlineHunkSize ||
            hunkChunkIdsToForceInline.contains(globalRefHunkValue->ChunkId))
        {
            RegisterRequest(value, *globalRefHunkValue);
        }
    }

    virtual TSharedRange<TMutableVersionedRow> OnFragmentsRead(const std::vector<TSharedRef>& fragments) override
    {
        YT_VERIFY(fragments.size() == HunkValues_.size());

        size_t bufferSize = 0;
        for (const auto& fragment : fragments) {
            bufferSize += GetInlineHunkValueSize(TInlineHunkValue{fragment});
        }

        struct TBufferTag
        { };
        auto buffer = TSharedMutableRef::Allocate<TBufferTag>(bufferSize, false);

        auto* currentPtr = buffer.Begin();
        for (int index = 0; index < std::ssize(fragments); ++index) {
            auto payload = WriteHunkValue(currentPtr, TInlineHunkValue{fragments[index]});
            SetValueRef(HunkValues_[index], payload);
            currentPtr += payload.Size();
        }

        return MakeSharedRange(Rows_, Rows_, std::move(buffer));
    }
};

THunkPayloadReader::THunkPayloadReader(
    IChunkFragmentReaderPtr chunkFragmentReader,
    TTableSchemaPtr schema,
    TClientChunkReadOptions options)
    : ChunkFragmentReader_(std::move(chunkFragmentReader))
    , Schema_(std::move(schema))
    , Options_(std::move(options))
{ }

template <class TSession, class TRow, class... TArgs>
TFuture<TSharedRange<TRow>> THunkPayloadReader::DoRead(
    TSharedRange<TRow> rows,
    TArgs&&... args)
{
    return New<TSession>(
        ChunkFragmentReader_,
        Schema_,
        std::move(rows),
        Options_)
        ->Run(std::forward<TArgs>(args)...);
}

TFuture<TSharedRange<TMutableUnversionedRow>> THunkPayloadReader::ReadAndDecode(
    TSharedRange<TMutableUnversionedRow> rows)
{
    return DoRead<TUnversionedDecodeSession, TMutableUnversionedRow>(
        std::move(rows));
}

TFuture<TSharedRange<TMutableVersionedRow>> THunkPayloadReader::ReadAndDecode(
    TSharedRange<TMutableVersionedRow> rows)
{
    return DoRead<TVersionedDecodeSession, TMutableVersionedRow>(
        std::move(rows));
}

TFuture<TSharedRange<TMutableVersionedRow>> THunkPayloadReader::ReadAndInline(
    TSharedRange<TMutableVersionedRow> rows,
    const THashSet<TChunkId>& hunkChunkIdsToForceInline)
{
    return DoRead<TVersionedInlineSession, TMutableVersionedRow>(
        std::move(rows),
        hunkChunkIdsToForceInline);
}

////////////////////////////////////////////////////////////////////////////////

template <class TRow>
TFuture<TSharedRange<TRow>> ReadAndDecodeHunks(
    IChunkFragmentReaderPtr chunkFragmentReader,
    TTableSchemaPtr schema,
    TClientChunkReadOptions options,
    TSharedRange<TRow> rows)
{
    THunkPayloadReader hunkReader(
        std::move(chunkFragmentReader),
        std::move(schema),
        std::move(options));
    return hunkReader.ReadAndDecode(std::move(rows));
}

template
TFuture<TSharedRange<TMutableUnversionedRow>> ReadAndDecodeHunks<TMutableUnversionedRow>(
    IChunkFragmentReaderPtr chunkFragmentReader,
    TTableSchemaPtr schema,
    TClientChunkReadOptions options,
    TSharedRange<TMutableUnversionedRow> rows);

template
TFuture<TSharedRange<TMutableVersionedRow>> ReadAndDecodeHunks<TMutableVersionedRow>(
    IChunkFragmentReaderPtr chunkFragmentReader,
    TTableSchemaPtr schema,
    TClientChunkReadOptions options,
    TSharedRange<TMutableVersionedRow> rows);

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
        TTableSchemaPtr schema,
        TClientChunkReadOptions options)
        : Config_(std::move(config))
        , Underlying_(std::move(underlying))
        , Schema_(std::move(schema))
        , Options_(std::move(options))
        , Logger(TabletNodeLogger.WithTag("ReadSessionId: %v",
            Options_.ReadSessionId))
        , HunkPayloadReader_(
            std::move(chunkFragmentReader),
            Schema_,
            Options_)
    { }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        // TODO(babenko): hunk statistics
        return Underlying_->GetDataStatistics();
    }

    virtual TCodecStatistics GetDecompressionStatistics() const override
    {
        // TODO(babenko): hunk statistics
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
    const TTableSchemaPtr Schema_;
    const TClientChunkReadOptions Options_;

    const NLogging::TLogger Logger;

    THunkPayloadReader HunkPayloadReader_;

    TFuture<void> ReadyEvent_ = VoidFuture;

    using IRowBatchPtr = typename TRowBatchTrait<TImmutableRow>::IRowBatchPtr;

    IRowBatchPtr UnderlyingRowBatch_;
    TSharedRange<TImmutableRow> EncodedRows_;

    std::vector<TMutableRow> MutableRows_;
    std::vector<TRange<TMutableRow>> MutableRowSlices_;

    int CurrentRowSliceIndex_ = -1;
    IRowBatchPtr ReadyRowBatch_;

    struct TRowBufferTag
    { };
    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TRowBufferTag());


    template <class THunkCounter, class TRowReader>
    IRowBatchPtr DoRead(
        const TRowBatchReadOptions& options,
        const THunkCounter& hunkCounter,
        const TRowReader& rowReader)
    {
        if (ReadyRowBatch_) {
            return std::move(ReadyRowBatch_);
        }

        ++CurrentRowSliceIndex_;

        if (CurrentRowSliceIndex_ >= std::ssize(MutableRowSlices_)) {
            CurrentRowSliceIndex_ = -1;
            MutableRowSlices_.clear();

            UnderlyingRowBatch_ = Underlying_->Read(options);
            if (!UnderlyingRowBatch_) {
                return nullptr;
            }

            if (UnderlyingRowBatch_->IsEmpty()) {
                ReadyEvent_ = Underlying_->GetReadyEvent();
                return UnderlyingRowBatch_;
            }

            EncodedRows_ = UnderlyingRowBatch_->MaterializeRows();

            YT_LOG_DEBUG("Hunk-encoded rows materialized (RowCount: %v)",
                EncodedRows_.size());

            RowBuffer_->Clear();
            MutableRows_.clear();
            for (auto row : EncodedRows_) {
                MutableRows_.push_back(RowBuffer_->CaptureRow(row, /*captureValues*/ false));
            }

            int startSliceRowIndex = 0;
            int cumulativeHunkCount = 0;
            i64 cumulativeTotalHunkLength = 0;

            auto addRowSlice = [&] (int startRowIndex, int endRowIndex) {
                if (startRowIndex < endRowIndex) {
                    MutableRowSlices_.emplace_back(MutableRows_.data() + startRowIndex, MutableRows_.data() + endRowIndex);
                    YT_LOG_DEBUG("Hunk-encoded row slice added (StartRowIndex: %v, EndRowIndex: %v, HunkCount: %v, TotalHunkLength: %v)",
                        startRowIndex,
                        endRowIndex,
                        cumulativeHunkCount,
                        cumulativeTotalHunkLength);
                }
            };

            for (int rowIndex = 0; rowIndex < std::ssize(MutableRows_); ++rowIndex) {
                if (cumulativeHunkCount >= Config_->MaxHunkCountPerRead ||
                    cumulativeTotalHunkLength >= Config_->MaxTotalHunkLengthPerRead)
                {
                    addRowSlice(startSliceRowIndex, rowIndex);
                    startSliceRowIndex = rowIndex;
                    cumulativeHunkCount = 0;
                    cumulativeTotalHunkLength = 0;
                }
                auto row = MutableRows_[rowIndex];
                auto [hunkCount, totalHunkLength] = hunkCounter(row);
                cumulativeHunkCount += hunkCount;
                cumulativeTotalHunkLength += totalHunkLength;
            }
            addRowSlice(startSliceRowIndex, std::ssize(MutableRows_));

            CurrentRowSliceIndex_ = 0;
        }

        YT_VERIFY(CurrentRowSliceIndex_ < std::ssize(MutableRowSlices_));

        YT_LOG_DEBUG("Reading hunks in row slice (SliceIndex: %v)",
            CurrentRowSliceIndex_);

        auto sharedMutableRows = MakeSharedRange(MutableRowSlices_[CurrentRowSliceIndex_], MakeStrong(this));
        auto hunkPayloadReaderFuture = rowReader(sharedMutableRows);
        if (!hunkPayloadReaderFuture) {
            return MakeBatch(sharedMutableRows);
        }

        ReadyEvent_ = hunkPayloadReaderFuture.Apply(
            BIND(&TBatchHunkReader::OnHunksRead, MakeStrong(this)));

        return CreateEmptyRowBatch<TImmutableRow>();
    }

private:
    static IRowBatchPtr MakeBatch(const TSharedRange<TMutableRow>& mutableRows)
    {
        return CreateBatchFromRows(MakeSharedRange(
            MakeRange<TImmutableRow>(mutableRows.Begin(), mutableRows.Size()),
            mutableRows));
    }

    void OnHunksRead(const TSharedRange<TMutableRow>& mutableRows)
    {
        ReadyRowBatch_ = MakeBatch(mutableRows);
    }
};

class THunkDecodingSchemafulUnversionedReader
    : public TBatchHunkReader<ISchemafulUnversionedReader, TUnversionedRow, TMutableUnversionedRow>
{
public:
    using TBatchHunkReader<ISchemafulUnversionedReader, TUnversionedRow, TMutableUnversionedRow>::TBatchHunkReader;

    virtual IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        return this->DoRead(
            options,
            [&] (TMutableUnversionedRow row) {
                i64 hunkCount = 0;
                i64 totalHunkLength = 0;
                for (auto id : this->Schema_->GetHunkColumnIds()) {
                    const auto& value = row[id];
                    if (value.Type == EValueType::Null) {
                        continue;
                    }

                    auto hunkValue = ReadHunkValue(GetValueRef(value));
                    Visit(
                        hunkValue,
                        [&] (const TInlineHunkValue& /*inlineHunkValue*/) {
                        },
                        [&] (const TLocalRefHunkValue& /*localRefHunkValue*/) {
                            THROW_ERROR_EXCEPTION("Unexpected local hunk reference");
                        },
                        [&] (const TGlobalRefHunkValue& globalRefHunkValue) {
                            hunkCount += 1;
                            totalHunkLength += globalRefHunkValue.Length;
                        });
                }
                return std::make_pair(hunkCount, totalHunkLength);
            },
            [&] (const TSharedRange<TMutableUnversionedRow>& sharedMutableRows) {
                return this->HunkPayloadReader_.ReadAndDecode(sharedMutableRows);
            });
    }
};

ISchemafulUnversionedReaderPtr CreateHunkDecodingSchemafulReader(
    TBatchHunkReaderConfigPtr config,
    ISchemafulUnversionedReaderPtr underlying,
    IChunkFragmentReaderPtr chunkFragmentReader,
    TTableSchemaPtr schema,
    TClientChunkReadOptions options)
{
    if (!schema->HasHunkColumns()) {
        return underlying;
    }
    return New<THunkDecodingSchemafulUnversionedReader>(
        std::move(config),
        std::move(underlying),
        std::move(chunkFragmentReader),
        std::move(schema),
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
            std::move(schema),
            std::move(options))
        , HunkChunkIdsToForceInline_(std::move(hunkChunkIdsToForceInline))
    { }

    virtual TFuture<void> Open() override
    {
        return this->Underlying_->Open();
    }

    virtual IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        return this->DoRead(
            options,
            [&] (TMutableVersionedRow row) {
                i64 hunkCount = 0;
                i64 totalHunkLength = 0;
                for (const auto* value = row.BeginValues(); value != row.EndValues(); ++value) {
                    if (value->Type == EValueType::Null) {
                        continue;
                    }

                    const auto& schema = this->Schema_->Columns()[value->Id];
                    auto maxInlineSizeHunk = schema.MaxInlineHunkSize();
                    if (!maxInlineSizeHunk) {
                        continue;
                    }

                    auto hunkValue = ReadHunkValue(GetValueRef(*value));
                    Visit(
                        hunkValue,
                        [&] (const TInlineHunkValue& /*inlineHunkValue*/) {
                        },
                        [&] (const TLocalRefHunkValue& /*localRefHunkValue*/) {
                            THROW_ERROR_EXCEPTION("Unexpected local hunk reference");
                        },
                        [&] (const TGlobalRefHunkValue& globalRefHunkValue) {
                            if (globalRefHunkValue.Length <= *maxInlineSizeHunk ||
                                HunkChunkIdsToForceInline_.contains(globalRefHunkValue.ChunkId))
                            {
                                hunkCount += 1;
                                totalHunkLength += globalRefHunkValue.Length;
                            }
                        });
                }
                return std::make_pair(hunkCount, totalHunkLength);
            },
            [&] (const TSharedRange<TMutableVersionedRow>& sharedMutableRows) {
                return this->HunkPayloadReader_.ReadAndInline(sharedMutableRows, HunkChunkIdsToForceInline_);
            });
    }

private:
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

} // namespace NYT::NTabletNode
