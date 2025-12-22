#include "chunk_meta_generator.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/client/arrow/columnar_statistics.h>
#include <yt/yt/client/arrow/schema.h>

#include <yt/yt/client/table_client/merge_table_schemas.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/value_consumer.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/random.h>

#include <yt/yt/library/formats/arrow_parser.h>

#include <yt/yt/library/erasure/public.h>

#include <yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/table.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/json/reader.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/csv/reader.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/util/future.h>
#include <contrib/libs/apache/arrow_next/cpp/src/parquet/arrow/reader.h>

namespace NYT::NChunkClient {

using namespace NTableClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////

template<class T>
TFuture<T> ToNativeFuture(arrow20::Future<T> future)
{
    auto promise = NewPromise<T>();
    // The cancellations are not supported by arrow, so this is all we need.
    future.AddCallback([future, promise] (const arrow20::Result<T>& result) {
        try {
            PARQUET_ASSIGN_OR_THROW(auto value, result);
            promise.Set(value);
        } catch (const std::exception& ex) {
            promise.Set(TError(ex));
        }
    });
    return promise;
}

////////////////////////////////////////////////////////////////////////////

template <class TExtension>
class TExtensionGuard
    : public TNonCopyable
{
public:
    explicit TExtensionGuard(const TRefCountedChunkMetaPtr& chunkMeta)
        : ChunkMeta_(chunkMeta)
    {
        YT_VERIFY(ChunkMeta_);
        YT_VERIFY(!HasProtoExtension<TExtension>(ChunkMeta_->extensions()));
    }

    TExtension& operator*()
    {
        return Extension_;
    }

    TExtension* operator->()
    {
        return &Extension_;
    }

    ~TExtensionGuard()
    {
        SetProtoExtension(ChunkMeta_->mutable_extensions(), Extension_);
    }

private:
    const TRefCountedChunkMetaPtr& ChunkMeta_;
    TExtension Extension_;
};

////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////

class TChunkMetaGeneratorBase
    : public ITableChunkMetaGenerator
{
public:
    void Generate() override
    {
        // First, we prepare whatever data is needed to fill chunk meta.
        Prepare();

        auto chunkMeta = New<TRefCountedChunkMeta>();

        FillGeneralChunkMeta(chunkMeta);

        {
            TExtensionGuard<NProto::TBlocksExt> blocksExt(chunkMeta);
            FillBlocksExt(*blocksExt);
        }

        {
            TExtensionGuard<NTableClient::NProto::TDataBlockMetaExt> dataBlockMetaExt(chunkMeta);
            FillDataBlockMetaExt(*dataBlockMetaExt);
        }

        {
            TExtensionGuard<NTableClient::NProto::TSamplesExt> samplesExt(chunkMeta);
            FillSamplesExt(*samplesExt);
        }

        {
            TExtensionGuard<NTableClient::NProto::TNameTableExt> nameTableExt(chunkMeta);
            FillNameTableExt(*nameTableExt);
        }

        {
            TExtensionGuard<NTableClient::NProto::TTableSchemaExt> schemaExt(chunkMeta);
            FillSchemaExt(*schemaExt);
        }
        if (auto columnar = GetColumnarChunkMeta()) {
            SetProtoExtension(
                chunkMeta->mutable_extensions(),
                ::NYT::ToProto<NTableClient::NProto::TColumnarStatisticsExt>(*columnar)
            );
        }

        FillAdditionalExtensions(chunkMeta);

        // Misc extension is filled last because it depends on total meta size.
        {
            TExtensionGuard<NProto::TMiscExt> miscExt(chunkMeta);
            FillMiscExt(*miscExt, chunkMeta->ByteSizeLong());
        }

        ChunkMeta_ = chunkMeta;
    }

    TRefCountedChunkMetaPtr GetChunkMeta() const override
    {
        return ChunkMeta_;
    }

protected:
    TRefCountedChunkMetaPtr ChunkMeta_;

protected:
    virtual NTableClient::TNameTablePtr GetChunkNameTable() const = 0;
    virtual EChunkFormat GetChunkFormat() const = 0;

    virtual i64 GetDataWeight() const = 0;

    virtual i64 GetMaxDataBlockSize() const = 0;

protected:
    virtual void Prepare()
    { }

    virtual void FillGeneralChunkMeta(TRefCountedChunkMetaPtr& chunkMeta)
    {
        chunkMeta->set_type(::NYT::ToProto<int>(EChunkType::Table));
        chunkMeta->set_format(::NYT::ToProto<int>(GetChunkFormat()));
    }

    virtual void FillBlocksExt(NProto::TBlocksExt& ext) = 0;
    virtual void FillDataBlockMetaExt(NTableClient::NProto::TDataBlockMetaExt& ext) = 0;
    virtual void FillSamplesExt(NTableClient::NProto::TSamplesExt& ext) = 0;

    virtual void FillNameTableExt(NTableClient::NProto::TNameTableExt& ext)
    {
        ToProto(&ext, GetChunkNameTable());
    }

    virtual void FillSchemaExt(NTableClient::NProto::TTableSchemaExt& ext)
    {
        ToProto(&ext, *GetChunkSchema());
    }

    virtual void FillAdditionalExtensions(const TRefCountedChunkMetaPtr& /*chunkMeta*/)
    { }

    virtual void FillMiscExt(NProto::TMiscExt& ext, i64 metaSize)
    {
        ext.set_uncompressed_data_size(GetUncompressedDataSize());
        ext.set_compressed_data_size(GetUncompressedDataSize());
        // TODO(achulkov2): Find a better approximation?
        ext.set_data_weight(GetDataWeight());
        ext.set_meta_size(metaSize);
        ext.set_row_count(GetRowCount());
        ext.set_compression_codec(::NYT::ToProto<int>(NCompression::ECodec::None));
        ext.set_sorted(false);
        // TODO(achulkov2): Set value_count.
        ext.set_max_data_block_size(GetMaxDataBlockSize());
        ext.set_sealed(false);
        ext.set_erasure_codec(::NYT::ToProto<int>(NErasure::ECodec::None));
        ext.set_system_block_count(0);
        ext.set_striped_erasure(false);
        // We are not setting block_format_version, since we are not dealing with versioned chunks.
    }

    virtual std::optional<TColumnarStatistics> GetColumnarChunkMeta()
    {
        return {};
    }
};

class TArrowChunkMetaGeneratorBase
    : public TChunkMetaGeneratorBase
{
public:
    TArrowChunkMetaGeneratorBase(
        EChunkFormat chunkFormat,
        std::shared_ptr<arrow20::io::RandomAccessFile> chunkFile,
        TArrowTableChunkMetaGeneratorOptions options = {})
        : ChunkFormat_(chunkFormat)
        , ChunkFile_(std::move(chunkFile))
        , Options_(std::move(options))
        , RandomGenerator_(options.SampleRandomSeed)
        , SampleThreshold_(static_cast<ui64>(MaxFloor<ui64>() * Options_.SampleRate))
    { }

protected:
    const EChunkFormat ChunkFormat_;
    const std::shared_ptr<arrow20::io::RandomAccessFile> ChunkFile_;
    const TArrowTableChunkMetaGeneratorOptions Options_;

    TRandomGenerator RandomGenerator_;
    const ui64 SampleThreshold_;
    std::vector<NTableClient::TUnversionedOwningRow> Samples_;

    EChunkFormat GetChunkFormat() const override
    {
        return ChunkFormat_;
    }

    i64 GetUnderlyingFileSize() const
    {
        PARQUET_ASSIGN_OR_THROW(auto chunkFileSize, ChunkFile_->GetSize());
        return chunkFileSize;
    }

    i64 GetUncompressedDataSize() const override
    {
        return GetUnderlyingFileSize();
    }

    i64 GetCompressedDataSize() const override
    {
        return GetUnderlyingFileSize();
    }

    // TODO(achulkov2): Think of some better approximation.
    i64 GetDataWeight() const override
    {
        return GetUnderlyingFileSize();
    }

    TNameTablePtr GetChunkNameTable() const override
    {
        return NTableClient::TNameTable::FromSchema(*GetChunkSchema());
    }

    void FillSamplesExt(NTableClient::NProto::TSamplesExt& ext) override
    {
        int rowCount = GetRowCount();
        if (rowCount == 0) {
            return;
        }

        THashSet<int> sampledRows;
        int numRowsToSample = std::ceil(static_cast<double>(rowCount) * Options_.SampleRate);
        while (std::ssize(sampledRows) < numRowsToSample) {
            int rowIndex = RandomGenerator_.Generate<ui64>() % Samples_.size();
            if (sampledRows.insert(rowIndex).second) {
                // This approach is taken from the schemaless_chunk_writer and TruncateUnversionedValues
                // is invoked with the same TUnversionedValueRangeTruncationOptions.
                auto truncatedSampleValueBuffer = New<TRowBuffer>();
                auto sampleValues = TruncateUnversionedValues(
                    Samples_[rowIndex].Elements(),
                    truncatedSampleValueBuffer, {.ClipAfterOverflow = false, .MaxTotalSize = MaxSampleSize});

                auto entry = SerializeToString(sampleValues.Values);
                ext.add_entries(entry);
                ext.add_weights(sampleValues.Size);
            }
        }

        YT_VERIFY(ext.entries_size() > 0);
    }
};

template <typename TStreamingReader>
    requires std::derived_from<TStreamingReader, arrow20::RecordBatchReader>
class TArrowStreamingReaderChunkMetaGeneratorBase
    : public TArrowChunkMetaGeneratorBase
{
private:
    struct TBlock
    {
        i64 Offset;
        i64 RowCount;
    };

    i64 RowCount_ = 0;
    i64 MaxDataBlockSize_ = 0;
    std::vector<TBlock> Blocks_;
    TTableSchemaPtr Schema_;

public:
    using TArrowChunkMetaGeneratorBase::TArrowChunkMetaGeneratorBase;

protected:
    virtual int64_t GetCurrentFileOffset(const TStreamingReader& streamingReader) = 0;

    virtual arrow20::Future<std::shared_ptr<arrow20::RecordBatch>> ReadNextAsync(TStreamingReader& streamingReader) = 0;

    std::optional<TColumnarStatistics> GetColumnarChunkMeta() override
    {
        return ColumnarStatistics_;
    }

    void PrepareFromStreamingReader(std::shared_ptr<TStreamingReader> streamingReader)
    {
        Schema_ = NArrow::CreateYTTableSchemaFromArrowSchema(streamingReader->schema());
        ColumnarStatistics_ = TColumnarStatistics::MakeEmpty(Schema_->GetColumnCount());
        while (true) {
            auto offset = GetCurrentFileOffset(*streamingReader);
            auto batch = WaitFor(ToNativeFuture(ReadNextAsync(*streamingReader)))
                .ValueOrThrow();
            if (!batch) {
                break;
            }

            i64 batchRowCount = batch->num_rows();

            RowCount_ += batchRowCount;
            MaxDataBlockSize_ = std::max(MaxDataBlockSize_, GetCurrentFileOffset(*streamingReader) - offset);
            Blocks_.push_back({.Offset = offset, .RowCount = batchRowCount});
            // This may error as it is not as flexible as the type inference inside the batches, but we are willing to take the risk.
            // Might improve later.
            Schema_ = MergeTableSchemas(Schema_, NArrow::CreateYTTableSchemaFromArrowSchema(batch->schema()));
            ColumnarStatistics_ += NArrow::ExtractColumnarStatistics(batch);

            // Decode the batch and save some samples to return them later; we will save the maximum amount
            // from every batch as we don't know the total number of rows yet.
            TCollectingValueConsumer consumer{GetChunkSchema()};
            PARQUET_THROW_NOT_OK(NFormats::DecodeRecordBatch(batch, &consumer));

            THashSet<int> sampledRows;
            int numRowsToSample = std::ceil(static_cast<double>(batchRowCount) * Options_.SampleRate);
            while (std::ssize(sampledRows) < numRowsToSample) {
                // For sample_rate <= 0.001 - which is the limit in TChunkWriterConfig - the expected value
                // of the number of calls to Generate method is ~numRowsToSample.
                int rowIndex = RandomGenerator_.Generate<ui64>() % batchRowCount;
                if (sampledRows.insert(rowIndex).second) {
                    Samples_.emplace_back(consumer.GetRow(rowIndex));
                }
            }
        }
        Blocks_.push_back({.Offset = GetUnderlyingFileSize(), .RowCount = 0});

        // With the algorithm above we should have more than enough rows to sample from, but
        // just in case let's perform this sanity check.
        int totalNumRowsToSample = std::ceil(static_cast<double>(GetRowCount()) * Options_.SampleRate);
        YT_VERIFY(totalNumRowsToSample <= std::ssize(Samples_));
    }

    i64 GetRowCount() const override
    {
        return RowCount_;
    }

    i64 GetMaxDataBlockSize() const override
    {
        return MaxDataBlockSize_;
    }

    TTableSchemaPtr GetChunkSchema() const override
    {
        return Schema_;
    }

    void FillBlocksExt(NProto::TBlocksExt& ext) override
    {
        for (size_t i = 1; i < Blocks_.size(); i++) {
            auto& block = Blocks_[i - 1];
            auto blockInfo = ext.add_blocks();
            blockInfo->set_size(Blocks_[i].Offset - block.Offset);
            blockInfo->set_offset(block.Offset);
            blockInfo->set_checksum(NullChecksum);
        }
    }

    void FillDataBlockMetaExt(NTableClient::NProto::TDataBlockMetaExt& ext) override
    {
        i64 chunkRowCount = 0;
        for (size_t i = 1; i < Blocks_.size(); i++) {
            auto& block = Blocks_[i - 1];
            auto dataBlockMeta = ext.add_data_blocks();
            dataBlockMeta->set_block_index(i - 1);
            dataBlockMeta->set_uncompressed_size(Blocks_[i].Offset - block.Offset);
            dataBlockMeta->set_row_count(block.RowCount);
            dataBlockMeta->set_chunk_row_count(chunkRowCount += block.RowCount);
        }
    }

private:
    TColumnarStatistics ColumnarStatistics_;
};

class TJsonChunkMetaGenerator
    : public TArrowStreamingReaderChunkMetaGeneratorBase<arrow20::json::StreamingReader>
{
public:
    TJsonChunkMetaGenerator(
        std::shared_ptr<arrow20::io::RandomAccessFile> chunkFile,
        TArrowTableChunkMetaGeneratorOptions options = {})
        : TArrowStreamingReaderChunkMetaGeneratorBase<arrow20::json::StreamingReader>(
            EChunkFormat::TableUnversionedArrowJsonLines,
            std::move(chunkFile),
            std::move(options))
    { }

protected:
    int64_t GetCurrentFileOffset(const arrow20::json::StreamingReader& streamingReader) override
    {
        // The docs say it is safe to separate rows using this.
        // The implementation only increments `bytes_processed` by header row during the initialization or by whole requested batches synchronously:
        // https://github.com/apache/arrow/blob/apache-arrow-20.0.0/cpp/src/arrow/json/reader.cc#L458
        // https://github.com/apache/arrow/blob/apache-arrow-20.0.0/cpp/src/arrow/json/reader.cc#L367
        return streamingReader.bytes_processed();
    }

    arrow20::Future<std::shared_ptr<arrow20::RecordBatch>> ReadNextAsync(arrow20::json::StreamingReader& streamingReader) override
    {
        return streamingReader.ReadNextAsync();
    }

    void Prepare() override
    {
        PrepareFromStreamingReader(WaitFor(ToNativeFuture(arrow20::json::StreamingReader::MakeAsync(
            ChunkFile_,
            arrow20::json::ReadOptions::Defaults(),
            arrow20::json::ParseOptions::Defaults(),
            arrow20::io::IOContext(arrow20::default_memory_pool()))))
            .ValueOrThrow());
    }
};

class TCsvChunkMetaGenerator
    : public TArrowStreamingReaderChunkMetaGeneratorBase<arrow20::csv::StreamingReader>
{
public:
    TCsvChunkMetaGenerator(
        std::shared_ptr<arrow20::io::RandomAccessFile> chunkFile,
        TArrowTableChunkMetaGeneratorOptions options = {})
        : TArrowStreamingReaderChunkMetaGeneratorBase<arrow20::csv::StreamingReader>(
            EChunkFormat::TableUnversionedArrowCsv,
            std::move(chunkFile),
            std::move(options))
    { }

protected:
    int64_t GetCurrentFileOffset(const arrow20::csv::StreamingReader& streamingReader) override
    {
        // The docs say it is safe to separate rows using this.
        // The implementation only increments `bytes_read` by header row during the initialization or by whole requested batches synchronously:
        // https://github.com/apache/arrow/blob/apache-arrow-20.0.0/cpp/src/arrow/csv/reader.cc#L884
        // https://github.com/apache/arrow/blob/apache-arrow-20.0.0/cpp/src/arrow/csv/reader.cc#L938
        return streamingReader.bytes_read();
    }

    arrow20::Future<std::shared_ptr<arrow20::RecordBatch>> ReadNextAsync(arrow20::csv::StreamingReader& streamingReader) override
    {
        return streamingReader.ReadNextAsync();
    }

    void Prepare() override
    {
        PrepareFromStreamingReader(WaitFor(ToNativeFuture(arrow20::csv::StreamingReader::MakeAsync(
            arrow20::io::IOContext(arrow20::default_memory_pool()),
            ChunkFile_,
            arrow20::internal::GetCpuThreadPool(),
            arrow20::csv::ReadOptions::Defaults(),
            arrow20::csv::ParseOptions::Defaults(),
            arrow20::csv::ConvertOptions::Defaults())))
            .ValueOrThrow());
    }
};

class TParquetChunkMetaGenerator
    : public TArrowChunkMetaGeneratorBase
{
public:
    TParquetChunkMetaGenerator(
        std::shared_ptr<arrow20::io::RandomAccessFile> chunkFile,
        TArrowTableChunkMetaGeneratorOptions options = {})
        : TArrowChunkMetaGeneratorBase(
            EChunkFormat::TableUnversionedArrowParquet,
            std::move(chunkFile),
            std::move(options))
    { }

private:
    std::unique_ptr<parquet20::arrow20::FileReader> ArrowParquetFileReader_;
    std::shared_ptr<parquet20::FileMetaData> ParquetFileMeta_;
    TTableSchemaPtr Schema_;
    std::vector<i64> RowGroupOffsets_;
    i64 MaxRowGroupSize_ = 0;

    //! Represents the size of the Parquet magic string "PAR1" at the end of the file.
    static constexpr i64 ParquetMagicSize = 4;
    //! Represents a 4 byte little-endian integer containing the length of the metadata block.
    static constexpr i64 MetadataLengthFieldSize = 4;

    //! Returns minimum non-zero offset from the given vector of offsets.
    static i64 GetMinNonZeroOffset(const std::vector<i64>& offsets)
    {
        i64 minOffset = std::numeric_limits<i64>::max();
        for (i64 offset : offsets) {
            if (offset > 0 && offset < minOffset) {
                minOffset = offset;
            }
        }

        return minOffset;
    }

    void Prepare() override
    {
        PARQUET_ASSIGN_OR_THROW(ArrowParquetFileReader_, parquet20::arrow20::OpenFile(ChunkFile_, arrow20::default_memory_pool()));
        ParquetFileMeta_ = ArrowParquetFileReader_->parquet_reader()->metadata();
        std::shared_ptr<arrow20::Schema> arrowSchema;
        PARQUET_THROW_NOT_OK(ArrowParquetFileReader_->GetSchema(&arrowSchema));
        Schema_ = NArrow::CreateYTTableSchemaFromArrowSchema(arrowSchema);

        RowGroupOffsets_.reserve(ParquetFileMeta_->num_row_groups() + 1);

        for (int rowGroupIndex = 0; rowGroupIndex < ParquetFileMeta_->num_row_groups(); ++rowGroupIndex) {
            auto rowGroupMeta = ParquetFileMeta_->RowGroup(rowGroupIndex);

            i64 rowGroupOffset = std::numeric_limits<i64>::max();

            for (int columnIndex = 0; columnIndex < rowGroupMeta->num_columns(); ++columnIndex) {
                auto columnChunkMeta = rowGroupMeta->ColumnChunk(columnIndex);
                // Zero offsets in Parquet indicate that the offset is not applicable.
                // Real offsets can never be zero, because parquet files start with the magic bytes "PAR1".
                auto columnOffset = GetMinNonZeroOffset({
                    columnChunkMeta->file_offset(),
                    columnChunkMeta->data_page_offset(),
                    columnChunkMeta->dictionary_page_offset(),
                    columnChunkMeta->index_page_offset(),
                });
                rowGroupOffset = std::min(rowGroupOffset, columnOffset);
            }

            RowGroupOffsets_.push_back(rowGroupOffset);
        }

        i64 metadataFooterSize = ParquetFileMeta_->size() + ParquetMagicSize + MetadataLengthFieldSize;
        RowGroupOffsets_.push_back(GetUnderlyingFileSize() - metadataFooterSize);

        for (int rowGroupIndex = 0; rowGroupIndex < ParquetFileMeta_->num_row_groups(); ++rowGroupIndex) {
            auto startOffset = RowGroupOffsets_[rowGroupIndex];
            auto endOffset = RowGroupOffsets_[rowGroupIndex + 1];

            if (startOffset < 0 || endOffset > GetUnderlyingFileSize() || startOffset >= endOffset) {
                THROW_ERROR_EXCEPTION("Invalid row group offsets")
                    << TErrorAttribute("row_group_index", rowGroupIndex)
                    << TErrorAttribute("start_offset", startOffset)
                    << TErrorAttribute("end_offset", endOffset)
                    << TErrorAttribute("num_row_groups", ParquetFileMeta_->num_row_groups())
                    << TErrorAttribute("file_size", GetUnderlyingFileSize());
            }

            MaxRowGroupSize_ = std::max(MaxRowGroupSize_, endOffset - startOffset);
        }

        // TODO(pavel-bash): Our goal is to only read the file(s) once during Prepare and don't touch
        // them anymore. Before this line we only have read the meta of the file(s). In the lines
        // that follow we perform the actual reading of the file(s) in order to collect samples in the separate
        // functions. This approach works as we don't have any other data we need to collect from the file itself right now.
        // However, if we need to add more info (e.g. first and last keys of the blocks), we will have to
        // pack this code into a loop which reads every row group and performs some actions on it.
        switch (Options_.SampleStrategy) {
            case NTableClient::EChunkMetaSampleGenerationStrategy::Fast: {
                ExtractSamplesPerRow();
                break;
            }
            case NTableClient::EChunkMetaSampleGenerationStrategy::Precise: {
                ExtractSamplesFullFile();
                break;
            }
            default:
                YT_ABORT();
        }
        // We must have extracted at least one sample if we have at least one row.
        YT_VERIFY(GetRowCount() == 0 || !Samples_.empty());
    }

    i64 GetRowCount() const override
    {
        return ParquetFileMeta_->num_rows();
    }

    i64 GetMaxDataBlockSize() const override
    {
        return MaxRowGroupSize_;
    }

    TTableSchemaPtr GetChunkSchema() const override
    {
        return Schema_;
    }

    void FillBlocksExt(NProto::TBlocksExt& ext) override
    {
        for (int rowGroupIndex = 0; rowGroupIndex < ParquetFileMeta_->num_row_groups(); ++rowGroupIndex) {
            auto startOffset = RowGroupOffsets_[rowGroupIndex];
            auto endOffset = RowGroupOffsets_[rowGroupIndex + 1];

            auto* blockInfo = ext.add_blocks();
            blockInfo->set_offset(startOffset);
            blockInfo->set_size(endOffset - startOffset);
            blockInfo->set_checksum(NullChecksum);
        }
    }

    void FillDataBlockMetaExt(NTableClient::NProto::TDataBlockMetaExt& ext) override
    {
        i64 currentRowCount = 0;

        for (int rowGroupIndex = 0; rowGroupIndex < ParquetFileMeta_->num_row_groups(); ++rowGroupIndex) {
            auto startOffset = RowGroupOffsets_[rowGroupIndex];
            auto endOffset = RowGroupOffsets_[rowGroupIndex + 1];

            auto rowCount = ParquetFileMeta_->RowGroup(rowGroupIndex)->num_rows();
            currentRowCount += rowCount;

            auto* dataBlockMeta = ext.add_data_blocks();
            dataBlockMeta->set_row_count(rowCount);
            dataBlockMeta->set_chunk_row_count(currentRowCount);
            dataBlockMeta->set_uncompressed_size(endOffset - startOffset);
            dataBlockMeta->set_block_index(rowGroupIndex);
        }
    }

    std::unique_ptr<arrow20::RecordBatchReader> GetRecordBatchReaderOrThrow(const std::vector<int>& rowGroups)
    {
        auto recordBatchReaderResult = ArrowParquetFileReader_->GetRecordBatchReader(rowGroups);
        PARQUET_THROW_NOT_OK(recordBatchReaderResult.status());

        // This is the recommended way (from the docs) to unpack the arrow's result.
        return std::move(recordBatchReaderResult).ValueOrDie();
    }

    //! In this algorithm we will understand roughly how many rows are to be included
    //! in the result, and then take as equal amount of first rows in each row group
    //! as possible. It's not the best if data is not shuffled or has some distribution
    //! logic across a single row group, but at least it should be fast.
    void ExtractSamplesPerRow()
    {
        if (GetRowCount() == 0) {
            return;
        }
        int rowCount = GetRowCount();
        int rowGroupsCount = ArrowParquetFileReader_->num_row_groups();

        int numRowsToSample = std::ceil(static_cast<double>(rowCount) * Options_.SampleRate);
        int numRowsToSamplePerGroup = std::ceil(static_cast<double>(numRowsToSample) / rowGroupsCount);

        // We set the batch_size to the number of rows we want to read from every
        // row group, so that one call to ReadNext reads exactly that amount or less.
        i64 originalBatchSize = ArrowParquetFileReader_->properties().batch_size();
        auto batchSizeGuard = Finally([this, originalBatchSize] {
            ArrowParquetFileReader_->set_batch_size(originalBatchSize);
        });
        ArrowParquetFileReader_->set_batch_size(numRowsToSamplePerGroup);

        std::unique_ptr<arrow20::RecordBatchReader> recordBatchReader;
        std::shared_ptr<arrow20::RecordBatch> batch;
        for (int rowGroupIndex = 0; rowGroupIndex < rowGroupsCount; ++rowGroupIndex) {
            recordBatchReader = GetRecordBatchReaderOrThrow({rowGroupIndex});
            PARQUET_THROW_NOT_OK(recordBatchReader->ReadNext(&batch));
            if (!batch) {
                continue;
            }

            TCollectingValueConsumer consumer{GetChunkSchema()};
            PARQUET_THROW_NOT_OK(NFormats::DecodeRecordBatch(batch, &consumer));
            for (const auto& row: consumer.GetRowList()) {
                Samples_.emplace_back(row);
            }
        }

        YT_VERIFY(!Samples_.empty());
    }

    //! In this algorithm we will read the whole file and throw a dice for every row,
    //! thus making the result as precise as possible.
    void ExtractSamplesFullFile()
    {
        if (GetRowCount() == 0) {
            return;
        }

        std::unique_ptr<arrow20::RecordBatchReader> recordBatchReader;
        std::shared_ptr<arrow20::RecordBatch> batch;

        // Strangely, there's no GetRecordBatchReader() to just read all row groups,
        // so we need to create this helper vector to specify all row groups.
        std::vector<int> rowGroups(ArrowParquetFileReader_->num_row_groups());
        std::iota(rowGroups.begin(), rowGroups.end(), 0);
        recordBatchReader = GetRecordBatchReaderOrThrow(rowGroups);

        PARQUET_THROW_NOT_OK(recordBatchReader->ReadNext(&batch));
        while (batch != nullptr) {
            // Throw a dice for every row in this batch without actually decoding the
            // batch; if no rows were selected, we can just skip it.
            THashSet<i64> selectedRows;
            for (i64 rowIndex = 0; rowIndex < batch->num_rows(); ++rowIndex) {
                if (RandomGenerator_.Generate<ui64>() < SampleThreshold_) {
                    selectedRows.insert(rowIndex);
                }
            }

            if (selectedRows.empty()) {
                PARQUET_THROW_NOT_OK(recordBatchReader->ReadNext(&batch));
                continue;
            }

            TCollectingValueConsumer consumer{GetChunkSchema()};
            PARQUET_THROW_NOT_OK(NFormats::DecodeRecordBatch(batch, &consumer));
            for (i64 rowIndex: selectedRows) {
                Samples_.emplace_back(consumer.GetRow(rowIndex));
            }

            PARQUET_THROW_NOT_OK(recordBatchReader->ReadNext(&batch));
        }

        if (Samples_.empty()) {
            // If no rows were selected, get a random row from a random row group's beginning.
            rowGroups = {std::abs(RandomGenerator_.Generate<int>()) % ArrowParquetFileReader_->num_row_groups()};
            recordBatchReader = GetRecordBatchReaderOrThrow(rowGroups);
            PARQUET_THROW_NOT_OK(recordBatchReader->ReadNext(&batch));
            YT_VERIFY(batch != nullptr);

            TCollectingValueConsumer consumer{GetChunkSchema()};
            PARQUET_THROW_NOT_OK(NFormats::DecodeRecordBatch(batch, &consumer));
            Samples_.emplace_back(consumer.GetRow(std::abs(RandomGenerator_.Generate<int>()) % consumer.Size()));
        }
    }

    void FillAdditionalExtensions(const TRefCountedChunkMetaPtr& chunkMeta) override
    {
        TExtensionGuard<NTableClient::NProto::TParquetFormatMetaExt> parquetFormatExt(chunkMeta);
        parquetFormatExt->set_footer(ArrowParquetFileReader_->parquet_reader()->metadata()->SerializeToString());
        parquetFormatExt->set_file_size(GetUnderlyingFileSize());
    }

    std::optional<TColumnarStatistics> GetColumnarChunkMeta() override
    {
        return NArrow::ExtractColumnarStatistics(*ParquetFileMeta_);
    }
};

////////////////////////////////////////////////////////////////////////////

ITableChunkMetaGeneratorPtr CreateArrowTableChunkMetaGenerator(
    EChunkFormat chunkFormat,
    const std::shared_ptr<arrow20::io::RandomAccessFile>& chunkFile,
    TArrowTableChunkMetaGeneratorOptions options)
{
    switch (chunkFormat) {
        case EChunkFormat::TableUnversionedArrowJsonLines:
            return New<TJsonChunkMetaGenerator>(chunkFile, std::move(options));
        case EChunkFormat::TableUnversionedArrowCsv:
            return New<TCsvChunkMetaGenerator>(chunkFile, std::move(options));
        case EChunkFormat::TableUnversionedArrowParquet:
            return New<TParquetChunkMetaGenerator>(chunkFile, std::move(options));
        default:
            THROW_ERROR_EXCEPTION("Unsupported chunk format %Qlv for arrow chunk meta generation", chunkFormat);
    }
}

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient