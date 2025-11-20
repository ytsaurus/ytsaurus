#include "chunk_meta_generator.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/client/arrow/columnar_statistics.h>
#include <yt/yt/client/arrow/schema.h>
#include <yt/yt/client/table_client/merge_table_schemas.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/library/erasure/public.h>

#include <yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/table.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/json/reader.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/csv/reader.h>
#include <contrib/libs/apache/arrow_next/cpp/src/parquet/arrow/reader.h>

namespace NYT::NChunkClient {

using namespace NTableClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////

namespace {

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
        std::shared_ptr<arrow20::io::RandomAccessFile> chunkFile)
        : ChunkFormat_(chunkFormat)
        , ChunkFile_(std::move(chunkFile))
    { }

protected:
    const EChunkFormat ChunkFormat_;
    const std::shared_ptr<arrow20::io::RandomAccessFile> ChunkFile_;

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
};

// TODO(achulkov2): Separate logic for single-block formats and formats convertible
// to arrow20::Table objects into two separate bases, since they are not really related.

class TSingleBlockArrowTableChunkMetaGeneratorBase
    : public TArrowChunkMetaGeneratorBase
{
public:
    using TArrowChunkMetaGeneratorBase::TArrowChunkMetaGeneratorBase;

protected:
    virtual std::shared_ptr<arrow20::Table> GetArrowTable() const = 0;

    std::optional<TColumnarStatistics> GetColumnarChunkMeta() override
    {
        return ColumnarStatistics_;
    }

    void Prepare() override
    {
        ColumnarStatistics_ = NArrow::ExtractColumnarStatistics(*GetArrowTable());
    }

    i64 GetRowCount() const override
    {
        return GetArrowTable()->num_rows();
    }

    // For single block tables, the maximum data block size is the size of the whole file.
    i64 GetMaxDataBlockSize() const override
    {
        return GetUnderlyingFileSize();
    }

    TTableSchemaPtr GetChunkSchema() const override
    {
        return NArrow::CreateYTTableSchemaFromArrowSchema(GetArrowTable()->schema());
    }

    void FillBlocksExt(NProto::TBlocksExt& ext) override
    {
        auto* blockInfo = ext.add_blocks();
        blockInfo->set_offset(0);
        blockInfo->set_size(GetUnderlyingFileSize());
        blockInfo->set_checksum(NullChecksum);
    }

    void FillDataBlockMetaExt(NTableClient::NProto::TDataBlockMetaExt& ext) override
    {
        auto* dataBlockMeta = ext.add_data_blocks();
        dataBlockMeta->set_row_count(GetRowCount());
        dataBlockMeta->set_chunk_row_count(GetRowCount());
        dataBlockMeta->set_uncompressed_size(GetUncompressedDataSize());
        dataBlockMeta->set_block_index(0);
    }

private:
    TColumnarStatistics ColumnarStatistics_;
};

template <typename TStreamingReader>
    requires std::derived_from<TStreamingReader, arrow20::RecordBatchReader> && requires(const TStreamingReader& reader) {
        { reader.bytes_read() } -> std::same_as<int64_t>;
    }
class TStreamingReaderArrowTableChunkMetaGeneratorBase
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
    std::optional<TColumnarStatistics> GetColumnarChunkMeta() override
    {
        return ColumnarStatistics_;
    }

    void PrepareFromStreamingReader(std::shared_ptr<TStreamingReader> streamingReader)
    {
        Schema_ = NArrow::CreateYTTableSchemaFromArrowSchema(streamingReader->schema());
        ColumnarStatistics_ = TColumnarStatistics::MakeEmpty(Schema_->GetColumnCount());
        while (true) {
            std::shared_ptr<arrow20::RecordBatch> batch;
            // The docs say it is safe to separate rows using this.
            // The implementation only increments `bytes_read` by header row during the initialization or by whole requested batches synchronously:
            // https://github.com/tractoai/mirror-ytsaurus-ytsaurus/blob/d20e61aecc890ce1d9df0cc27bd29f766fc32313/contrib/libs/apache/arrow/cpp/src/arrow/csv/reader.cc#L887
            // https://github.com/tractoai/mirror-ytsaurus-ytsaurus/blob/d20e61aecc890ce1d9df0cc27bd29f766fc32313/contrib/libs/apache/arrow/cpp/src/arrow/csv/reader.cc#L931
            auto offset = streamingReader->bytes_read();
            PARQUET_THROW_NOT_OK(streamingReader->ReadNext(&batch));
            if (!batch) {
                break;
            }
            RowCount_ += batch->num_rows();
            MaxDataBlockSize_ = std::max(MaxDataBlockSize_, streamingReader->bytes_read() - offset);
            Blocks_.push_back({.Offset = offset, .RowCount = batch->num_rows()});
            // This may error as it is not as flexible as the type inference inside the batches, but we are willing to take the risk.
            // Might improve later.
            Schema_ = MergeTableSchemas(Schema_, NArrow::CreateYTTableSchemaFromArrowSchema(batch->schema()));
            ColumnarStatistics_ += NArrow::ExtractColumnarStatistics(batch);
        }
        Blocks_.push_back({.Offset = GetUnderlyingFileSize(), .RowCount = 0});
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
    : public TSingleBlockArrowTableChunkMetaGeneratorBase
{
public:
    TJsonChunkMetaGenerator(std::shared_ptr<arrow20::io::RandomAccessFile> chunkFile)
        : TSingleBlockArrowTableChunkMetaGeneratorBase(
            EChunkFormat::TableUnversionedArrowJsonLines,
            std::move(chunkFile))
    { }

private:
    std::shared_ptr<arrow20::Table> ArrowTable_;

    void Prepare() override
    {
        // `TableReader::Read()` is blocking the thread executing the fiber, so we hide this in a new one.
        // `TableReader::Make()` spawns a new thread itself, so it is fine to just create another one right here.
        auto actionQueue = New<TActionQueue>("TJsonChunkMetaGenerator");
        PARQUET_ASSIGN_OR_THROW(ArrowTable_, (WaitFor(BIND(([chunkFile = ChunkFile_]() {
            PARQUET_ASSIGN_OR_THROW(auto jsonReader, arrow20::json::TableReader::Make(
                arrow20::default_memory_pool(),
                chunkFile,
                arrow20::json::ReadOptions::Defaults(),
                arrow20::json::ParseOptions::Defaults()));
            return jsonReader->Read();
        }))
            .AsyncVia(actionQueue->GetInvoker())
            .Run())
            .ValueOrThrow()));

        TSingleBlockArrowTableChunkMetaGeneratorBase::Prepare();
    }

    std::shared_ptr<arrow20::Table> GetArrowTable() const override
    {
        return ArrowTable_;
    }
};

class TCsvChunkMetaGenerator
    : public TStreamingReaderArrowTableChunkMetaGeneratorBase<arrow20::csv::StreamingReader>
{
public:
    TCsvChunkMetaGenerator(std::shared_ptr<arrow20::io::RandomAccessFile> chunkFile)
        : TStreamingReaderArrowTableChunkMetaGeneratorBase<arrow20::csv::StreamingReader>(
            EChunkFormat::TableUnversionedArrowCsv,
            std::move(chunkFile))
    { }

protected:
    void Prepare() override
    {
        // `StreamingReader::ReadNext()` is blocking the thread executing the fiber, so we hide this in a new one.
        // `StreamingReader::Make()` spawns a new thread itself, so it is fine to just create another one right here.
        auto actionQueue = New<TActionQueue>("TCsvChunkMetaGenerator");
        WaitFor(BIND(([this_ = MakeStrong(this), this]() {
            PARQUET_ASSIGN_OR_THROW(
                auto streamingReader,
                arrow20::csv::StreamingReader::Make(
                    arrow20::io::IOContext(arrow20::default_memory_pool()),
                    ChunkFile_,
                    arrow20::csv::ReadOptions::Defaults(),
                    arrow20::csv::ParseOptions::Defaults(),
                    arrow20::csv::ConvertOptions::Defaults()));
            // Continues the initialization in another thread, which is fine as the access is exclusive.
            PrepareFromStreamingReader(streamingReader);
        }))
            .AsyncVia(actionQueue->GetInvoker())
            .Run())
            .ThrowOnError();
    }
};

class TParquetChunkMetaGenerator
    : public TArrowChunkMetaGeneratorBase
{
public:
    TParquetChunkMetaGenerator(std::shared_ptr<arrow20::io::RandomAccessFile> chunkFile)
        : TArrowChunkMetaGeneratorBase(
            EChunkFormat::TableUnversionedArrowParquet,
            std::move(chunkFile))
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
    const std::shared_ptr<arrow20::io::RandomAccessFile>& chunkFile)
{
    switch (chunkFormat) {
        case EChunkFormat::TableUnversionedArrowJsonLines:
            return New<TJsonChunkMetaGenerator>(chunkFile);
        case EChunkFormat::TableUnversionedArrowCsv:
            return New<TCsvChunkMetaGenerator>(chunkFile);
        case EChunkFormat::TableUnversionedArrowParquet:
            return New<TParquetChunkMetaGenerator>(chunkFile);
        default:
            THROW_ERROR_EXCEPTION("Unsupported chunk format %Qlv for arrow chunk meta generation", chunkFormat);
    }
}

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient