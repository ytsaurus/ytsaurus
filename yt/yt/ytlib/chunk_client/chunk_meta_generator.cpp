#include "chunk_meta_generator.h"

#include <parquet/arrow/reader.h>
#include <yt/yt/client/arrow/schema.h>
#include <yt/yt/library/erasure/public.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/table.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/json/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/csv/api.h>

#include <contrib/libs/apache/arrow/cpp/src/parquet/api/reader.h>

namespace NYT::NChunkClient {

using namespace NTableClient;

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
    virtual NTableClient::TTableSchemaPtr GetChunkSchema() const = 0;
    virtual NTableClient::TNameTablePtr GetChunkNameTable() const = 0;
    virtual EChunkFormat GetChunkFormat() const = 0;

    virtual i64 GetDataWeight() const = 0;

    virtual i64 GetMaxDataBlockSize() const = 0;
    
protected:
    virtual void Prepare()
    { }

    virtual void FillGeneralChunkMeta(TRefCountedChunkMetaPtr& chunkMeta)
    {
        chunkMeta->set_type(ToProto(EChunkType::Table));
        chunkMeta->set_format(ToProto(GetChunkFormat()));
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
        ext.set_uncompressed_data_size(GetUncompressedSize());
        ext.set_compressed_data_size(GetUncompressedSize());
        // TODO(achulkov2): Find a better approximation?
        ext.set_data_weight(GetDataWeight());
        ext.set_meta_size(metaSize);
        ext.set_row_count(GetRowCount());
        ext.set_compression_codec(ToProto(NCompression::ECodec::None));
        ext.set_sorted(false);
        // value_count
        ext.set_max_data_block_size(GetMaxDataBlockSize());
        ext.set_sealed(false);
        ext.set_erasure_codec(ToProto(NErasure::ECodec::None));
        ext.set_system_block_count(0);
        ext.set_striped_erasure(false);
        // block_format_version?
    }
};

class TArrowChunkMetaGeneratorBase
    : public TChunkMetaGeneratorBase
{
public:
    TArrowChunkMetaGeneratorBase(
        EChunkFormat chunkFormat,
        std::shared_ptr<arrow::io::RandomAccessFile> chunkFile)
        : ChunkFormat_(chunkFormat)
        , ChunkFile_(std::move(chunkFile))
    { }

protected:
    virtual std::shared_ptr<arrow::Schema> GetArrowSchema() const = 0;

protected:
    const EChunkFormat ChunkFormat_;
    const std::shared_ptr<arrow::io::RandomAccessFile> ChunkFile_;

    EChunkFormat GetChunkFormat() const override
    {
        return ChunkFormat_;
    }

    i64 GetUnderlyingFileSize() const
    {
        PARQUET_ASSIGN_OR_THROW(auto chunkFileSize, ChunkFile_->GetSize());
        return chunkFileSize;
    }

    i64 GetUncompressedSize() const override
    {
        return GetUnderlyingFileSize();
    }

    // TODO(achulkov2): Think of some better approximation.
    i64 GetDataWeight() const override
    {
        return GetUnderlyingFileSize();
    }

    TTableSchemaPtr GetChunkSchema() const override
    {
        return NArrow::CreateYTTableSchemaFromArrowSchema(GetArrowSchema());
    }

    TNameTablePtr GetChunkNameTable() const override
    {
        return NTableClient::TNameTable::FromSchema(*GetChunkSchema());
    }
};

// TODO(achulkov2): Separate arrow table and single block bases.

class TSingleBlockArrowTableChunkMetaGeneratorBase
    : public TArrowChunkMetaGeneratorBase
{
public:
    using TArrowChunkMetaGeneratorBase::TArrowChunkMetaGeneratorBase;

protected:
    virtual std::shared_ptr<arrow::Table> GetArrowTable() const = 0;

protected:
    i64 GetRowCount() const override
    {
        return GetArrowTable()->num_rows();
    }

    // For single block tables, the maximum data block size is the size of the whole file.
    i64 GetMaxDataBlockSize() const override
    {
        return GetUnderlyingFileSize();
    }

    std::shared_ptr<arrow::Schema> GetArrowSchema() const override
    {
        return GetArrowTable()->schema();
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
        dataBlockMeta->set_uncompressed_size(GetUncompressedSize());
        dataBlockMeta->set_block_index(0);
    }
};

class TFileChunkMetaGenerator
    : public IChunkMetaGenerator
{
public:
    TFileChunkMetaGenerator(std::shared_ptr<arrow::io::RandomAccessFile> chunkFile)
        : ChunkFile_(chunkFile)
    { }

    void Generate() override
    {
        PARQUET_ASSIGN_OR_THROW(auto ChunkFileSize_, ChunkFile_->GetSize());

        auto meta = New<TRefCountedChunkMeta>();

        meta->set_type(ToProto(EChunkType::File));
        meta->set_format(ToProto(EChunkFormat::FileDefault));

        i64 maxDataBlockSize = 4 << 20;
        NProto::TBlocksExt blocksExt;
        while (auto remaining = ChunkFileSize_) {
            auto size = remaining < maxDataBlockSize ? remaining : maxDataBlockSize;
            remaining -= size;
            auto* block = blocksExt.add_blocks();
            block->set_size(size);
        }

        SetProtoExtension(meta->mutable_extensions(), blocksExt);
        NProto::TMiscExt miscExt;
        miscExt.set_uncompressed_data_size(ChunkFileSize_);
        miscExt.set_compressed_data_size(ChunkFileSize_);
        miscExt.set_max_data_block_size(maxDataBlockSize);
        miscExt.set_meta_size(meta->ByteSize());
        SetProtoExtension(meta->mutable_extensions(), miscExt);

        ChunkMeta_ = meta;
    }

    i64 GetUncompressedSize() const override
    {
        return ChunkFileSize_;
    }

    TRefCountedChunkMetaPtr GetChunkMeta() const override
    {
        return ChunkMeta_;
    }

protected:
    TRefCountedChunkMetaPtr ChunkMeta_;

private:
    std::shared_ptr<arrow::io::RandomAccessFile> ChunkFile_;
    i64 ChunkFileSize_ = -1;
};

class TJsonChunkMetaGenerator
    : public TSingleBlockArrowTableChunkMetaGeneratorBase
{
public:
    TJsonChunkMetaGenerator(std::shared_ptr<arrow::io::RandomAccessFile> chunkFile)
        : TSingleBlockArrowTableChunkMetaGeneratorBase(
            EChunkFormat::TableUnversionedArrowJson,
            std::move(chunkFile))
    { }

private:
    std::shared_ptr<arrow::Table> ArrowTable_;

    void Prepare() override
    {
        PARQUET_ASSIGN_OR_THROW(auto jsonReader, arrow::json::TableReader::Make(
            arrow::default_memory_pool(),
            ChunkFile_,
            arrow::json::ReadOptions::Defaults(),
            arrow::json::ParseOptions::Defaults()));
        
        PARQUET_ASSIGN_OR_THROW(ArrowTable_, jsonReader->Read());
    }

    std::shared_ptr<arrow::Table> GetArrowTable() const override
    {
        return ArrowTable_;
    }
};

class TCsvChunkMetaGenerator
    : public TSingleBlockArrowTableChunkMetaGeneratorBase
{
public:
    TCsvChunkMetaGenerator(std::shared_ptr<arrow::io::RandomAccessFile> chunkFile)
        : TSingleBlockArrowTableChunkMetaGeneratorBase(
            EChunkFormat::TableUnversionedArrowCsv,
            std::move(chunkFile))
    { }

private:
    std::shared_ptr<arrow::Table> ArrowTable_;

    void Prepare() override
    {
        PARQUET_ASSIGN_OR_THROW(
            auto csvReader,
            arrow::csv::TableReader::Make(
                arrow::io::IOContext(arrow::default_memory_pool()),
                ChunkFile_,
                arrow::csv::ReadOptions::Defaults(),
                arrow::csv::ParseOptions::Defaults(),
                arrow::csv::ConvertOptions::Defaults()));

        PARQUET_ASSIGN_OR_THROW(ArrowTable_, csvReader->Read());
    }

    std::shared_ptr<arrow::Table> GetArrowTable() const override
    {
        return ArrowTable_;
    }
};

class TParquetChunkMetaGenerator
    : public TArrowChunkMetaGeneratorBase
{
public:
    TParquetChunkMetaGenerator(std::shared_ptr<arrow::io::RandomAccessFile> chunkFile)
        : TArrowChunkMetaGeneratorBase(
            EChunkFormat::TableUnversionedArrowParquet,
            std::move(chunkFile))
    { }

private:
    std::unique_ptr<parquet::arrow::FileReader> ArrowParquetFileReader_;
    std::shared_ptr<parquet::FileMetaData> ParquetFileMeta_;
    std::shared_ptr<arrow::Schema> ArrowSchema_;
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
        PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(ChunkFile_, arrow::default_memory_pool(), &ArrowParquetFileReader_));
        ParquetFileMeta_ = ArrowParquetFileReader_->parquet_reader()->metadata();
        PARQUET_THROW_NOT_OK(ArrowParquetFileReader_->GetSchema(&ArrowSchema_));

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

    std::shared_ptr<arrow::Schema> GetArrowSchema() const override
    {
        return ArrowSchema_;
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
};

////////////////////////////////////////////////////////////////////////////

IChunkMetaGeneratorPtr CreateArrowChunkMetaGenerator(
    EChunkFormat chunkFormat,
    const std::shared_ptr<arrow::io::RandomAccessFile>& chunkFile)
{
    switch (chunkFormat) {
        case EChunkFormat::FileDefault:
            return New<TFileChunkMetaGenerator>(chunkFile);
        case EChunkFormat::TableUnversionedArrowJson:
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