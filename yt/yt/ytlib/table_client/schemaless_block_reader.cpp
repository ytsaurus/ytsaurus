#include "schemaless_block_reader.h"
#include "helpers.h"
#include "hunks.h"
#include "columnar_chunk_meta.h"

#include <arrow/ipc/writer.h>
#include <arrow/io/memory.h> // Include the header for BufferOutputStream
#include <arrow/table.h>
#include <parquet/arrow/reader.h>
#include <arrow/json/reader.h>
#include <yt/yt/client/table_client/key_bound.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/private.h>

#include <yt/yt/library/formats/arrow_parser.h>
#include <yt/yt/library/numeric/algorithm_helpers.h>

// TODO(achulkov2): Remove unnecessary includes.
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <yt/yt/library/arrow_parquet_adapter/arrow.h>

namespace NYT::NTableClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

std::vector<bool> GetCompositeColumnFlags(const TTableSchemaPtr& schema)
{
    std::vector<bool> compositeColumnFlag;
    const auto& schemaColumns = schema->Columns();
    compositeColumnFlag.assign(schemaColumns.size(), false);
    for (int i = 0; i < std::ssize(schemaColumns); ++i) {
        compositeColumnFlag[i] = IsV3Composite(schemaColumns[i].LogicalType());
    }
    return compositeColumnFlag;
}

std::vector<bool> GetHunkColumnFlags(
    EChunkFormat chunkFormat,
    NChunkClient::EChunkFeatures chunkFeatures,
    const TTableSchemaPtr& schema)
{
    if ((chunkFormat == EChunkFormat::TableUnversionedSchemaful ||
        chunkFormat == EChunkFormat::TableUnversionedColumnar ||
        chunkFormat == EChunkFormat::TableUnversionedSchemalessHorizontal) &&
        None(chunkFeatures & EChunkFeatures::UnversionedHunks))
    {
        return {};
    }

    std::vector<bool> columnHunkFlags;
    if (schema->HasHunkColumns()) {
        auto columnCount = schema->GetColumnCount();
        columnHunkFlags.resize(columnCount);
        for (int i = 0; i < columnCount; ++i) {
            columnHunkFlags[i] = schema->Columns()[i].MaxInlineHunkSize().has_value();
        }
    }
    return columnHunkFlags;
}

////////////////////////////////////////////////////////////////////////////////

THorizontalBlockReader::THorizontalBlockReader(
    const TSharedRef& block,
    const NProto::TDataBlockMeta& meta,
    const std::vector<bool>& compositeColumnFlags,
    const std::vector<bool>& hunkColumnFlags,
    const std::vector<THunkChunkRef>& hunkChunkRefs,
    const std::vector<THunkChunkMeta>& hunkChunkMetas,
    const std::vector<int>& chunkToReaderIdMapping,
    TRange<ESortOrder> sortOrders,
    int commonKeyPrefix,
    const TKeyWideningOptions& keyWideningOptions,
    int extraColumnCount,
    bool decodeInlineHunkValues)
    : Block_(block)
    , Meta_(meta)
    , ChunkToReaderIdMapping_(chunkToReaderIdMapping)
    , CompositeColumnFlags_(compositeColumnFlags)
    , HunkColumnFlags_(hunkColumnFlags)
    , HunkChunkRefs_(hunkChunkRefs)
    , HunkChunkMetas_(hunkChunkMetas)
    , KeyWideningOptions_(keyWideningOptions)
    , SortOrders_(sortOrders.begin(), sortOrders.end())
    , CommonKeyPrefix_(commonKeyPrefix)
    , ExtraColumnCount_(extraColumnCount)
    , DecodeInlineHunkValues_(decodeInlineHunkValues)
{
    YT_VERIFY(GetKeyColumnCount() >= GetChunkKeyColumnCount());
    YT_VERIFY(Meta_.row_count() > 0);

    auto keyDataSize = GetUnversionedRowByteSize(GetKeyColumnCount());
    KeyBuffer_.reserve(keyDataSize);
    Key_ = TMutableUnversionedRow::Create(KeyBuffer_.data(), GetKeyColumnCount());

    // NB: First key of the block will be initialized below during JumpToRowIndex(0) call.
    // Nulls here are used to widen chunk's key to comparator length.
    for (int index = 0; index < GetKeyColumnCount(); ++index) {
        Key_[index] = MakeUnversionedSentinelValue(EValueType::Null, index);
    }

    i64 offsetsLength = sizeof(ui32) * Meta_.row_count();

    Offsets_ = TRef(Block_.Begin(), Block_.Begin() + offsetsLength);
    Data_ = TRef(Offsets_.End(), Block_.End());

    JumpToRowIndex(0);
}

bool THorizontalBlockReader::NextRow()
{
    return JumpToRowIndex(RowIndex_ + 1);
}

bool THorizontalBlockReader::SkipToRowIndex(i64 rowIndex)
{
    YT_VERIFY(rowIndex >= RowIndex_);
    return JumpToRowIndex(rowIndex);
}

bool THorizontalBlockReader::SkipToKeyBound(const TKeyBoundRef& lowerBound)
{
    auto inBound = [&] (TUnversionedRow row) {
        // Key is already widened here.
        return TestKey(ToKeyRef(row), lowerBound, SortOrders_);
    };

    if (inBound(GetLegacyKey())) {
        return true;
    }

    // BinarySearch returns first element such that !pred(it).
    // We are looking for the first row such that Comparator_.TestKey(row, lowerBound);
    auto index = BinarySearch(
        RowIndex_,
        Meta_.row_count(),
        [&] (i64 index) {
            YT_VERIFY(JumpToRowIndex(index));
            return !inBound(GetLegacyKey());
        });

    return JumpToRowIndex(index);
}

bool THorizontalBlockReader::SkipToKey(TUnversionedRow lowerBound)
{
    return SkipToKeyBound(ToKeyBoundRef(lowerBound, /*upper*/ false, GetKeyColumnCount()));
}

// TODO(lukyan): Keep one version of get key.
TLegacyKey THorizontalBlockReader::GetLegacyKey() const
{
    return Key_;
}

TKey THorizontalBlockReader::GetKey() const
{
    return TKey::FromRowUnchecked(Key_, GetKeyColumnCount());
}

bool THorizontalBlockReader::IsHunkValue(TUnversionedValue value)
{
    return IsStringLikeType(value.Type) &&
        !HunkColumnFlags_.empty() &&
        HunkColumnFlags_[value.Id];
}

TUnversionedValue THorizontalBlockReader::DecodeAnyValue(TUnversionedValue value)
{
    if (value.Type != EValueType::Any) {
        return value;
    }

    if (value.Id < CompositeColumnFlags_.size() && CompositeColumnFlags_[value.Id]) {
        value.Type = EValueType::Composite;
        return value;
    }

    if (value.Id < HunkColumnFlags_.size() && HunkColumnFlags_[value.Id]) {
        // Leave this any value to hunk decoding reader.
        return value;
    }

    return TryDecodeUnversionedAnyValue(value);
}

int THorizontalBlockReader::GetChunkKeyColumnCount() const
{
    return CommonKeyPrefix_;
}

int THorizontalBlockReader::GetKeyColumnCount() const
{
    return SortOrders_.Size();
}

TMutableUnversionedRow THorizontalBlockReader::GetRow(TChunkedMemoryPool* memoryPool, bool remapIds)
{
    int totalValueCount = ValueCount_ + std::ssize(KeyWideningOptions_.InsertedColumnIds) + ExtraColumnCount_;
    auto row = TMutableUnversionedRow::Allocate(memoryPool, totalValueCount);
    int valueCount = 0;

    auto pushRegularValue = [&] {
        TUnversionedValue value;
        CurrentPointer_ += ReadRowValue(CurrentPointer_, &value);

        if (IsHunkValue(value)) {
            YT_VERIFY(GetChunkKeyColumnCount() <= value.Id);
            GlobalizeHunkValueAndSetHunkFlag(
                memoryPool,
                HunkChunkRefs_,
                HunkChunkMetas_,
                &value);
            if (DecodeInlineHunkValues_) {
                if (!TryDecodeInlineHunkValue(&value)) {
                    THROW_ERROR_EXCEPTION("Expected an inline hunk value for decoding, got %v",
                        value);
                }
            }
        }

        auto remappedId = remapIds ? ChunkToReaderIdMapping_[value.Id] : value.Id;
        if (remappedId >= 0) {
            value = DecodeAnyValue(value);
            value.Id = remappedId;
            row[valueCount] = value;
            ++valueCount;
        }
    };

    auto pushNullValue = [&] (int id) {
        row[valueCount] = MakeUnversionedNullValue(id);
        ++valueCount;
    };

    if (KeyWideningOptions_.InsertPosition < 0) {
        for (int i = 0; i < static_cast<int>(ValueCount_); ++i) {
            pushRegularValue();
        }
    } else {
        for (int i = 0; i < KeyWideningOptions_.InsertPosition; ++i) {
            pushRegularValue();
        }
        for (int id : KeyWideningOptions_.InsertedColumnIds) {
            pushNullValue(id);
        }
        for (int i = KeyWideningOptions_.InsertPosition; i < static_cast<int>(ValueCount_); ++i) {
            pushRegularValue();
        }
    }

    row.SetCount(valueCount);
    return row;
}

TMutableVersionedRow THorizontalBlockReader::GetVersionedRow(
    TChunkedMemoryPool* memoryPool,
    TTimestamp timestamp)
{
    YT_VERIFY(KeyWideningOptions_.InsertPosition == -1);
    YT_VERIFY(KeyWideningOptions_.InsertedColumnIds.empty());
    YT_VERIFY(ExtraColumnCount_ == 0);

    int valueCount = 0;
    auto currentPointer = CurrentPointer_;
    for (int i = 0; i < static_cast<int>(ValueCount_); ++i) {
        TUnversionedValue value;
        currentPointer += ReadRowValue(currentPointer, &value);

        if (ChunkToReaderIdMapping_[value.Id] >= GetKeyColumnCount()) {
            ++valueCount;
        }
    }

    auto versionedRow = TMutableVersionedRow::Allocate(
        memoryPool,
        GetKeyColumnCount(),
        valueCount,
        1,
        0);

    for (int index = 0; index < GetKeyColumnCount(); ++index) {
        versionedRow.Keys()[index] = MakeUnversionedSentinelValue(EValueType::Null, index);
    }

    auto* currentValue = versionedRow.BeginValues();
    for (int i = 0; i < static_cast<int>(ValueCount_); ++i) {
        TUnversionedValue value;
        CurrentPointer_ += ReadRowValue(CurrentPointer_, &value);

        if (IsHunkValue(value)) {
            YT_VERIFY(GetChunkKeyColumnCount() <= value.Id);
            GlobalizeHunkValueAndSetHunkFlag(
                memoryPool,
                HunkChunkRefs_,
                HunkChunkMetas_,
                &value);
            if (DecodeInlineHunkValues_) {
                if (!TryDecodeInlineHunkValue(&value)) {
                    THROW_ERROR_EXCEPTION("Expected an inline hunk value for decoding, got %v",
                        value);
                }
            }
        }

        int id = ChunkToReaderIdMapping_[value.Id];
        if (id >= GetKeyColumnCount()) {
            value = DecodeAnyValue(value);
            value.Id = id;
            *currentValue = MakeVersionedValue(value, timestamp);
            ++currentValue;
        } else if (id >= 0) {
            versionedRow.Keys()[id] = value;
        }
    }

    versionedRow.WriteTimestamps()[0] = timestamp;

    return versionedRow;
}

i64 THorizontalBlockReader::GetRowIndex() const
{
    return RowIndex_;
}

bool THorizontalBlockReader::JumpToRowIndex(i64 rowIndex)
{
    if (rowIndex >= Meta_.row_count()) {
        return false;
    }

    RowIndex_ = rowIndex;

    ui32 offset = *reinterpret_cast<const ui32*>(Offsets_.Begin() + rowIndex * sizeof(ui32));
    CurrentPointer_ = Data_.Begin() + offset;

    CurrentPointer_ += ReadVarUint32(CurrentPointer_, &ValueCount_);
    YT_VERIFY(static_cast<int>(ValueCount_) >= GetChunkKeyColumnCount());

    const char* ptr = CurrentPointer_;
    for (int i = 0; i < GetChunkKeyColumnCount(); ++i) {
        auto* currentKeyValue = Key_.Begin() + i;
        ptr += ReadRowValue(ptr, currentKeyValue);
        if (currentKeyValue->Type == EValueType::Any
            && currentKeyValue->Id < CompositeColumnFlags_.size()
            && CompositeColumnFlags_[currentKeyValue->Id])
        {
            currentKeyValue->Type = EValueType::Composite;
        }
        YT_VERIFY(None(currentKeyValue->Flags & EValueFlags::Hunk));
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

class TCollectingValueConsumer
    : public NTableClient::IValueConsumer
{
public:
    explicit TCollectingValueConsumer(NTableClient::TTableSchemaPtr schema = New<NTableClient::TTableSchema>())
        : Schema_(std::move(schema))
    { }

    explicit TCollectingValueConsumer(NTableClient::TNameTablePtr nameTable, NTableClient::TTableSchemaPtr schema = New<NTableClient::TTableSchema>())
        : Schema_(std::move(schema))
        , NameTable_(std::move(nameTable))
    { }

    const NTableClient::TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    const NTableClient::TTableSchemaPtr& GetSchema() const override
    {
        return Schema_;
    }

    bool GetAllowUnknownColumns() const override
    {
        return true;
    }

    void OnBeginRow() override
    {
    }

    void OnValue(const NTableClient::TUnversionedValue& value) override
    {
        Builder_.AddValue(value);
    }

    void OnEndRow() override
    {
        RowList_.emplace_back(Builder_.FinishRow());
    }

    NTableClient::TUnversionedRow GetRow(size_t rowIndex)
    {
        return RowList_.at(rowIndex);
    }

    int Size() const
    {
        return std::ssize(RowList_);
    }

    const std::vector<NTableClient::TUnversionedOwningRow>& GetRowList() const
    {
        return RowList_;
    }

private:
    const NTableClient::TTableSchemaPtr Schema_;
    const NTableClient::TNameTablePtr NameTable_ = New<NTableClient::TNameTable>();

    NTableClient::TUnversionedOwningRowBuilder Builder_;
    std::vector<NTableClient::TUnversionedOwningRow> RowList_;
};

////////////////////////////////////////////////////////////////////////////////

void CreateArrowTableForParquet(
    const TSharedRef& block,
    const NProto::TDataBlockMeta& dataBlockMeta,
    const TColumnarChunkMetaPtr& chunkMeta,
    std::shared_ptr<arrow::Table>* table)
{
    YT_VERIFY(chunkMeta->Blocks());
    YT_VERIFY(chunkMeta->ParquetFormatMetaExt());

    auto footer = chunkMeta->ParquetFormatMetaExt()->footer();
    uint32_t footerSize = footer.size();
    
    auto fileMeta = parquet::FileMetaData::Make(
        footer.data(),
        &footerSize);

    auto parquetFileSize = chunkMeta->ParquetFormatMetaExt()->file_size();

    auto blockIndex = dataBlockMeta.block_index();
    auto chunkBlockMeta = chunkMeta->Blocks()->blocks(blockIndex);
    auto rowGroupOffset = chunkBlockMeta.offset();

    // TODO(achulkov2): Introduce constsant for ref to PAR1 string?
    auto rowGroupReader = std::make_shared<NArrow::TCompositeBufferArrowRandomAccessFile>(
        std::vector<NArrow::TCompositeBufferArrowRandomAccessFile::TBufferDescriptor>{
            {TSharedRef::FromString("PAR1"), 0},
            {block, rowGroupOffset},
            {TSharedRef::FromString("PAR1"), parquetFileSize - 4},
        },
        parquetFileSize);

    auto parquetReader = parquet::ParquetFileReader::Open(rowGroupReader, 
        parquet::default_reader_properties(),
        fileMeta);

    std::unique_ptr<parquet::arrow::FileReader> arrowParquetReader;
    PARQUET_THROW_NOT_OK(parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(),
        std::move(parquetReader),
        &arrowParquetReader));

    PARQUET_THROW_NOT_OK(arrowParquetReader->ReadRowGroup(blockIndex, table));
}

void CreateArrowTableForJson(
    const TSharedRef& block,
    const NProto::TDataBlockMeta& /*dataBlockMeta*/,
    const TColumnarChunkMetaPtr& /*chunkMeta*/,
    std::shared_ptr<arrow::Table>* table)
{
    PARQUET_ASSIGN_OR_THROW(auto jsonReader, arrow::json::TableReader::Make(
        arrow::default_memory_pool(),
        std::make_shared<arrow::io::BufferReader>(
            reinterpret_cast<const uint8_t*>(block.Data()),
            block.Size()),
        arrow::json::ReadOptions::Defaults(),
        arrow::json::ParseOptions::Defaults()
    ));
    PARQUET_ASSIGN_OR_THROW(*table, jsonReader->Read());
}

std::shared_ptr<arrow::Table> CreateArrowTable(
    const TSharedRef& block,
    const NProto::TDataBlockMeta& dataBlockMeta,
    const TColumnarChunkMetaPtr& chunkMeta,
    EChunkFormat chunkFormat)
{
    std::shared_ptr<arrow::Table> table;
    switch (chunkFormat) {
        case EChunkFormat::TableUnversionedArrowParquet:
            CreateArrowTableForParquet(block, dataBlockMeta, chunkMeta, &table);
            break;
        case EChunkFormat::TableUnversionedArrowJson:
            CreateArrowTableForJson(block, dataBlockMeta, chunkMeta, &table);
            break;
        default:
            YT_ABORT();
    }

    return table;
}

////////////////////////////////////////////////////////////////////////////////

TArrowHorizontalBlockReader::TArrowHorizontalBlockReader(
    const TSharedRef& block,
    const NProto::TDataBlockMeta& dataBlockMeta,
    const TColumnarChunkMetaPtr& chunkMeta,
    const std::vector<bool>& compositeColumnFlags,
    const std::vector<bool>& hunkColumnFlags,
    const std::vector<THunkChunkRef>& hunkChunkRefs,
    const std::vector<THunkChunkMeta>& hunkChunkMetas,
    const std::vector<int>& chunkToReaderIdMapping,
    TRange<ESortOrder> sortOrders,
    int commonKeyPrefix,
    const TKeyWideningOptions& keyWideningOptions,
    int extraColumnCount,
    bool decodeInlineHunkValues)
    : Block_(block)
    , DataBlockMeta_(dataBlockMeta)
    , ChunkMeta_(chunkMeta)
    , ChunkToReaderIdMapping_(chunkToReaderIdMapping)
    , CompositeColumnFlags_(compositeColumnFlags)
    , HunkColumnFlags_(hunkColumnFlags)
    , HunkChunkRefs_(hunkChunkRefs)
    , HunkChunkMetas_(hunkChunkMetas)
    , KeyWideningOptions_(keyWideningOptions)
    , SortOrders_(sortOrders.begin(), sortOrders.end())
    , CommonKeyPrefix_(commonKeyPrefix)
    , ExtraColumnCount_(extraColumnCount)
    , DecodeInlineHunkValues_(decodeInlineHunkValues)
{
    // TODO(achulkov2): Check that there are no hunk columns in the block. Or in the decoded values!
    // TODO(achulkov2): Throw when reading sorted chunks (for now).

    // TODO(achulkov2): Insert proper format here once they are actually supported.
    std::shared_ptr<arrow::Table> table = CreateArrowTable(
        Block_,
        DataBlockMeta_,
        ChunkMeta_,
        ChunkMeta_->GetChunkFormat());

    arrow::TableBatchReader batchReader(*table);

    TCollectingValueConsumer consumer(ChunkMeta_->ChunkNameTable(), ChunkMeta_->ChunkSchema());

    std::shared_ptr<arrow::RecordBatch> batch;
    while (true) {
        PARQUET_THROW_NOT_OK(batchReader.ReadNext(&batch));

        if (!batch) {
            break;
        }

        PARQUET_THROW_NOT_OK(
            NFormats::DecodeRecordBatch(batch, &consumer));
    }
    
    Rows_ = std::move(consumer.GetRowList());

    KeyBuffer_.reserve(GetUnversionedRowByteSize(GetKeyColumnCount()));
    Key_ = TMutableUnversionedRow::Create(KeyBuffer_.data(), GetKeyColumnCount());

    // NB: First key of the block will be initialized below during JumpToRowIndex(0) call.
    // Nulls here are used to widen chunk's key to comparator length.
    for (int index = 0; index < GetKeyColumnCount(); ++index) {
        Key_[index] = MakeUnversionedNullValue(index);
    }

    JumpToRowIndex(0);
}

bool TArrowHorizontalBlockReader::NextRow()
{
    return JumpToRowIndex(RowIndex_ + 1);
}

bool TArrowHorizontalBlockReader::SkipToRowIndex(i64 rowIndex)
{
    YT_VERIFY(rowIndex >= RowIndex_);
    return JumpToRowIndex(rowIndex);
}

bool TArrowHorizontalBlockReader::SkipToKey(TUnversionedRow lowerBound)
{
    return SkipToKeyBound(ToKeyBoundRef(lowerBound, /*upper*/ false, GetKeyColumnCount()));
}

bool TArrowHorizontalBlockReader::SkipToKeyBound(const TKeyBoundRef& lowerBound)
{
    auto inBound = [&] (TUnversionedRow row) {
        // Key is already widened here.
        return TestKey(ToKeyRef(row), lowerBound, SortOrders_);
    };

    if (inBound(GetLegacyKey())) {
        return true;
    }

    // BinarySearch returns first element such that !pred(it).
    // We are looking for the first row such that Comparator_.TestKey(row, lowerBound);
    auto index = BinarySearch(
        RowIndex_,
        DataBlockMeta_.row_count(),
        [&] (i64 index) {
            YT_VERIFY(JumpToRowIndex(index));
            return !inBound(GetLegacyKey());
        });

    return JumpToRowIndex(index);
}

bool TArrowHorizontalBlockReader::JumpToRowIndex(i64 rowIndex)
{
    if (rowIndex >= std::ssize(Rows_)) {
        return false;
    }

    RowIndex_ = rowIndex;

    std::memcpy(
        Key_.Begin(),
        Rows_[RowIndex_].Begin(),
        sizeof(TUnversionedValue) * GetChunkKeyColumnCount());

    return true;
}

TLegacyKey TArrowHorizontalBlockReader::GetLegacyKey() const
{
    return Key_;
}

TKey TArrowHorizontalBlockReader::GetKey() const
{
    return TKey::FromRowUnchecked(Key_, GetKeyColumnCount());
}

int TArrowHorizontalBlockReader::GetChunkKeyColumnCount() const
{
    return CommonKeyPrefix_;
}

int TArrowHorizontalBlockReader::GetKeyColumnCount() const
{
    return SortOrders_.Size();
}

TMutableUnversionedRow TArrowHorizontalBlockReader::GetRow(TChunkedMemoryPool* memoryPool, bool remapIds)
{
    YT_VERIFY(RowIndex_ >= 0 && RowIndex_ < std::ssize(Rows_));

    auto chunkRow = Rows_[RowIndex_];

    int totalValueCount = chunkRow.GetCount() + std::ssize(KeyWideningOptions_.InsertedColumnIds) + ExtraColumnCount_;
    
    auto row = TMutableUnversionedRow::Allocate(memoryPool, totalValueCount);

    int valueCount = 0;

    auto pushRegularValue = [&] (int chunkValueIndex) {
        auto value = chunkRow[chunkValueIndex];
        auto remappedId = remapIds ? ChunkToReaderIdMapping_[value.Id] : value.Id;
        if (remappedId >= 0) {
            value.Id = remappedId;
            row[valueCount] = value;
            ++valueCount;
        }
    };

    auto pushNullValue = [&] (int id) {
        row[valueCount] = MakeUnversionedNullValue(id);
        ++valueCount;
    };

    if (KeyWideningOptions_.InsertPosition < 0) {
        for (int i = 0; i < chunkRow.GetCount(); ++i) {
            pushRegularValue(i);
        }
    } else {
        for (int i = 0; i < KeyWideningOptions_.InsertPosition; ++i) {
            pushRegularValue(i);
        }
        for (int id : KeyWideningOptions_.InsertedColumnIds) {
            pushNullValue(id);
        }
        for (int i = KeyWideningOptions_.InsertPosition; i < chunkRow.GetCount(); ++i) {
            pushRegularValue(i);
        }
    }

    row.SetCount(valueCount);

    return row;
}

i64 TArrowHorizontalBlockReader::GetRowIndex() const
{
    return RowIndex_;
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
