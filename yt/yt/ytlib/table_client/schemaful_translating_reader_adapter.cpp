#include "schemaful_translating_reader_adapter.h"

#include "chunk_column_mapping.h"

#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/complex_types/positional_yson_translation.h>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TSchemafulTranslatingReaderAdapterPoolTag { };

class TSchemafulTranslatingReaderAdapter
    : public ISchemafulUnversionedReader
{
public:
    TSchemafulTranslatingReaderAdapter(
        ISchemafulUnversionedReaderPtr underlyingReader,
        const TTableSchemaPtr& tableSchema,
        const std::vector<TColumnIdMapping>& schemaIdMapping)
        : UnderlyingReader_(std::move(underlyingReader))
        , TableColumnCount_(tableSchema->GetColumnCount())
        , ColumnTranslators_(std::invoke([&] {
            std::vector<TColumnTranslator> result;
            for (const auto& entry : schemaIdMapping) {
                if (entry.Translator.has_value()) {
                    result.push_back({
                        .TableColumnIndex = entry.ReaderSchemaIndex,
                        .Translator = *entry.Translator,
                    });
                }
            }

            YT_VERIFY(!result.empty());
            std::ranges::sort(result, std::ranges::less{}, &TColumnTranslator::TableColumnIndex);
            return result;
        }))
        , MemoryPool_(TSchemafulTranslatingReaderAdapterPoolTag{})
    { }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        MemoryPool_.Clear();

        auto sourceRowBatch = UnderlyingReader_->Read(options);
        if (!sourceRowBatch) {
            return nullptr;
        }
        auto sourceRows = sourceRowBatch->MaterializeRows();

        std::vector<TUnversionedRow> targetRows;
        targetRows.reserve(sourceRows.Size());
        for (const auto& sourceRow : sourceRows) {
            targetRows.push_back(TranslateRow(sourceRow));
        }

        return CreateBatchFromUnversionedRows(MakeSharedRange(
            std::move(targetRows),
            MakeStrong(this)));
    }

    TFuture<void> GetReadyEvent() const override
    {
        return UnderlyingReader_->GetReadyEvent();
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return UnderlyingReader_->GetDataStatistics();
    }

    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return UnderlyingReader_->GetDecompressionStatistics();
    }

    bool IsFetchingCompleted() const override
    {
        return UnderlyingReader_->IsFetchingCompleted();
    }

    std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return UnderlyingReader_->GetFailedChunkIds();
    }

private:
    using TThis = TSchemafulTranslatingReaderAdapter;

    struct TColumnTranslator
    {
        int TableColumnIndex;
        NComplexTypes::TPositionalYsonTranslator Translator;
    };

    const ISchemafulUnversionedReaderPtr UnderlyingReader_;
    const int TableColumnCount_;
    const std::vector<TColumnTranslator> ColumnTranslators_;

    TChunkedMemoryPool MemoryPool_;

    TUnversionedRow TranslateRow(const TUnversionedRow& sourceRow)
    {
        auto targetRow = TMutableUnversionedRow::Allocate(&MemoryPool_, TableColumnCount_);
        auto* translatorIt = ColumnTranslators_.begin();
        for (auto columnIndex : std::views::iota(0, TableColumnCount_)) {
            const auto& sourceValue = sourceRow[columnIndex];
            auto& targetValue = targetRow[columnIndex];

            while (translatorIt != ColumnTranslators_.end() &&
                translatorIt->TableColumnIndex < columnIndex)
            {
                ++translatorIt;
            }
            if (translatorIt == ColumnTranslators_.end() ||
                translatorIt->TableColumnIndex != columnIndex)
            {
                // No translation is needed.
                targetValue = sourceValue;
                continue;
            }
            targetValue = translatorIt->Translator(sourceValue);

            // Translation is only ever requested for composite values.
            YT_VERIFY(targetValue.Type == EValueType::Composite);

            // Relocate unversioned value data to the memory pool to extend its lifetime.
            auto* buffer = MemoryPool_.AllocateUnaligned(targetValue.Length);
            ::memcpy(buffer, targetValue.Data.String, targetValue.Length);
            targetValue.Data.String = buffer;
        }
        return targetRow;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

ISchemafulUnversionedReaderPtr MaybeWrapWithTranslatingAdapter(
    ISchemafulUnversionedReaderPtr underlyingReader,
    const TTableSchemaPtr& tableSchema,
    const std::vector<TColumnIdMapping>& schemaIdMapping)
{
    bool isTranslationRequired = std::ranges::any_of(
        schemaIdMapping,
        [] (const auto& entry) { return entry.Translator.has_value(); });

    if (isTranslationRequired) {
        return New<TSchemafulTranslatingReaderAdapter>(
            std::move(underlyingReader),
            tableSchema,
            schemaIdMapping);
    }
    return underlyingReader;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
