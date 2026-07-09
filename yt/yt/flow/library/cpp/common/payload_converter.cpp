#include "payload_converter.h"

#include "column_evaluator_cache.h"

#include <yt/yt/flow/library/cpp/misc/compact_unversioned_owning_row.h>

#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <library/cpp/containers/insert_only_concurrent_cache/cache.h>
#include <library/cpp/yt/memory/new.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

// Describes what to put into a single target column.
struct TColumnAction
{
    // If >= 0, copy value from source column at this index.
    // If < 0, fill with null (expression column placeholder or missing column).
    int SourceIndex;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TPayloadConverter
    : public IPayloadConverter
{
public:
    TPayloadConverter(
        const TTableSchemaPtr& sourceSchema,
        const TTableSchemaPtr& targetSchema,
        TColumnEvaluatorPtr evaluator)
        : TargetColumnCount_(targetSchema->GetColumnCount())
        , HasComputedColumns_(targetSchema->HasComputedColumns())
        , Evaluator_(std::move(evaluator))
    {
        YT_VERIFY(Evaluator_);

        Actions_.reserve(TargetColumnCount_);

        for (int i = 0; i < TargetColumnCount_; ++i) {
            const auto& column = targetSchema->Columns()[i];
            if (column.Expression()) {
                Actions_.push_back({.SourceIndex = -1});
                YT_VERIFY(!IsStringLikeType(column.GetWireType()));
            } else {
                auto* sourceColumn = sourceSchema->FindColumn(column.Name());
                if (sourceColumn) {
                    THROW_ERROR_EXCEPTION_UNLESS(sourceColumn->GetWireType() == column.GetWireType(),
                        "Column %Qv has inconsistent types in source and target schemas (SourceColumnType: %v,  TargetColumnType: %v)",
                        column.Name(),
                        sourceColumn->GetWireType(),
                        column.GetWireType());

                    int sourceId = sourceSchema->GetColumnIndex(*sourceColumn);
                    Actions_.push_back({.SourceIndex = sourceId});

                    if (IsStringLikeType(sourceColumn->GetWireType())) {
                        StringLikeSourceIndexes_.push_back(sourceId);
                    }
                } else {
                    Actions_.push_back({.SourceIndex = -1});
                }
            }
        }

        // Identity if there are no computed columns and each target column copies the source
        // column at the same index (same shape and order). In that case Convert can return
        // the source row as-is, avoiding allocation and value copy per message.
        IsIdentity_ = !HasComputedColumns_ && std::ssize(Actions_) == sourceSchema->GetColumnCount();
        if (IsIdentity_) {
            for (int i = 0; i < TargetColumnCount_; ++i) {
                if (Actions_[i].SourceIndex != i) {
                    IsIdentity_ = false;
                    break;
                }
            }
        }
    }

    TCompactUnversionedOwningRow Convert(const TCompactUnversionedOwningRow& sourceValues) const override
    {
        if (IsIdentity_) {
            return sourceValues;
        }

        size_t stringDataSize = 0;
        for (int i : StringLikeSourceIndexes_) {
            stringDataSize += sourceValues[i].Length;
        }

        // Helper: fill target row values from source according to Actions_.
        auto fillRow = [&] (TMutableUnversionedRow row) {
            for (int i = 0; i < TargetColumnCount_; ++i) {
                int sourceIndex = Actions_[i].SourceIndex;
                if (sourceIndex >= 0) {
                    row[i] = sourceValues[sourceIndex];
                    row[i].Id = i;
                } else {
                    row[i] = MakeUnversionedNullValue(i);
                }
            }
        };

        if (!HasComputedColumns_) {
            return TCompactUnversionedOwningRow(TargetColumnCount_, stringDataSize, fillRow);
        }

        // EvaluateKeys requires a TRowBufferPtr argument for potential string allocations,
        // but the constructor above verified that all expression columns are non-string-like,
        // so the buffer will never actually be used for allocation.
        // We reuse a thread-local instance to avoid per-call heap allocation.
        // EvaluateKeys doesn't switch fiber context, so thread-local is safe here.
        TForbidContextSwitchGuard contextSwitchGuard;
        static thread_local TRowBufferPtr BufferCache = New<TRowBuffer>();
        auto finally = Finally([&] {
            BufferCache->Clear();
        });

        // All expression columns are inplace (no string data): allocate the owning row
        // once and evaluate keys directly into its storage via the functor constructor.
        return TCompactUnversionedOwningRow(
            TargetColumnCount_,
            stringDataSize,
            [&] (TMutableUnversionedRow row) {
                fillRow(row);
                Evaluator_->EvaluateKeys(row, BufferCache, /*preserveColumnsIds*/ false);
            });
    }

private:
    const int TargetColumnCount_;
    const bool HasComputedColumns_;
    const TColumnEvaluatorPtr Evaluator_;
    std::vector<TColumnAction> Actions_;
    std::vector<int> StringLikeSourceIndexes_;
    bool IsIdentity_ = false;
};

////////////////////////////////////////////////////////////////////////////////

class TPayloadConverterCache
    : public IPayloadConverterCache
{
public:
    explicit TPayloadConverterCache(IColumnEvaluatorCachePtr evaluatorCache)
        : EvaluatorCache_(std::move(evaluatorCache))
    {
        YT_VERIFY(EvaluatorCache_);
    }

    const IPayloadConverterPtr& GetImpl(
        const TTableSchemaPtr& sourceSchema,
        const TTableSchemaPtr& targetSchema)
    {
        auto key = std::pair(sourceSchema.Get(), targetSchema.Get());
        const auto& entry = Cache_->FindOrInsert(key, [&] {
            return TCacheEntry{
                New<TPayloadConverter>(sourceSchema, targetSchema, EvaluatorCache_->Find(targetSchema)),
                sourceSchema,
                targetSchema,
            };
        });
        return entry.Converter;
    }

    IPayloadConverterPtr Get(
        const TTableSchemaPtr& sourceSchema,
        const TTableSchemaPtr& targetSchema) override
    {
        return GetImpl(sourceSchema, targetSchema);
    }

    TCompactUnversionedOwningRow Convert(
        const TCompactUnversionedOwningRow& row,
        const TTableSchemaPtr& sourceSchema,
        const TTableSchemaPtr& targetSchema) override
    {
        return GetImpl(sourceSchema, targetSchema)->Convert(row);
    }

private:
    using TKey = std::pair<const TTableSchema*, const TTableSchema*>;

    // Holds the converter and keeps the schema pointers alive.
    struct TCacheEntry
    {
        IPayloadConverterPtr Converter;
        TTableSchemaPtr SourceSchema;
        TTableSchemaPtr TargetSchema;
    };

    const IColumnEvaluatorCachePtr EvaluatorCache_;

    std::unique_ptr<TInsertOnlyConcurrentCache<TKey, TCacheEntry>> Cache_ =
        std::make_unique<TInsertOnlyConcurrentCache<TKey, TCacheEntry>>();
};

DEFINE_REFCOUNTED_TYPE(TPayloadConverter)
DEFINE_REFCOUNTED_TYPE(TPayloadConverterCache)

////////////////////////////////////////////////////////////////////////////////

IPayloadConverterCachePtr CreatePayloadConverterCache(
    IColumnEvaluatorCachePtr evaluatorCache)
{
    return New<TPayloadConverterCache>(std::move(evaluatorCache));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
