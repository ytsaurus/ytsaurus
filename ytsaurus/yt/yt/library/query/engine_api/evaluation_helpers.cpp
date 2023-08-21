#include "evaluation_helpers.h"

#include "position_independent_value_transfer.h"

#include <yt/yt/library/query/base/private.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h>

#include <yt/yt/client/query_client/query_statistics.h>

namespace NYT::NQueryClient {

using namespace NConcurrency;
using namespace NTableClient;

static const auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

constexpr ssize_t BufferLimit = 512_KB;

struct TTopCollectorBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

TTopCollector::TTopCollector(
    i64 limit,
    TComparerFunction* comparer,
    size_t rowSize,
    IMemoryChunkProviderPtr memoryChunkProvider,
    TLookupRows* lookup)
    : Comparer_(comparer)
    , RowSize_(rowSize)
    , MemoryChunkProvider_(std::move(memoryChunkProvider))
    , Lookup_(lookup)
{
    Rows_.reserve(limit);
}

TTopCollector::TRowAndBuffer TTopCollector::Capture(const TPIValue* row)
{
    if (EmptyBufferIds_.empty()) {
        if (GarbageMemorySize_ > TotalMemorySize_ / 2) {
            // Collect garbage.

            std::vector<std::vector<size_t>> buffersToRows(Buffers_.size());
            for (size_t rowId = 0; rowId < Rows_.size(); ++rowId) {
                buffersToRows[Rows_[rowId].BufferIndex].push_back(rowId);
            }

            auto buffer = New<TRowBuffer>(TTopCollectorBufferTag(), MemoryChunkProvider_);

            TotalMemorySize_ = 0;
            AllocatedMemorySize_ = 0;
            GarbageMemorySize_ = 0;

            for (size_t bufferId = 0; bufferId < buffersToRows.size(); ++bufferId) {
                for (auto rowId : buffersToRows[bufferId]) {
                    auto oldRow = Rows_[rowId].Row;

                    auto savedSize = buffer->GetSize();
                    auto newRow = CapturePIValueRange(buffer.Get(), MakeRange(oldRow, RowSize_)).Begin();
                    Rows_[rowId].Row = newRow;
                    AllocatedMemorySize_ += buffer->GetSize() - savedSize;

                    if (Lookup_) {
                        Lookup_->erase(oldRow);
                        Lookup_->insert(newRow);
                    }
                }

                TotalMemorySize_ += buffer->GetCapacity();

                if (buffer->GetSize() < BufferLimit) {
                    EmptyBufferIds_.push_back(bufferId);
                }

                std::swap(buffer, Buffers_[bufferId]);
                buffer->Clear();
            }
        } else {
            // Allocate buffer and add to emptyBufferIds.
            EmptyBufferIds_.push_back(Buffers_.size());
            Buffers_.push_back(New<TRowBuffer>(TTopCollectorBufferTag(), MemoryChunkProvider_));
        }
    }

    YT_VERIFY(!EmptyBufferIds_.empty());

    auto bufferId = EmptyBufferIds_.back();
    auto buffer = Buffers_[bufferId];

    auto savedSize = buffer->GetSize();
    auto savedCapacity = buffer->GetCapacity();

    TPIValue* capturedRow = CapturePIValueRange(buffer.Get(), MakeRange(row, RowSize_)).Begin();

    AllocatedMemorySize_ += buffer->GetSize() - savedSize;
    TotalMemorySize_ += buffer->GetCapacity() - savedCapacity;

    if (buffer->GetSize() >= BufferLimit) {
        EmptyBufferIds_.pop_back();
    }

    return {capturedRow, bufferId};
}

void TTopCollector::AccountGarbage(const TPIValue* row)
{
    GarbageMemorySize_ += GetUnversionedRowByteSize(RowSize_);
    for (int index = 0; index < static_cast<int>(RowSize_); ++index) {
        const auto& value = row[index];

        if (IsStringLikeType(EValueType(value.Type))) {
            GarbageMemorySize_ += value.Length;
        }
    }
}

const TPIValue* TTopCollector::AddRow(const TPIValue* row)
{
    if (Rows_.size() < Rows_.capacity()) {
        auto capturedRow = Capture(row);

        Rows_.emplace_back(capturedRow);
        std::push_heap(Rows_.begin(), Rows_.end(), Comparer_);
        if (Lookup_) {
            auto inserted = Lookup_->insert(capturedRow.Row);
            YT_ASSERT(inserted.second);
        }

        return capturedRow.Row;
    }

    if (!Rows_.empty() && !Comparer_(Rows_.front().Row, row)) {
        auto capturedRow = Capture(row);

        std::pop_heap(Rows_.begin(), Rows_.end(), Comparer_);
        auto popped = Rows_.back().Row;
        AccountGarbage(popped);
        if (Lookup_) {
            Lookup_->erase(popped);
        }

        Rows_.back() = capturedRow;
        std::push_heap(Rows_.begin(), Rows_.end(), Comparer_);
        if (Lookup_) {
            auto inserted = Lookup_->insert(capturedRow.Row);
            YT_ASSERT(inserted.second);
        }

        return capturedRow.Row;
    }

    return nullptr;
}

std::vector<const TPIValue*> TTopCollector::GetRows() const
{
    std::vector<const TPIValue*> result;
    result.reserve(Rows_.size());
    for (const auto& [value, _] : Rows_) {
        result.push_back(value);
    }
    std::sort(result.begin(), result.end(), Comparer_);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TMultiJoinClosure::TItem::TItem(
    IMemoryChunkProviderPtr chunkProvider,
    size_t keySize,
    TComparerFunction* prefixEqComparer,
    THasherFunction* lookupHasher,
    TComparerFunction* lookupEqComparer)
    : Buffer(New<TRowBuffer>(TPermanentBufferTag(), std::move(chunkProvider)))
    , KeySize(keySize)
    , PrefixEqComparer(prefixEqComparer)
    , Lookup(
        InitialGroupOpHashtableCapacity,
        lookupHasher,
        lookupEqComparer)
{
    Lookup.set_empty_key(nullptr);
}

TGroupByClosure::TGroupByClosure(
    IMemoryChunkProviderPtr chunkProvider,
    TComparerFunction* prefixEqComparer,
    THasherFunction* groupHasher,
    TComparerFunction* groupComparer,
    int keySize,
    int groupStateSize,
    int orderKeySize,
    i64 orderLimit,
    TComparerFunction* orderComparer,
    bool checkNulls)
    : Buffer(New<TRowBuffer>(TPermanentBufferTag(), chunkProvider))
    , PrefixEqComparer(prefixEqComparer)
    , Lookup(
        InitialGroupOpHashtableCapacity,
        groupHasher,
        groupComparer)
    , KeySize(keySize)
    , GroupStateSize(groupStateSize)
    , OrderKeySize(orderKeySize)
    , CheckNulls(checkNulls)
{
    if (orderComparer) {
        TopCollector = std::make_unique<TTopCollector>(
            orderLimit,
            orderComparer,
            KeySize + GroupStateSize + OrderKeySize,
            chunkProvider,
            &Lookup);
    }

    Lookup.set_empty_key(NDetail::TRowComparer::MakeSentinel(
        NDetail::TRowComparer::ESentinelType::Empty));

    Lookup.set_deleted_key(NDetail::TRowComparer::MakeSentinel(
        NDetail::TRowComparer::ESentinelType::Deleted));
}

TWriteOpClosure::TWriteOpClosure(IMemoryChunkProviderPtr chunkProvider)
    : OutputBuffer(New<TRowBuffer>(TOutputBufferTag(), std::move(chunkProvider)))
{ }

////////////////////////////////////////////////////////////////////////////////

std::pair<TQueryPtr, TDataSource> GetForeignQuery(
    TQueryPtr subquery,
    TConstJoinClausePtr joinClause,
    std::vector<TRow> keys,
    TRowBufferPtr permanentBuffer)
{
    auto foreignKeyPrefix = joinClause->ForeignKeyPrefix;
    const auto& foreignEquations = joinClause->ForeignEquations;

    auto newQuery = New<TQuery>(*subquery);

    TDataSource dataSource;
    dataSource.ObjectId = joinClause->ForeignObjectId;
    dataSource.CellId = joinClause->ForeignCellId;

    if (foreignKeyPrefix > 0) {
        if (foreignKeyPrefix == foreignEquations.size()) {
            YT_LOG_DEBUG("Using join via source ranges");
            dataSource.Keys = MakeSharedRange(std::move(keys), std::move(permanentBuffer));
        } else {
            YT_LOG_DEBUG("Using join via prefix ranges");
            std::vector<TRow> prefixKeys;
            for (auto key : keys) {
                prefixKeys.push_back(permanentBuffer->CaptureRow(MakeRange(key.Begin(), foreignKeyPrefix), false));
            }
            prefixKeys.erase(std::unique(prefixKeys.begin(), prefixKeys.end()), prefixKeys.end());
            dataSource.Keys = MakeSharedRange(std::move(prefixKeys), std::move(permanentBuffer));
        }

        for (size_t index = 0; index < foreignKeyPrefix; ++index) {
            dataSource.Schema.push_back(foreignEquations[index]->LogicalType);
        }

        newQuery->InferRanges = false;
        // COMPAT(lukyan): Use ordered read without modification of protocol
        newQuery->Limit = std::numeric_limits<i64>::max() - 1;
    } else {
        TRowRanges ranges;

        YT_LOG_DEBUG("Using join via IN clause");
        ranges.emplace_back(
            permanentBuffer->CaptureRow(NTableClient::MinKey().Get()),
            permanentBuffer->CaptureRow(NTableClient::MaxKey().Get()));

        auto inClause = New<TInExpression>(
            foreignEquations,
            MakeSharedRange(std::move(keys), permanentBuffer));

        dataSource.Ranges = MakeSharedRange(std::move(ranges), std::move(permanentBuffer));

        newQuery->WhereClause = newQuery->WhereClause
            ? MakeAndExpression(inClause, newQuery->WhereClause)
            : inClause;
    }

    return std::make_pair(newQuery, dataSource);
}

////////////////////////////////////////////////////////////////////////////////

TRange<void*> TCGVariables::GetOpaqueData() const
{
    return OpaquePointers_;
}

void TCGVariables::Clear()
{
    OpaquePointers_.clear();
    Holder_.Clear();
    OwningLiteralValues_.clear();
    LiteralValues_.reset();
}

int TCGVariables::AddLiteralValue(TOwningValue value)
{
    YT_ASSERT(!LiteralValues_);
    int index = static_cast<int>(OwningLiteralValues_.size());
    OwningLiteralValues_.emplace_back(std::move(value));
    return index;
}

TRange<TPIValue> TCGVariables::GetLiteralValues() const
{
    InitLiteralValuesIfNeeded(this);
    return {LiteralValues_.get(), OwningLiteralValues_.size()};
}

void TCGVariables::InitLiteralValuesIfNeeded(const TCGVariables* variables)
{
    if (!variables->LiteralValues_) {
        variables->LiteralValues_ = std::make_unique<TPIValue[]>(variables->OwningLiteralValues_.size());
        size_t index = 0;
        for (const auto& value : variables->OwningLiteralValues_) {
            MakePositionIndependentFromUnversioned(&variables->LiteralValues_[index], value);
            ++index;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
