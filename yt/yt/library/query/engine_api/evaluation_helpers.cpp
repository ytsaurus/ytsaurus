#include "evaluation_helpers.h"

#include "position_independent_value_transfer.h"

#include <yt/yt/library/query/base/private.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h>

namespace NYT::NQueryClient {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NWebAssembly;

static const auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

bool IsRe2SpecialCharacter(char character)
{
    return character == '\\' ||
        character == '^' ||
        character == '$' ||
        character == '.' ||
        character == '[' ||
        character == ']' ||
        character == '|' ||
        character == '(' ||
        character == ')' ||
        character == '?' ||
        character == '*' ||
        character == '+' ||
        character == '{' ||
        character == '}';
}

TString ConvertLikePatternToRegex(
    TStringBuf pattern,
    EStringMatchOp matchOp,
    TStringBuf escapeCharacter,
    bool escapeCharacterUsed)
{
    if (matchOp == EStringMatchOp::Regex) {
        if (escapeCharacterUsed) {
            THROW_ERROR_EXCEPTION("Nontrivial ESCAPE value should not be used together with REGEX (RLIKE) operators");
        }
        return TString(pattern);
    }

    TStringBuilder builder;
    if (matchOp == EStringMatchOp::CaseInsensitiveLike) {
        builder.AppendString("(?is)"); // Match case-insensitively and let '.' match '\n'.
    } else if (matchOp == EStringMatchOp::Like) {
        builder.AppendString("(?s)"); // Let '.' match '\n'.
    } else {
        YT_ABORT();
    }

    char escape = '\\';
    if (escapeCharacterUsed) {
        if (escapeCharacter.Size() > 1) {
            THROW_ERROR_EXCEPTION("Escape string must be empty or one character");
        }

        if (!escapeCharacter.empty()) {
            escape = escapeCharacter[0];
        } else {
            escape = '\0';
        }
    }

    builder.AppendString("^");
    auto* it = pattern.begin();
    while (it < pattern.end()) {
        if (*it == escape) {
            if (it + 1 == pattern.end()) {
                THROW_ERROR_EXCEPTION("Incomplete escape sequence at the end of LIKE pattern");
            }

            ++it;

            if (IsRe2SpecialCharacter(*it)) {
                builder.AppendChar('\\');
            }

            builder.AppendChar(*it);
        } else if (IsRe2SpecialCharacter(*it)) {
            builder.AppendChar('\\');
            builder.AppendChar(*it);
        } else if (*it == '%') {
            builder.AppendString(".*");
        } else if (*it == '_') {
            builder.AppendChar('.');
        } else {
            builder.AppendChar(*it);
        }

        ++it;
    }

    builder.AppendChar('$');
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

constexpr ssize_t BufferLimit = 512_KB;

struct TTopCollectorBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

TTopCollector::TTopCollector(
    i64 limit,
    TCompartmentFunction<TComparerFunction> comparer,
    size_t rowSize,
    IMemoryChunkProviderPtr memoryChunkProvider)
    : Comparer_(comparer)
    , RowSize_(rowSize)
    , MemoryChunkProvider_(std::move(memoryChunkProvider))
{
    Rows_.reserve(limit);
}

std::pair<const TPIValue*, int> TTopCollector::Capture(const TPIValue* row)
{
    if (EmptyContextIds_.empty()) {
        if (GarbageMemorySize_ > TotalMemorySize_ / 2) {
            // Collect garbage.

            std::vector<std::vector<size_t>> contextsToRows(Contexts_.size());
            for (size_t rowId = 0; rowId < Rows_.size(); ++rowId) {
                contextsToRows[Rows_[rowId].second].push_back(rowId);
            }

            auto context = MakeExpressionContext(TTopCollectorBufferTag(), MemoryChunkProvider_);

            TotalMemorySize_ = 0;
            AllocatedMemorySize_ = 0;
            GarbageMemorySize_ = 0;

            for (size_t contextId = 0; contextId < contextsToRows.size(); ++contextId) {
                for (auto rowId : contextsToRows[contextId]) {
                    auto* oldRow = Rows_[rowId].first;
                    i64 savedSize = context.GetSize();
                    auto* newRow = CapturePIValueRange(
                        &context,
                        MakeRange(oldRow, RowSize_),
                        NWebAssembly::EAddressSpace::WebAssembly,
                        NWebAssembly::EAddressSpace::WebAssembly,
                        /*captureValues*/ true)
                        .Begin();
                    Rows_[rowId].first = newRow;
                    AllocatedMemorySize_ += context.GetSize() - savedSize;
                }

                TotalMemorySize_ += context.GetCapacity();

                if (context.GetSize() < BufferLimit) {
                    EmptyContextIds_.push_back(contextId);
                }

                std::swap(context, Contexts_[contextId]);
                context.Clear();
            }
        } else {
            // Allocate context and add to emptyContextIds.
            EmptyContextIds_.push_back(Contexts_.size());
            Contexts_.push_back(MakeExpressionContext(TTopCollectorBufferTag(), MemoryChunkProvider_));
        }
    }

    YT_VERIFY(!EmptyContextIds_.empty());

    auto contextId = EmptyContextIds_.back();
    auto& context = Contexts_[contextId];

    auto savedSize = context.GetSize();
    auto savedCapacity = context.GetCapacity();

    TPIValue* capturedRow = CapturePIValueRange(
        &context,
        MakeRange(row, RowSize_),
        NWebAssembly::EAddressSpace::WebAssembly,
        NWebAssembly::EAddressSpace::WebAssembly,
        /*captureValues*/ true)
        .Begin();

    AllocatedMemorySize_ += context.GetSize() - savedSize;
    TotalMemorySize_ += context.GetCapacity() - savedCapacity;

    if (context.GetSize() >= BufferLimit) {
        EmptyContextIds_.pop_back();
    }

    return std::pair(capturedRow, contextId);
}

void TTopCollector::AccountGarbage(const TPIValue* row)
{
    row = ConvertPointerFromWasmToHost(row, RowSize_);
    GarbageMemorySize_ += GetUnversionedRowByteSize(RowSize_);
    for (int index = 0; index < static_cast<int>(RowSize_); ++index) {
        auto& value = row[index];
        if (IsStringLikeType(EValueType(value.Type))) {
            GarbageMemorySize_ += value.Length;
        }
    }
}

void TTopCollector::AddRow(const TPIValue* row)
{
    if (Rows_.size() < Rows_.capacity()) {
        auto capturedRow = Capture(row);
        Rows_.emplace_back(capturedRow);
        std::push_heap(Rows_.begin(), Rows_.end(), Comparer_);
    } else if (!Rows_.empty() && !Comparer_(Rows_.front().first, row)) {
        auto capturedRow = Capture(row);
        std::pop_heap(Rows_.begin(), Rows_.end(), Comparer_);
        AccountGarbage(Rows_.back().first);
        Rows_.back() = capturedRow;
        std::push_heap(Rows_.begin(), Rows_.end(), Comparer_);
    }
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
    TCompartmentFunction<TComparerFunction> prefixEqComparer,
    TCompartmentFunction<THasherFunction> lookupHasher,
    TCompartmentFunction<TComparerFunction> lookupEqComparer)
    : Context(MakeExpressionContext(TPermanentBufferTag(), std::move(chunkProvider)))
    , KeySize(keySize)
    , PrefixEqComparer(prefixEqComparer)
    , Lookup(
        InitialGroupOpHashtableCapacity,
        lookupHasher,
        lookupEqComparer)
{
    Lookup.set_empty_key(nullptr);
}

////////////////////////////////////////////////////////////////////////////////

TWriteOpClosure::TWriteOpClosure(IMemoryChunkProviderPtr chunkProvider)
    : OutputContext(MakeExpressionContext(TOutputBufferTag(), std::move(chunkProvider)))
{ }

////////////////////////////////////////////////////////////////////////////////

TCGQueryInstance::TCGQueryInstance(
    TCGQueryCallback callback,
    std::unique_ptr<IWebAssemblyCompartment> compartment)
    : Callback_(std::move(callback))
    , Compartment_(std::move(compartment))
{ }

void TCGQueryInstance::Run(
    TRange<TPIValue> literalValues,
    TRange<void*> opaqueData,
    TRange<size_t> opaqueDataSizes,
    TExecutionContext* context)
{
    Callback_(literalValues, opaqueData, opaqueDataSizes, context, Compartment_.get());
}

////////////////////////////////////////////////////////////////////////////////

TCGQueryImage::TCGQueryImage(
    TCGQueryCallback callback,
    std::unique_ptr<IWebAssemblyCompartment> compartment)
    : Callback_(std::move(callback))
    , Compartment_(std::move(compartment))
{ }

TCGQueryInstance TCGQueryImage::Instantiate() const
{
    return TCGQueryInstance(
        Callback_,
        Compartment_ ? Compartment_->Clone() : std::unique_ptr<IWebAssemblyCompartment>());
}

////////////////////////////////////////////////////////////////////////////////

TCGExpressionInstance::TCGExpressionInstance(
    TCGExpressionCallback callback,
    std::unique_ptr<IWebAssemblyCompartment> compartment)
    : Callback_(std::move(callback))
    , Compartment_(std::move(compartment))
{ }

void TCGExpressionInstance::Run(
    TRange<TPIValue> literalValues,
    TRange<void*> opaqueData,
    TRange<size_t> opaqueDataSizes,
    TValue* result,
    TRange<TValue> inputRow,
    const TRowBufferPtr& buffer)
{
    Callback_(literalValues, opaqueData, opaqueDataSizes, result, inputRow, buffer, Compartment_.get());
}

TCGExpressionInstance::operator bool() const
{
    return bool(Callback_);
}

////////////////////////////////////////////////////////////////////////////////

TCGExpressionImage::TCGExpressionImage(
    TCGExpressionCallback callback,
    std::unique_ptr<IWebAssemblyCompartment> compartment)
    : Callback_(std::move(callback))
    , Compartment_(std::move(compartment))
{ }

TCGExpressionInstance TCGExpressionImage::Instantiate() const
{
    return TCGExpressionInstance(
        Callback_,
        Compartment_ ? Compartment_->Clone() : std::unique_ptr<IWebAssemblyCompartment>());
}

TCGExpressionImage::operator bool() const
{
    return bool(Callback_);
}

////////////////////////////////////////////////////////////////////////////////

TCGAggregateInstance::TCGAggregateInstance(
    TCGAggregateCallbacks callbacks,
    std::unique_ptr<IWebAssemblyCompartment> compartment)
    : Callbacks_(std::move(callbacks))
    , Compartment_(std::move(compartment))
{ }

void TCGAggregateInstance::RunInit(const TRowBufferPtr& buffer, TValue* state)
{
    Callbacks_.Init(buffer, state, Compartment_.get());
}

void TCGAggregateInstance::RunUpdate(const TRowBufferPtr& buffer, TValue* state, TRange<TValue> arguments)
{
    Callbacks_.Update(buffer, state, arguments, Compartment_.get());
}

void TCGAggregateInstance::RunMerge(const TRowBufferPtr& buffer, TValue* firstState, const TValue* secondState)
{
    Callbacks_.Merge(buffer, firstState, secondState, Compartment_.get());
}

void TCGAggregateInstance::RunFinalize(const TRowBufferPtr& buffer, TValue* firstState, const TValue* secondState)
{
    Callbacks_.Finalize(buffer, firstState, secondState, Compartment_.get());
}

////////////////////////////////////////////////////////////////////////////////

TCGAggregateImage::TCGAggregateImage(
    TCGAggregateCallbacks callbacks,
    std::unique_ptr<IWebAssemblyCompartment> compartment)
    : Callbacks_(std::move(callbacks))
    , Compartment_(std::move(compartment))
{ }

TCGAggregateInstance TCGAggregateImage::Instantiate() const
{
    return TCGAggregateInstance(
        Callbacks_,
        Compartment_ ? Compartment_->Clone() : std::unique_ptr<IWebAssemblyCompartment>());
}

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

    return std::pair(newQuery, dataSource);
}

////////////////////////////////////////////////////////////////////////////////

TRange<void*> TCGVariables::GetOpaqueData() const
{
    return OpaquePointers_;
}

TRange<size_t> TCGVariables::GetOpaqueDataSizes() const
{
    return OpaquePointeeSizes_;
}

void TCGVariables::Clear()
{
    OpaquePointers_.clear();
    OpaquePointeeSizes_.clear();
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
