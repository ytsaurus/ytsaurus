#include "evaluation_helpers.h"

#include <yt/yt/library/query/base/helpers.h>
#include <yt/yt/library/query/base/private.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h>

namespace NYT::NQueryClient {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NWebAssembly;

static constexpr auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

TGroupHasher::TGroupHasher(NWebAssembly::TCompartmentFunction<THasherFunction> hasher)
    : Hasher_(hasher)
{ }

ui64 TGroupHasher::operator () (const TPIValue* row) const
{
    return Hasher_(row);
}

const TPIValue* TRowComparer::MakeSentinel(ESentinelType type)
{
    return std::bit_cast<TPIValue*>(type);
}

bool TRowComparer::IsSentinel(const TPIValue* value)
{
    return value == MakeSentinel(ESentinelType::Empty) ||
        value == MakeSentinel(ESentinelType::Deleted);
}

TRowComparer::TRowComparer(NWebAssembly::TCompartmentFunction<TComparerFunction> comparer)
    : Comparer_(comparer)
{ }

bool TRowComparer::operator () (const TPIValue* lhs, const TPIValue* rhs) const
{
    return (lhs == rhs) ||
        (!IsSentinel(lhs) && !IsSentinel(rhs) && Comparer_(lhs, rhs));
}

} // namespace NDetail

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
        if (escapeCharacter.size() > 1) {
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
    TExecutionContext* context) const
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
    const TRowBufferPtr& buffer) const
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

void TCGAggregateInstance::RunInit(const TRowBufferPtr& buffer, TValue* state) const
{
    Callbacks_.Init(buffer, state, Compartment_.get());
}

void TCGAggregateInstance::RunUpdate(const TRowBufferPtr& buffer, TValue* state, TRange<TValue> arguments) const
{
    Callbacks_.Update(buffer, state, arguments, Compartment_.get());
}

void TCGAggregateInstance::RunMerge(const TRowBufferPtr& buffer, TValue* firstState, const TValue* secondState) const
{
    Callbacks_.Merge(buffer, firstState, secondState, Compartment_.get());
}

void TCGAggregateInstance::RunFinalize(const TRowBufferPtr& buffer, TValue* firstState, const TValue* secondState) const
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

bool AllComputedColumnsEvaluated(const TJoinClause& joinClause)
{
    auto isColumnInEquations = [&] (TStringBuf column) {
        for (const auto& equation : joinClause.ForeignEquations) {
            if (const auto* reference = equation->As<TReferenceExpression>();
                reference && column == reference->ColumnName)
            {
                return true;
            }
        }

        return false;
    };

    auto renamedSchema = joinClause.Schema.GetRenamedSchema();

    for (const auto& column : renamedSchema->Columns()) {
        if (!column.SortOrder()) {
            break;
        }
        if (!column.Expression()) {
            continue;
        }
        if (!isColumnInEquations(column.Name())) {
            return false;
        }
    }

    return true;
}

std::pair<TQueryPtr, TDataSource> GetForeignQuery(
    std::vector<TRow> keys,
    TRowBufferPtr buffer,
    const TJoinClause& joinClause)
{
    TDataSource dataSource;
    dataSource.ObjectId = joinClause.ForeignObjectId;
    dataSource.CellId = joinClause.ForeignCellId;

    const auto& foreignEquations = joinClause.ForeignEquations;
    auto foreignKeyPrefix = joinClause.ForeignKeyPrefix;
    auto newQuery = joinClause.GetJoinSubquery();

    auto predicateRefines = false;

    if (joinClause.Predicate) {
        auto keyColumns = joinClause.Schema.GetKeyColumns();

        auto dummyInClause = New<TInExpression>(
            foreignEquations,
            nullptr);

        auto dummyWhereClause = MakeAndExpression(std::move(dummyInClause), joinClause.Predicate);

        auto signature = GetExpressionConstraintSignature(std::move(dummyWhereClause), keyColumns);

        auto score = GetConstraintSignatureScore(signature);

        YT_LOG_DEBUG("Calculated score for join via IN with predicate (Signature: %v, Score: %v)",
            signature,
            score);

        predicateRefines = score > static_cast<int>(2 * foreignKeyPrefix);
    }

    if (foreignKeyPrefix == 0 || predicateRefines) {
        YT_LOG_DEBUG("Using join via IN clause");

        TRowRanges universalRange{{
            buffer->CaptureRow(NTableClient::MinKey().Get()),
            buffer->CaptureRow(NTableClient::MaxKey().Get()),
        }};

        dataSource.Ranges = MakeSharedRange(std::move(universalRange), buffer);

        auto inClause = New<TInExpression>(
            foreignEquations,
            MakeSharedRange(std::move(keys), std::move(buffer)));

        newQuery->WhereClause = newQuery->WhereClause
            ? MakeAndExpression(inClause, newQuery->WhereClause)
            : inClause;

        if (joinClause.Schema.Original->HasComputedColumns() &&
            AllComputedColumnsEvaluated(joinClause))
        {
            newQuery->ForceLightRangeInference = true;
        }

        if (foreignKeyPrefix > 0) {
            // COMPAT(lukyan): Use ordered read without modification of protocol
            newQuery->Limit = OrderedReadWithPrefetchHint;
        }
    } else {
        if (foreignKeyPrefix == foreignEquations.size()) {
            YT_LOG_DEBUG("Using join via source ranges");
            dataSource.Keys = MakeSharedRange(std::move(keys), std::move(buffer));
        } else {
            YT_LOG_DEBUG("Using join via prefix ranges");
            std::vector<TRow> prefixKeys;
            prefixKeys.reserve(keys.size());
            for (auto key : keys) {
                prefixKeys.push_back(buffer->CaptureRow(
                    TRange(key.Begin(), foreignKeyPrefix),
                    /*captureValues*/ false));
            }
            prefixKeys.erase(std::unique(prefixKeys.begin(), prefixKeys.end()), prefixKeys.end());
            dataSource.Keys = MakeSharedRange(std::move(prefixKeys), std::move(buffer));
        }

        newQuery->InferRanges = false;
        // COMPAT(lukyan): Use ordered read without modification of protocol
        newQuery->Limit = OrderedReadWithPrefetchHint;
    }

    newQuery->GroupClause = joinClause.GroupClause;

    return std::make_pair(std::move(newQuery), std::move(dataSource));
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
    int index = std::ssize(OwningLiteralValues_);
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
