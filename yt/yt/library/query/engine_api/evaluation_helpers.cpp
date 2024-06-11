#include "evaluation_helpers.h"

#include <yt/yt/library/query/base/private.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h>

namespace NYT::NQueryClient {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NWebAssembly;

static constexpr auto& Logger = QueryClientLogger;

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
                prefixKeys.push_back(permanentBuffer->CaptureRow(
                    MakeRange(key.Begin(), foreignKeyPrefix),
                    /*captureValues*/ false));
            }
            prefixKeys.erase(std::unique(prefixKeys.begin(), prefixKeys.end()), prefixKeys.end());
            dataSource.Keys = MakeSharedRange(std::move(prefixKeys), std::move(permanentBuffer));
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
