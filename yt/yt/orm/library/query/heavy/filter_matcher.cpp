#include "filter_matcher.h"

#include "expression_evaluator.h"

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ypath/tokenizer.h>
#include <yt/yt/core/yson/string.h>

#include <yt/yt/library/query/base/query_preparer.h>

#include <util/string/cast.h>

namespace NYT::NOrm::NQuery {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NTableClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

class TFilterMatcher
    : public IFilterMatcher
{
public:
    explicit TFilterMatcher(IExpressionEvaluatorPtr evaluator)
        : Evaluator_(std::move(evaluator))
    { }

    TErrorOr<bool> Match(
        const std::vector<TNonOwningAttributePayload>& attributePayloads,
        TRowBufferPtr rowBuffer) const override
    {
        if (!rowBuffer) {
            rowBuffer = New<TRowBuffer>(TRowBufferTag());
        }
        auto resultOrError = Evaluator_->Evaluate(attributePayloads, rowBuffer);
        if (!resultOrError.IsOK()) {
            return TError("Error matching the filter")
                .With(resultOrError);
        }

        const auto& resultValue = resultOrError.Value();
        return resultValue.Type == EValueType::Boolean && resultValue.Data.Boolean;
    }

    TErrorOr<bool> Match(const TNonOwningAttributePayload& attributePayload, TRowBufferPtr rowBuffer) const override
    {
        return Match(std::vector<TNonOwningAttributePayload>{attributePayload}, std::move(rowBuffer));
    }

private:
    struct TRowBufferTag
    { };

    const IExpressionEvaluatorPtr Evaluator_;
};

////////////////////////////////////////////////////////////////////////////////

class TConstantFilterMatcher
    : public IFilterMatcher
{
public:
    explicit TConstantFilterMatcher(bool constant)
        : Constant_(constant)
    { }

    TErrorOr<bool> Match(
        const std::vector<TNonOwningAttributePayload>& /*attributePayloads*/,
        TRowBufferPtr /*rowBuffer*/) const override
    {
        return Constant_;
    }

    TErrorOr<bool> Match(
        const TNonOwningAttributePayload& /*attributePayload*/,
        TRowBufferPtr /*rowBuffer*/) const override
    {
        return Constant_;
    }

private:
    const bool Constant_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

IFilterMatcherPtr CreateFilterMatcher(
    std::unique_ptr<NQueryClient::TParsedSource> parsedQuery,
    std::vector<TTypedAttributePath> typedAttributePaths,
    EExpressionEvaluatorMode mode)
{
    return New<TFilterMatcher>(CreateOrmExpressionEvaluator(
        std::move(parsedQuery),
        std::move(typedAttributePaths),
        mode));
}

IFilterMatcherPtr CreateFilterMatcher(
    std::unique_ptr<NQueryClient::TParsedSource> parsedQuery,
    std::vector<TYPath> attributePaths,
    EExpressionEvaluatorMode mode)
{
    return New<TFilterMatcher>(CreateOrmExpressionEvaluator(
        std::move(parsedQuery),
        std::move(attributePaths),
        mode));
}

////////////////////////////////////////////////////////////////////////////////

IFilterMatcherPtr CreateConstantFilterMatcher(bool constant)
{
    return New<TConstantFilterMatcher>(constant);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
