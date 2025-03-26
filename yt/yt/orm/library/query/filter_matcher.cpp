#include "filter_matcher.h"

#include "expression_evaluator.h"
#include "helpers.h"

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ypath/tokenizer.h>
#include <yt/yt/core/yson/string.h>

#include <yt/yt/library/query/base/query_preparer.h>

#include <util/string/cast.h>

namespace NYT::NOrm::NQuery {

using namespace NTableClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TFilterMatcher
    : public IFilterMatcher
{
public:
    TFilterMatcher(
        std::string filterQuery,
        std::vector<TTypedAttributePath> attributePaths)
        : Evaluator_(CreateOrmExpressionEvaluator(
            std::move(filterQuery),
            std::move(attributePaths)))
    { }

    TErrorOr<bool> Match(
        const std::vector<TNonOwningAttributePayload>& attributePayloads,
        TRowBufferPtr rowBuffer) override
    {
        if (!rowBuffer) {
            rowBuffer = New<TRowBuffer>(TRowBufferTag());
        }
        auto resultOrError = Evaluator_->Evaluate(attributePayloads, rowBuffer);
        if (!resultOrError.IsOK()) {
            return TError("Error matching the filter")
                << resultOrError;
        }

        const auto& resultValue = resultOrError.Value();
        return resultValue.Type == EValueType::Boolean && resultValue.Data.Boolean;
    }

    TErrorOr<bool> Match(const TNonOwningAttributePayload& attributePayload, TRowBufferPtr rowBuffer) override
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
        TRowBufferPtr /*rowBuffer*/) override
    {
        return Constant_;
    }

    TErrorOr<bool> Match(const TNonOwningAttributePayload& /*attributePayload*/, TRowBufferPtr /*rowBuffer*/) override
    {
        return Constant_;
    }

private:
    const bool Constant_;
};

////////////////////////////////////////////////////////////////////////////////

}

IFilterMatcherPtr CreateFilterMatcher(
    std::string filterQuery,
    std::vector<TTypedAttributePath> typedAttributePaths)
{
    return New<TFilterMatcher>(
        std::move(filterQuery),
        std::move(typedAttributePaths));
}

IFilterMatcherPtr CreateFilterMatcher(
    std::string filterQuery,
    std::vector<TYPath> attributePaths)
{
    std::vector<TTypedAttributePath> typedAttributePaths;
    typedAttributePaths.reserve(attributePaths.size());

    for (auto& path : attributePaths) {
        typedAttributePaths.push_back(TTypedAttributePath{
            .Path = std::move(path),
            .TypeResolver = GetTypeResolver(EValueType::Any),
        });
    }
    return New<TFilterMatcher>(
        std::move(filterQuery),
        std::move(typedAttributePaths));
}

////////////////////////////////////////////////////////////////////////////////

IFilterMatcherPtr CreateConstantFilterMatcher(
    bool constant)
{
    return New<TConstantFilterMatcher>(constant);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
