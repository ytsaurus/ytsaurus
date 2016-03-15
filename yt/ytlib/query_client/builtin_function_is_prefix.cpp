#include "builtin_functions.h"
#include "cg_fragment_compiler.h"
#include "helpers.h"
#include "user_defined_functions.h"
#include "udf/is_prefix.h"

namespace NYT {
namespace NQueryClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TIsPrefixFunction
    : public TTypedFunction
    , public TExternallyDefinedFunction
{
public:
    TIsPrefixFunction()
        : TTypedFunction(
            "is_prefix",
            std::unordered_map<TTypeArgument, TUnionType>(),
            std::vector<TType>{EValueType::String, EValueType::String},
            EValueType::Boolean)
        , TExternallyDefinedFunction(
            "is_prefix",
            "is_prefix",
            TSharedRef(is_prefix_bc, is_prefix_bc_len, nullptr),
            GetCallingConvention(ECallingConvention::Simple, 2, EValueType::Null))
    { }

    virtual TKeyTriePtr ExtractKeyRange(
        const TConstFunctionExpressionPtr& expr,
        const TKeyColumns& keyColumns,
        const TRowBufferPtr& rowBuffer) const override;
};

TKeyTriePtr TIsPrefixFunction::ExtractKeyRange(
    const TConstFunctionExpressionPtr& expr,
    const TKeyColumns& keyColumns,
    const TRowBufferPtr& rowBuffer) const
{
    auto result = TKeyTrie::Universal();
    auto lhsExpr = expr->Arguments[0];
    auto rhsExpr = expr->Arguments[1];

    auto referenceExpr = rhsExpr->As<TReferenceExpression>();
    auto constantExpr = lhsExpr->As<TLiteralExpression>();

    if (referenceExpr && constantExpr) {
        int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
        if (keyPartIndex >= 0) {
            auto value = TValue(constantExpr->Value);

            YCHECK(value.Type == EValueType::String);

            result = New<TKeyTrie>(keyPartIndex);
            result->Bounds.emplace_back(value, true);

            ui32 length = value.Length;
            while (length > 0 && value.Data.String[length - 1] == std::numeric_limits<char>::max()) {
                --length;
            }

            if (length > 0) {
                char* newValue = rowBuffer->GetPool()->AllocateUnaligned(length);
                memcpy(newValue, value.Data.String, length);
                ++newValue[length - 1];

                value.Length = length;
                value.Data.String = newValue;
            } else {
                value = MakeSentinelValue<TUnversionedValue>(EValueType::Max);
            }
            result->Bounds.emplace_back(value, false);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

IFunctionDescriptorPtr CreateIsPrefixFunction()
{
    return New<TIsPrefixFunction>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
