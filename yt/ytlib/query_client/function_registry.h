#pragma once

#include "key_trie.h"
#include "cg_fragment_compiler.h"
#include "ast.h"

#include <util/generic/stroka.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef int TTypeArgument;
typedef TVariant<EValueType, TTypeArgument> TGenericType;

using TLLVMCodegenExpression = std::function<Value*(TCGContext& builder, Value* row)>;
using TCodegenFunctionExpression = std::function<TLLVMCodegenExpression(std::vector<TCodegenExpression>, Twine nameTwine)>;
using TGenericCodegenFunctionExpression = std::function<TCodegenFunctionExpression(std::vector<EValueType>)>;
using TConstFunctionExpressionPtr = TIntrusivePtr<const NAst::TFunctionExpression>;
using TRangeBuilder = std::function<TKeyTrieNode(const TConstFunctionExpressionPtr&, const TKeyColumns&, TRowBuffer*)>;
using TGenericRangeBuilder = std::function<TRangeBuilder(std::vector<EValueType>)>;

class TFunctionRegistry
{
public:
    void RegisterFunction(
        const Stroka& functionName,
        EValueType resultType,
        std::vector<EValueType> argumentTypes,
        TCodegenFunctionExpression bodyBuilder,
        TRangeBuilder rangeBuilder = UniversalRange);

    void RegisterGenericFunction(
        const Stroka& functionName,
        TGenericType resultType,
        std::vector<TGenericType> argumentTypes,
        TGenericCodegenFunctionExpression bodyBuilder,
        TGenericRangeBuilder rangeBuilder = GenericUniversalRange);


    bool IsRegistered(const Stroka& functionName);

    TGenericType GetGenericResultType(
        const Stroka& functionName);

    std::vector<TGenericType> GetGenericArgumentTypes(
        const Stroka& functionName);

    TCodegenExpression GetCodegenExpression(
        const Stroka& functionName,
        std::vector<TCodegenExpression> codegenArgs, 
        const Stroka& name,
        std::vector<EValueType> argumentTypes = std::vector<EValueType>());

    TRangeBuilder ExtractRangeConstraints(
        const Stroka& functionName,
        std::vector<EValueType> argumentTypes = std::vector<EValueType>());


private:
    static TKeyTrieNode UniversalRange(
        const TConstFunctionExpressionPtr& expr,
        const TKeyColumns& keyColumns,
        TRowBuffer* rowBuffer)
    {
        return TKeyTrieNode::Universal();
    }

    static TRangeBuilder GenericUniversalRange(std::vector<EValueType> argumentTypes)
    {
        return UniversalRange;
    }

//TODO: choose between overloads?
};

TFunctionRegistry* registry = new TFunctionRegistry();

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
