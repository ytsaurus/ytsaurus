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

using TCodegenBuilder = std::function<TCodegenExpression(std::vector<TCodegenExpression>, EValueType, Stroka)>;

using TConstFunctionExpressionPtr = TIntrusivePtr<const NAst::TFunctionExpression>;
using TRangeBuilder = std::function<TKeyTrieNode(const TConstFunctionExpressionPtr&, const TKeyColumns&, TRowBuffer*)>;

class TFunctionRegistry
{
public:
    void RegisterFunction(
        const Stroka& functionName,
        std::vector<TGenericType> argumentTypes,
        TGenericType resultType,
        TCodegenBuilder bodyBuilder,
        TRangeBuilder rangeBuilder = UniversalRange);


    bool IsRegistered(const Stroka& functionName);

    TGenericType GetGenericResultType(
        const Stroka& functionName);

    std::vector<TGenericType> GetGenericArgumentTypes(
        const Stroka& functionName);

    TCodegenBuilder GetCodegenBuilder(
        const Stroka& functionName,
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

//TODO: choose between overloads?
};

TFunctionRegistry* registry = new TFunctionRegistry();

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
