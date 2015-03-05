#pragma once

#include "key_trie.h"
#include "cg_fragment_compiler.h"
#include "ast.h"

#include <util/generic/stroka.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef int TTypeArgument;
typedef std::set<EValueType> TUnionType;
typedef TVariant<EValueType, TTypeArgument, TUnionType> TType;

using TCodegenBuilder = std::function<TCodegenExpression(std::vector<TCodegenExpression>, EValueType, Stroka)>;

using TConstFunctionExpressionPtr = TIntrusivePtr<const NAst::TFunctionExpression>;
using TRangeBuilder = std::function<TKeyTrieNode(const TConstFunctionExpressionPtr&, const TKeyColumns&, TRowBuffer*)>;

class TFunctionRegistry
{
public:
    TFunctionRegistry();

    void RegisterFunction(
        const Stroka& functionName,
        std::vector<TType> argumentTypes,
        TType resultType,
        TCodegenBuilder bodyBuilder,
        TRangeBuilder rangeBuilder = UniversalRange);

    void RegisterVariadicFunction(
        const Stroka& functionName,
        std::vector<TType> argumentTypes,
        TType repeatedArgumentType,
        TType resultType,
        TCodegenBuilder bodyBuilder,
        TRangeBuilder rangeBuilder = UniversalRange);


    bool IsRegistered(const Stroka& functionName);

    std::vector<TType> GetArgumentTypes(
        const Stroka& functionName);

    TType GetRepeatedArgumentType(
        const Stroka& functionName);

    TType GetResultType(
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
