#pragma once

#include <util/generic/stroka.h>
#include <ytlib/query_client/cg_fragment_compiler.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<TCodegenExpression(std::vector<EValueType>)> TGenericCodegenExpression;
typedef TIntrusivePtr<const TFunctionExpression> TConstFunctionExpressionPtr;
typedef std::function<TKeyTrieNode(const TConstFunctionExpressionPtr&, const TKeyColumns&, TRowBuffer*)> TRangeBuilder;
typedef std::function<TRangeBuilder(std::vector<EValueType>)> TGenericRangeBuilder;

class TGenericType
{
    TGenericType(
        EValueType type)
        : IsGeneric(false)
        , Concrete(type)
    {}

    TGenericType(
        int typeArgument)
        : IsGeneric(true)
        , GenericIndex(typeArgument)
    {}

    bool IsGeneric;
    int GenericIndex;
    EValueType Concrete;
};

class TFunctionRegistry
{
public:
    void RegisterFunction(
        const Stroka& name,
        EValueType resultType,
        std::vector<EValueType> argumentTypes,
        TCodegenExpression bodyBuilder,
        TRangeBuilder rangeBuilder = universalRange);

    void RegisterGenericFunction(
        const Stroka& name,
        TGenericType resultType,
        std::vector<TGenericType> argumentTypes,
        TGenericCodegenExpression bodyBuilder,
        TGenericRangeBuilder rangeBuilder = genericUniversalRange);


    EValueType GetResultType(
        const Stroka& name,
        std::vector<EValueType> typeArguments = std::vector<EValueType>());

    std::vector<EValueType> GetArgumentTypes(
        const Stroka& name,
        std::vector<EValueType> typeArguments = std::vector<EValueType>());

    TCodegenExpression GetCodegenExpression(
        const Stroka& name,
        std::vector<EValueType> typeArguments = std::vector<EValueType>());

    TRangeBuilder ExtractRangeConstraints(
        const Stroka& name,
        std::vector<EValueType> typeArguments = std::vector<EValueType>());


private:
    static TKeyTrieNode universalRange(
        const TConstFunctionExpressionPtr& expr,
        const TKeyColumns& keyColumns,
        TRowBuffer* rowBuffer)
    {
        return TKeyTrieNode::Universal();
    }

    static TRangeBuilder genericUniversalRange(std::vector<EValueType> typeArguments)
    {
        return universalRange;
    }

//TODO: choose between overloads?
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
