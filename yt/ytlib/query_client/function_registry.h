#pragma once

#include "key_trie.h"
#include "ast.h"

#include <util/generic/stroka.h>

#include <core/codegen/module.h>

#include <unordered_map>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef int TTypeArgument;
typedef std::set<EValueType> TUnionType;
typedef TVariant<EValueType, TTypeArgument, TUnionType> TType;

using llvm::Value;
class TCGValue;
class TCGContext;
typedef std::function<TCGValue(TCGContext& builder, Value* row)> TCodegenExpression;
using TCodegenBuilder = std::function<TCodegenExpression(std::vector<TCodegenExpression>, EValueType, Stroka)>;

using TConstFunctionExpressionPtr = TIntrusivePtr<const NAst::TFunctionExpression>;
using TRangeBuilder = std::function<TKeyTrieNode(const TConstFunctionExpressionPtr&, const TKeyColumns&, TRowBuffer*)>;

class TFunctionRegistry
{
public:
    TFunctionRegistry();

    //void RegisterFunction(IFunctionDescriptor descriptor);
    //IFunctionDescriptor GetFunctionDescriptor(const Stroka& functionName);

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


    static TKeyTrieNode UniversalRange(
        const TConstFunctionExpressionPtr& expr,
        const TKeyColumns& keyColumns,
        TRowBuffer* rowBuffer)
    {
        return TKeyTrieNode::Universal();
    }

    class TFunctionMetadata
    {
    public:
       TFunctionMetadata(
            std::vector<TType> argumentTypes,
            TType repeatedArgumentType,
            TType resultType,
            TCodegenBuilder bodyBuilder,
            TRangeBuilder rangeBuilder)
            : ArgumentTypes(argumentTypes)
            , RepeatedArgumentType(repeatedArgumentType)
            , ResultType(resultType)
            , BodyBuilder(bodyBuilder)
            , RangeBuilder(rangeBuilder)
        {}

        Stroka FunctionName;
        std::vector<TType> ArgumentTypes;
        TType RepeatedArgumentType;
        TType ResultType;
        TCodegenBuilder BodyBuilder;
        TRangeBuilder RangeBuilder;
    };

    void RegisterFunctionImpl(const Stroka& functionName, const TFunctionMetadata& functionMetadata);
    TFunctionMetadata& LookupMetadata(const Stroka& functionName);

    std::unordered_map<Stroka, TFunctionMetadata> registeredFunctions;

//TODO: choose between overloads?
};

static TFunctionRegistry* GetFunctionRegistry();

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
