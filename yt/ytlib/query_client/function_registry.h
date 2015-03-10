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

using TTypingFunction = std::function<EValueType(const std::vector<EValueType>&, const TStringBuf&)>;

class TFunctionRegistry
{
public:
    TFunctionRegistry();

    //void RegisterFunction(IFunctionDescriptor descriptor);
    //IFunctionDescriptor GetFunctionDescriptor(const Stroka& functionName);

    void RegisterFunction(
        const Stroka& functionName,
        TTypingFunction typingFunction,
        TCodegenBuilder bodyBuilder,
        TRangeBuilder rangeBuilder = UniversalRange);


    bool IsRegistered(const Stroka& functionName);

    TTypingFunction GetTypingFunction(
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
            TTypingFunction typingFunction,
            TCodegenBuilder bodyBuilder,
            TRangeBuilder rangeBuilder)
            : TypingFunction(typingFunction)
            , BodyBuilder(bodyBuilder)
            , RangeBuilder(rangeBuilder)
        {}

        Stroka FunctionName;
        TTypingFunction TypingFunction;
        TCodegenBuilder BodyBuilder;
        TRangeBuilder RangeBuilder;
    };

    void RegisterFunctionImpl(const Stroka& functionName, const TFunctionMetadata& functionMetadata);
    TFunctionMetadata& LookupMetadata(const Stroka& functionName);

    std::unordered_map<Stroka, TFunctionMetadata> registeredFunctions;

//TODO: choose between overloads?
};

TFunctionRegistry* GetFunctionRegistry();

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
