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


class FunctionDescriptor
{
public:
    virtual Stroka GetName() = 0;

    virtual EValueType InferResultType(
        const std::vector<EValueType>& argumentTypes,
        const TStringBuf& source) = 0;

    //TODO: code generation
    //TODO: range inference
};

class TFunctionRegistry
{
public:
    TFunctionRegistry();

    void RegisterFunction(std::unique_ptr<FunctionDescriptor> descriptor);
    FunctionDescriptor& GetFunction(const Stroka& functionName);

    bool IsRegistered(const Stroka& functionName);

    static TKeyTrieNode UniversalRange(
        const TConstFunctionExpressionPtr& expr,
        const TKeyColumns& keyColumns,
        TRowBuffer* rowBuffer)
    {
        return TKeyTrieNode::Universal();
    }

    std::unordered_map<Stroka, std::unique_ptr<FunctionDescriptor>> registeredFunctions;

//TODO: choose between overloads?
};

TFunctionRegistry* GetFunctionRegistry();

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
