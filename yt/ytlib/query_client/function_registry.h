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

class TFunctionDescriptor
{
public:
    virtual Stroka GetName() = 0;

    virtual EValueType InferResultType(
        const std::vector<EValueType>& argumentTypes,
        const TStringBuf& source) = 0;

    virtual TCodegenExpression MakeCodegenExpr(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name) = 0;

    //TODO: range inference
};

class TFunctionRegistry
{
public:
    TFunctionRegistry();

    void RegisterFunction(std::unique_ptr<TFunctionDescriptor> descriptor);
    TFunctionDescriptor& GetFunction(const Stroka& functionName);

    bool IsRegistered(const Stroka& functionName);

private:
    std::unordered_map<Stroka, std::unique_ptr<TFunctionDescriptor>> registeredFunctions;
};

TFunctionRegistry* GetFunctionRegistry();

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
