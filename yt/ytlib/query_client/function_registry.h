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

class TFunctionDescriptor
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
