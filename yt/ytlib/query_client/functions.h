#pragma once

#include "cg_fragment_compiler.h"
#include "key_trie.h"
#include "ast.h"

#include <core/codegen/module.h>

#include <util/generic/stroka.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

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

typedef int TTypeArgument;
typedef std::set<EValueType> TUnionType;
typedef TVariant<EValueType, TTypeArgument, TUnionType> TType;

class TTypedFunction : public virtual TFunctionDescriptor
{
public:
    TTypedFunction(
        Stroka functionName,
        std::vector<TType> argumentTypes,
        TType repeatedArgumentType,
        TType resultType)
        : FunctionName(functionName)
        , ArgumentTypes(argumentTypes)
        , RepeatedArgumentType(repeatedArgumentType)
        , ResultType(resultType)
    {}

    TTypedFunction(
        Stroka functionName,
        std::vector<TType> argumentTypes,
        TType resultType)
        : TTypedFunction(
            functionName,
            argumentTypes,
            EValueType::Null,
            resultType)
    {}

    Stroka GetName()
    {
        return FunctionName;
    }

    EValueType InferResultType(
        const std::vector<EValueType>& argumentTypes,
        const TStringBuf& source)
    {
        return TypingFunction(
                ArgumentTypes,
                RepeatedArgumentType,
                ResultType,
                GetName(),
                argumentTypes,
                source);
    }

private:
    EValueType TypingFunction(
        const std::vector<TType>& expectedArgTypes,
        TType repeatedArgType,
        TType resultType,
        Stroka functionName,
        const std::vector<EValueType>& argTypes,
        const TStringBuf& source);

    Stroka FunctionName;
    std::vector<TType> ArgumentTypes;
    TType RepeatedArgumentType;
    TType ResultType;
};

class TCodegenFunction : public virtual TFunctionDescriptor
{
    virtual TCGValue MakeCodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row) = 0;

    virtual TCodegenExpression MakeCodegenExpr(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name)
    {
        return [
            this,
            codegenArgs,
            type,
            name
        ] (TCGContext& builder, Value* row) {
            return MakeCodegenValue(
                codegenArgs,
                type,
                name,
                builder,
                row);
        };
    }
};

class IfFunction : public TTypedFunction, public TCodegenFunction
{
public:
    IfFunction() : TTypedFunction(
        "if",
        std::vector<TType>({ EValueType::Boolean, 0, 0 }),
        0)
    {}

    virtual TCGValue MakeCodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row);
};

class IsPrefixFunction : public TTypedFunction, public TCodegenFunction
{
public:
    IsPrefixFunction() : TTypedFunction(
        "is_prefix",
        std::vector<TType>({ EValueType::String, EValueType::String }),
        EValueType::Boolean)
    {}

    virtual TCGValue MakeCodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row);
};

class IsSubstrFunction : public TTypedFunction, public TCodegenFunction
{
public:
    IsSubstrFunction() : TTypedFunction(
        "is_substr",
        std::vector<TType>({ EValueType::String, EValueType::String }),
        EValueType::Boolean)
    {}

    virtual TCGValue MakeCodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row);
};

class LowerFunction : public TTypedFunction, public TCodegenFunction
{
public:
    LowerFunction() : TTypedFunction(
        "lower",
        std::vector<TType>({ EValueType::String }),
        EValueType::String)
    {}

    virtual TCGValue MakeCodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row);
};

class SimpleHashFunction : public TTypedFunction, public TCodegenFunction
{
public:
    SimpleHashFunction() : TTypedFunction(
        "simple_hash",
        std::vector<TType>({ HashTypes }),
        HashTypes,
        EValueType::String)
    {}

    virtual TCGValue MakeCodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row);

private:
    static const std::set<EValueType> HashTypes;
};

class IsNullFunction : public TTypedFunction, public TCodegenFunction
{
public:
    IsNullFunction() : TTypedFunction(
        "is_null",
        std::vector<TType>({ 0 }),
        EValueType::Boolean)
    {}

    virtual TCGValue MakeCodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row);
};

class CastFunction : public TTypedFunction, public TCodegenFunction
{
public:
    CastFunction(EValueType resultType, Stroka functionName)
        : TTypedFunction(
            functionName,
            std::vector<TType>({ CastTypes }),
            resultType)
    {}

    virtual TCGValue MakeCodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row);

private:
    static const std::set<EValueType> CastTypes;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
