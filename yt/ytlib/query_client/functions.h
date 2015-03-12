#pragma once

#include "cg_fragment_compiler.h"

#include "key_trie.h"
#include "plan_fragment.h"

#include <core/codegen/module.h>

#include <core/misc/variant.h>

#include <util/generic/stroka.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TFunctionDescriptor
{
public:
    virtual ~TFunctionDescriptor()
    { }

    virtual Stroka GetName() = 0;

    virtual EValueType InferResultType(
        const std::vector<EValueType>& argumentTypes,
        const TStringBuf& source) = 0;

    virtual TCodegenExpression MakeCodegenExpr(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name) = 0;

    virtual TKeyTrieNode ExtractKeyRange(
        const TIntrusivePtr<const TFunctionExpression>& expr,
        const TKeyColumns& keyColumns,
        TRowBuffer* rowBuffer) = 0;
};

typedef int TTypeArgument;
typedef std::vector<EValueType> TUnionType;
typedef TVariant<EValueType, TTypeArgument, TUnionType> TType;

class TTypedFunction
    : public virtual TFunctionDescriptor
{
public:
    TTypedFunction(
        Stroka functionName,
        std::vector<TType> argumentTypes,
        TType repeatedArgumentType,
        TType resultType)
        : FunctionName_(functionName)
        , ArgumentTypes_(argumentTypes)
        , RepeatedArgumentType_(repeatedArgumentType)
        , ResultType_(resultType)
    { }

    TTypedFunction(
        Stroka functionName,
        std::vector<TType> argumentTypes,
        TType resultType)
        : TTypedFunction(
            functionName,
            argumentTypes,
            EValueType::Null,
            resultType)
    { }

    Stroka GetName();

    EValueType InferResultType(
        const std::vector<EValueType>& argumentTypes,
        const TStringBuf& source);

private:
    EValueType TypingFunction(
        const std::vector<TType>& expectedArgTypes,
        TType repeatedArgType,
        TType resultType,
        Stroka functionName,
        const std::vector<EValueType>& argTypes,
        const TStringBuf& source);

    Stroka FunctionName_;
    std::vector<TType> ArgumentTypes_;
    TType RepeatedArgumentType_;
    TType ResultType_;
};

class TCodegenFunction
    : public virtual TFunctionDescriptor
{
    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row) = 0;

    virtual TCodegenExpression MakeCodegenExpr(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name);
};

class TUniversalRangeFunction
    : public virtual TFunctionDescriptor
{
    virtual TKeyTrieNode ExtractKeyRange(
        const TIntrusivePtr<const TFunctionExpression>& expr,
        const TKeyColumns& keyColumns,
        TRowBuffer* rowBuffer);
};

class IfFunction
    : public TTypedFunction
    , public TCodegenFunction
    , public TUniversalRangeFunction
{
public:
    IfFunction() : TTypedFunction(
        "if",
        std::vector<TType>{ EValueType::Boolean, 0, 0 },
        0)
    { }

    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row);
};

class IsPrefixFunction
    : public TTypedFunction
    , public TCodegenFunction
{
public:
    IsPrefixFunction() : TTypedFunction(
        "is_prefix",
        std::vector<TType>{ EValueType::String, EValueType::String },
        EValueType::Boolean)
    { }

    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row);

    virtual TKeyTrieNode ExtractKeyRange(
        const TIntrusivePtr<const TFunctionExpression>& expr,
        const TKeyColumns& keyColumns,
        TRowBuffer* rowBuffer);
};

class IsSubstrFunction
    : public TTypedFunction
    , public TCodegenFunction
    , public TUniversalRangeFunction
{
public:
    IsSubstrFunction() : TTypedFunction(
        "is_substr",
        std::vector<TType>{ EValueType::String, EValueType::String },
        EValueType::Boolean)
    { }

    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row);
};

class LowerFunction
    : public TTypedFunction
    , public TCodegenFunction
    , public TUniversalRangeFunction
{
public:
    LowerFunction() : TTypedFunction(
        "lower",
        std::vector<TType>{ EValueType::String },
        EValueType::String)
    { }

    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row);
};

class SimpleHashFunction
    : public TTypedFunction
    , public TCodegenFunction
    , public TUniversalRangeFunction
{
public:
    SimpleHashFunction() : TTypedFunction(
        "simple_hash",
        std::vector<TType>{ HashTypes_ },
        HashTypes_,
        EValueType::String)
    { }

    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row);

private:
    static const TUnionType HashTypes_;
};

class IsNullFunction
    : public TTypedFunction
    , public TCodegenFunction
    , public TUniversalRangeFunction
{
public:
    IsNullFunction() : TTypedFunction(
        "is_null",
        std::vector<TType>{ 0 },
        EValueType::Boolean)
    { }

    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row);
};

class CastFunction
    : public TTypedFunction
    , public TCodegenFunction
    , public TUniversalRangeFunction
{
public:
    CastFunction(EValueType resultType, Stroka functionName)
        : TTypedFunction(
            functionName,
            std::vector<TType>{ CastTypes_ },
            resultType)
    { }

    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row);

private:
    static const TUnionType CastTypes_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
