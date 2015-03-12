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
    virtual ~TFunctionDescriptor();

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
        TType resultType);

    TTypedFunction(
        Stroka functionName,
        std::vector<TType> argumentTypes,
        TType resultType);

    virtual Stroka GetName() override;

    virtual EValueType InferResultType(
        const std::vector<EValueType>& argumentTypes,
        const TStringBuf& source) override;

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
        Stroka name) override;
};

class TUniversalRangeFunction
    : public virtual TFunctionDescriptor
{
    virtual TKeyTrieNode ExtractKeyRange(
        const TIntrusivePtr<const TFunctionExpression>& expr,
        const TKeyColumns& keyColumns,
        TRowBuffer* rowBuffer) override;
};

class TIfFunction
    : public TTypedFunction
    , public TCodegenFunction
    , public TUniversalRangeFunction
{
public:
    TIfFunction();

    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row) override;
};

class TIsPrefixFunction
    : public TTypedFunction
    , public TCodegenFunction
{
public:
    TIsPrefixFunction();

    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row) override;

    virtual TKeyTrieNode ExtractKeyRange(
        const TIntrusivePtr<const TFunctionExpression>& expr,
        const TKeyColumns& keyColumns,
        TRowBuffer* rowBuffer) override;
};

class TIsSubstrFunction
    : public TTypedFunction
    , public TCodegenFunction
    , public TUniversalRangeFunction
{
public:
    TIsSubstrFunction();

    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row) override;
};

class TLowerFunction
    : public TTypedFunction
    , public TCodegenFunction
    , public TUniversalRangeFunction
{
public:
    TLowerFunction();

    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row) override;
};

class TSimpleHashFunction
    : public TTypedFunction
    , public TCodegenFunction
    , public TUniversalRangeFunction
{
public:
    TSimpleHashFunction();

    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row) override;

private:
    static const TUnionType HashTypes_;
};

class TIsNullFunction
    : public TTypedFunction
    , public TCodegenFunction
    , public TUniversalRangeFunction
{
public:
    TIsNullFunction();

    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row) override;
};

class TCastFunction
    : public TTypedFunction
    , public TCodegenFunction
    , public TUniversalRangeFunction
{
public:
    TCastFunction(EValueType resultType, Stroka functionName);

    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row) override;

private:
    static const TUnionType CastTypes_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
