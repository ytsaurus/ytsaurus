#pragma once

#include "cg_fragment_compiler.h"

#include "key_trie.h"
#include "plan_fragment.h"

#include <core/codegen/module.h>

#include <core/misc/variant.h>
#include <core/misc/ref_counted.h>

#include <util/generic/stroka.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct IFunctionDescriptor
    : public TRefCounted
{
    virtual ~IFunctionDescriptor();

    virtual Stroka GetName() const = 0;

    virtual EValueType InferResultType(
        const std::vector<EValueType>& argumentTypes,
        const TStringBuf& source) const = 0;

    virtual TCodegenExpression MakeCodegenExpr(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name) const = 0;

    virtual TKeyTrieNode ExtractKeyRange(
        const TIntrusivePtr<const TFunctionExpression>& expr,
        const TKeyColumns& keyColumns,
        TRowBuffer* rowBuffer) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IFunctionDescriptor)

typedef int TTypeArgument;
typedef std::vector<EValueType> TUnionType;
typedef TVariant<EValueType, TTypeArgument, TUnionType> TType;

class TTypedFunction
    : public virtual IFunctionDescriptor
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

    virtual Stroka GetName() const override;

    virtual EValueType InferResultType(
        const std::vector<EValueType>& argumentTypes,
        const TStringBuf& source) const override;

private:
    EValueType TypingFunction(
        const std::vector<TType>& expectedArgTypes,
        TType repeatedArgType,
        TType resultType,
        Stroka functionName,
        const std::vector<EValueType>& argTypes,
        const TStringBuf& source) const;

    Stroka FunctionName_;
    std::vector<TType> ArgumentTypes_;
    TType RepeatedArgumentType_;
    TType ResultType_;
};

class TCodegenFunction
    : public virtual IFunctionDescriptor
{
    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row) const = 0;

    virtual TCodegenExpression MakeCodegenExpr(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name) const override;
};

class TUniversalRangeFunction
    : public virtual IFunctionDescriptor
{
    virtual TKeyTrieNode ExtractKeyRange(
        const TIntrusivePtr<const TFunctionExpression>& expr,
        const TKeyColumns& keyColumns,
        TRowBuffer* rowBuffer) const override;
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
        Value* row) const override;
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
        Value* row) const override;

    virtual TKeyTrieNode ExtractKeyRange(
        const TIntrusivePtr<const TFunctionExpression>& expr,
        const TKeyColumns& keyColumns,
        TRowBuffer* rowBuffer) const override;
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
        Value* row) const override;
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
        Value* row) const override;
};

class THashFunction
    : public TTypedFunction
    , public TCodegenFunction
    , public TUniversalRangeFunction
{
public:
    THashFunction(Stroka routineName, Stroka functionName);

    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name,
        TCGContext& builder,
        Value* row) const override;

private:
    static const TUnionType HashTypes_;

    const Stroka RoutineName_;
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
        Value* row) const override;
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
        Value* row) const override;

private:
    static const TUnionType CastTypes_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
