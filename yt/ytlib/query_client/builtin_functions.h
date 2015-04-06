#include "functions.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef int TTypeArgument;
typedef std::vector<EValueType> TUnionType;
typedef TVariant<EValueType, TTypeArgument, TUnionType> TType;

class TTypedFunction
    : public virtual IFunctionDescriptor
{
public:
    TTypedFunction(
        const Stroka& functionName,
        std::vector<TType> argumentTypes,
        TType repeatedArgumentType,
        TType resultType);

    TTypedFunction(
        const Stroka& functionName,
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
        const Stroka& functionName,
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
        const Stroka& name,
        TCGContext& builder,
        Value* row) const = 0;

    virtual TCodegenExpression MakeCodegenExpr(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        const Stroka& name) const override;
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
        const Stroka& name,
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
        const Stroka& name,
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
        const Stroka& name,
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
        const Stroka& name,
        TCGContext& builder,
        Value* row) const override;
};

class THashFunction
    : public TTypedFunction
    , public TCodegenFunction
    , public TUniversalRangeFunction
{
public:
    THashFunction(
        const Stroka& routineName,
        const Stroka& functionName);

    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        const Stroka& name,
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
        const Stroka& name,
        TCGContext& builder,
        Value* row) const override;
};

class TCastFunction
    : public TTypedFunction
    , public TCodegenFunction
    , public TUniversalRangeFunction
{
public:
    TCastFunction(
        EValueType resultType,
        const Stroka& functionName);

    virtual TCGValue CodegenValue(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        const Stroka& name,
        TCGContext& builder,
        Value* row) const override;

private:
    static const TUnionType CastTypes_;
};

class TUserDefinedFunction
    : public TTypedFunction
    , public TUniversalRangeFunction
{
public:
    TUserDefinedFunction(
        const Stroka& functionName,
        std::vector<EValueType> argumentTypes,
        EValueType resultType,
        TSharedRef implementationFile);

    virtual TCodegenExpression MakeCodegenExpr(
        std::vector<TCodegenExpression> codgenArgs,
        EValueType type,
        const Stroka& name) const override;

private:
    Stroka FunctionName_;
    TSharedRef ImplementationFile_;
    EValueType ResultType_;
    std::vector<EValueType> ArgumentTypes_;

    Function* GetLLVMFunction(TCGContext& builder) const;
    void CheckCallee(Function* callee, TCGContext& builder) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
