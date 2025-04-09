#pragma once

#include "cg_ir_builder.h"
#include "cg_types.h"

#include <yt/yt/library/codegen/module.h>
#include <yt/yt/library/codegen/llvm_migrate_helpers.h>

#include <yt/yt/client/table_client/llvm_types.h>

namespace NYT::NQueryClient {

// Import extensively used LLVM types.
using llvm::BasicBlock;
using llvm::Constant;
using llvm::ConstantFP;
using llvm::ConstantInt;
using llvm::ConstantPointerNull;
using llvm::Function;
using llvm::FunctionType;
using llvm::Instruction;
using llvm::PHINode;
using llvm::PointerType;
using llvm::Twine;
using llvm::Type;
using llvm::Value;
using llvm::StringRef;

using NCodegen::TCGModulePtr;
using NCodegen::TTypeBuilder;

////////////////////////////////////////////////////////////////////////////////

StringRef ToStringRef(TStringBuf stringBuf);
StringRef ToStringRef(TRef ref);

////////////////////////////////////////////////////////////////////////////////

class TCGIRBuilderPtr
{
public:
    explicit TCGIRBuilderPtr(TCGIRBuilder* builder);

    TCGIRBuilder* operator->() const noexcept;

    TCGIRBuilder* GetBuilder() const noexcept;

protected:
    TCGIRBuilder* const Builder_;
};

////////////////////////////////////////////////////////////////////////////////

using TValueTypeBuilder = TTypeBuilder<TValue>;
using TDataTypeBuilder = TTypeBuilder<TValueData>;

Type* GetABIType(llvm::LLVMContext& context, NYT::NTableClient::EValueType staticType);

Type* GetLLVMType(llvm::LLVMContext& context, NYT::NTableClient::EValueType staticType);

class TCGValue
{
public:
    TCGValue(const TCGValue& other) = default;

    TCGValue(TCGValue&& other) noexcept;

    TCGValue& operator=(TCGValue&& other);

    TCGValue&& Steal();

    void Reset();

    EValueType GetStaticType() const;

    void StoreToValues(const TCGIRBuilderPtr& builder, Value* valuePtr, size_t index, Twine name) const;

    void StoreToValues(const TCGIRBuilderPtr& builder, Value* valuePtr, size_t index) const;

    void StoreToValue(const TCGIRBuilderPtr& builder, Value* valuePtr, Twine name = {}) const;

    Value* IsNull() const;

    Value* GetIsNull(const TCGIRBuilderPtr& builder) const;

    Value* GetLength() const;

    Value* GetData() const;

    Value* GetTypedData(const TCGIRBuilderPtr& builder, bool isAbi = false) const;

    TCGValue Cast(const TCGIRBuilderPtr& builder, EValueType destination) const;

    static TCGValue Create(
        const TCGIRBuilderPtr& builder,
        Value* isNull,
        Value* isAggregate,
        Value* length,
        Value* data,
        EValueType staticType,
        Twine name = {});

    static TCGValue Create(
        const TCGIRBuilderPtr& builder,
        Value* isNull,
        Value* length,
        Value* data,
        EValueType staticType,
        Twine name = {});

    static TCGValue CreateNull(
        const TCGIRBuilderPtr& builder,
        EValueType staticType,
        Twine name = {});

    static TCGValue LoadFromRowValues(
        const TCGIRBuilderPtr& builder,
        Value* rowValues,
        int index,
        bool nullable,
        bool aggregate,
        EValueType staticType,
        Twine name = {});

    static TCGValue LoadFromRowValues(
        const TCGIRBuilderPtr& builder,
        Value* rowValues,
        int index,
        EValueType staticType,
        Twine name = {});

    static TCGValue LoadFromRowValue(
        const TCGIRBuilderPtr& builder,
        Value* valuePtr,
        bool nullable,
        EValueType staticType,
        Twine name = {});

    static TCGValue LoadFromRowValue(
        const TCGIRBuilderPtr& builder,
        Value* valuePtr,
        EValueType staticType,
        Twine name = {});

    static TCGValue LoadFromAggregate(
        const TCGIRBuilderPtr& builder,
        Value* valuePtr,
        EValueType staticType,
        Twine name = {});

private:
    // Can be either boolean value or type value.
    // It is converted lazily from one representation to another.
    Value* IsNull_;
    // Is usually undefined (nullptr).
    // It allows not to emit code for store and load.
    Value* IsAggregate_;
    Value* Length_;
    Value* Data_;
    EValueType StaticType_;
    std::string Name_;

    TCGValue(EValueType staticType, Value* isNull, Value* isAggregate, Value* length, Value* data, Twine name);
};

////////////////////////////////////////////////////////////////////////////////

class TCGBaseContext
    : public TCGIRBuilderPtr
{
public:
    const TCGModulePtr Module;

    TCGBaseContext(const TCGIRBuilderPtr& base, const TCGModulePtr& cgModule);
    TCGBaseContext(const TCGIRBuilderPtr& base, const TCGBaseContext& other);
};

class TCGOpaqueValuesContext
    : public TCGBaseContext
{
public:
    TCGOpaqueValuesContext(const TCGBaseContext& base, Value* literals, Value* opaqueValues);

    TCGOpaqueValuesContext(const TCGBaseContext& base, const TCGOpaqueValuesContext& other);

    Value* GetLiterals() const;

    Value* GetOpaqueValues() const;

    Value* GetOpaqueValue(size_t index) const;

private:
    Value* const Literals_;
    Value* const OpaqueValues_;
};

////////////////////////////////////////////////////////////////////////////////

struct TCodegenFragmentInfo;
struct TCodegenFragmentInfos;

using TClosureTypeBuilder = TTypeBuilder<TExpressionClosure>;

class TCGExprData
{
public:
    const TCodegenFragmentInfos& ExpressionFragments;

    Value* const Buffer;
    Value* const RowValues;
    Value* const ExpressionClosurePtr;

    TCGExprData(
        const TCodegenFragmentInfos& expressionFragments,
        Value* buffer,
        Value* rowValues,
        Value* expressionClosurePtr);
};

////////////////////////////////////////////////////////////////////////////////

class TCGExprContext
    : public TCGOpaqueValuesContext
    , public TCGExprData
{
public:
    TCGExprContext(const TCGOpaqueValuesContext& base, TCGExprData exprData);
    TCGExprContext(const TCGOpaqueValuesContext& base, const TCGExprContext& other);

    Value* GetExpressionClosurePtr();

    Value* GetFragmentResult(size_t index) const;
    Value* GetFragmentFlag(size_t index) const;

    static TCGExprContext Make(
        const TCGBaseContext& builder,
        const TCodegenFragmentInfos& fragmentInfos,
        Value* expressionClosure,
        Value* literals,
        Value* rowValues);

    static TCGExprContext Make(
        const TCGOpaqueValuesContext& builder,
        const TCodegenFragmentInfos& fragmentInfos,
        Value* row,
        Value* buffer,
        Value* expressionClosurePtr = nullptr);
};

////////////////////////////////////////////////////////////////////////////////

struct TCGContext;

using TCodegenConsumer = std::function<Value*(TCGContext& builder, Value* row)>;

class TCGOperatorContext
    : public TCGOpaqueValuesContext
{
public:
    TCGOperatorContext(
        const TCGOpaqueValuesContext& base,
        Value* executionContext,
        std::vector<std::shared_ptr<TCodegenConsumer>>* consumers);

    TCGOperatorContext(const TCGOpaqueValuesContext& base, const TCGOperatorContext& other);

    Value* GetExecutionContext() const;

    TCodegenConsumer& operator[] (size_t index) const;

protected:
    Value* const ExecutionContext_;
    std::vector<std::shared_ptr<TCodegenConsumer>>* Consumers_ = nullptr;
};

struct TCGContext
    : public TCGOperatorContext
{
    Value* const Buffer;

    TCGContext(const TCGOperatorContext& base, Value* buffer);
};

////////////////////////////////////////////////////////////////////////////////

TCGValue MakePhi(
    const TCGIRBuilderPtr& builder,
    BasicBlock* thenBB,
    BasicBlock* elseBB,
    TCGValue thenValue,
    TCGValue elseValue,
    Twine name = {});

Value* MakePhi(
    const TCGIRBuilderPtr& builder,
    BasicBlock* thenBB,
    BasicBlock* elseBB,
    Value* thenValue,
    Value* elseValue,
    Twine name = {});

template <class TBuilder, class TResult>
TResult CodegenIf(
    TBuilder& builder,
    Value* condition,
    const std::function<TResult(TBuilder& builder)>& thenCodegen,
    const std::function<TResult(TBuilder& builder)>& elseCodegen,
    Twine name = {})
{
    if (llvm::Constant* constantCondition = llvm::dyn_cast<llvm::Constant>(condition)) {
        if (constantCondition->isNullValue()) {
            return elseCodegen(builder);
        } else {
            return thenCodegen(builder);
        }
    }

    auto* thenBB = builder->CreateBBHere("then");
    auto* elseBB = builder->CreateBBHere("else");
    auto* endBB = builder->CreateBBHere("end");

    builder->CreateCondBr(condition, thenBB, elseBB);

    builder->SetInsertPoint(thenBB);
    auto thenValue = thenCodegen(builder);
    thenBB = builder->GetInsertBlock();

    builder->SetInsertPoint(elseBB);
    auto elseValue = elseCodegen(builder);
    elseBB = builder->GetInsertBlock();

    builder->SetInsertPoint(endBB);

    auto result = MakePhi(builder, thenBB, elseBB, thenValue, elseValue, name);

    builder->SetInsertPoint(thenBB);
    builder->CreateBr(endBB);

    builder->SetInsertPoint(elseBB);
    builder->CreateBr(endBB);

    builder->SetInsertPoint(endBB);

    return result;
}

template <class TBuilder>
void CodegenIf(
    TBuilder& builder,
    Value* condition,
    const std::function<void(TBuilder& builder)>& thenCodegen,
    const std::function<void(TBuilder& builder)>& elseCodegen)
{
    if (llvm::Constant* constantCondition = llvm::dyn_cast<llvm::Constant>(condition)) {
        if (constantCondition->isNullValue()) {
            elseCodegen(builder);
        } else {
            thenCodegen(builder);
        }

        return;
    }

    auto* thenBB = builder->CreateBBHere("then");
    auto* elseBB = builder->CreateBBHere("else");
    auto* endBB = builder->CreateBBHere("end");

    builder->CreateCondBr(condition, thenBB, elseBB);

    builder->SetInsertPoint(thenBB);
    thenCodegen(builder);
    builder->CreateBr(endBB);
    thenBB = builder->GetInsertBlock();

    builder->SetInsertPoint(elseBB);
    elseCodegen(builder);
    builder->CreateBr(endBB);
    elseBB = builder->GetInsertBlock();

    builder->SetInsertPoint(endBB);
}

template <class TBuilder>
void CodegenIf(
    TBuilder& builder,
    Value* condition,
    const std::function<void(TBuilder& builder)>& thenCodegen)
{
    if (llvm::Constant* constantCondition = llvm::dyn_cast<llvm::Constant>(condition)) {
        if (!constantCondition->isNullValue()) {
            thenCodegen(builder);
        }

        return;
    }
    auto* thenBB = builder->CreateBBHere("then");
    auto* endBB = builder->CreateBBHere("end");

    builder->CreateCondBr(condition, thenBB, endBB);

    builder->SetInsertPoint(thenBB);
    thenCodegen(builder);
    builder->CreateBr(endBB);
    thenBB = builder->GetInsertBlock();

    builder->SetInsertPoint(endBB);
}

////////////////////////////////////////////////////////////////////////////////

template <class TSequence>
struct TApplyCallback;

template <unsigned... Indexes>
struct TApplyCallback<std::integer_sequence<unsigned, Indexes...>>
{
    template <class TBody, class TBuilder>
    static void Do(TBody&& body, TBuilder&& builder, Value* argsArray[sizeof...(Indexes)])
    {
        body(builder, argsArray[Indexes]...);
    }
};

////////////////////////////////////////////////////////////////////////////////

llvm::Attribute BuildUnwindTableAttribute(llvm::LLVMContext& context);

////////////////////////////////////////////////////////////////////////////////

struct TLlvmClosure
{
    Value* ClosurePtr;
    llvm::Function* Function;
};

template <class TSignature>
struct TClosureFunctionDefiner;

template <class TResult, class... TArgs>
struct TClosureFunctionDefiner<TResult(TArgs...)>
{
    using TIndexesPack = typename std::make_integer_sequence<unsigned, sizeof...(TArgs)>;

    template <class TBody>
    static TLlvmClosure Do(const TCGModulePtr& cgModule, TCGOperatorContext& parentBuilder, TBody&& body, llvm::Twine name)
    {
        Function* function = Function::Create(
            TTypeBuilder<TResult(void**, TArgs...)>::Get(cgModule->GetModule()->getContext()),
            Function::ExternalLinkage,
            name,
            cgModule->GetModule());

        function->addFnAttr(BuildUnwindTableAttribute(cgModule->GetModule()->getContext()));
        function->addFnAttr(llvm::Attribute::OptimizeForSize);

        auto args = function->arg_begin();
        Value* closurePtr = ConvertToPointer(args++); closurePtr->setName("closure");

        Value* argsArray[sizeof...(TArgs)];
        size_t index = 0;
        while (args != function->arg_end()) {
            argsArray[index++] = ConvertToPointer(args++);
        }
        YT_VERIFY(index == sizeof...(TArgs));

        TCGIRBuilder baseBuilder(function, parentBuilder.GetBuilder(), closurePtr);
        TCGOperatorContext builder(
            TCGOpaqueValuesContext(
                TCGBaseContext(
                    TCGIRBuilderPtr(&baseBuilder),
                    parentBuilder),
                parentBuilder),
            parentBuilder);
        TApplyCallback<TIndexesPack>::template Do(std::forward<TBody>(body), builder, argsArray);

        return TLlvmClosure{builder->GetClosure(), function};
    }
};

template <class TSignature, class TBody>
TLlvmClosure MakeClosure(TCGOperatorContext& builder, llvm::Twine name, TBody&& body)
{
    return TClosureFunctionDefiner<TSignature>::Do(
        builder.Module,
        builder,
        std::forward<TBody>(body),
        name);
}

template <class TSignature>
struct TFunctionDefiner;

template <class TResult, class... TArgs>
struct TFunctionDefiner<TResult(TArgs...)>
{
    using TIndexesPack = typename std::make_integer_sequence<unsigned, sizeof...(TArgs)>;

    template <class TBody>
    static Function* Do(const TCGModulePtr& cgModule, TBody&& body, llvm::Twine name)
    {
        auto& llvmContext = cgModule->GetModule()->getContext();
        Function* function = Function::Create(
            FunctionType::get(
                TTypeBuilder<TResult>::Get(llvmContext),
                {
                    TTypeBuilder<TArgs>::Get(llvmContext)...
                },
                false),
            Function::ExternalLinkage,
            name,
            cgModule->GetModule());

        function->addFnAttr(BuildUnwindTableAttribute(llvmContext));
        function->addFnAttr(llvm::Attribute::OptimizeForSize);

        auto args = function->arg_begin();
        Value* argsArray[sizeof...(TArgs)];
        size_t index = 0;
        while (args != function->arg_end()) {
            argsArray[index++] = ConvertToPointer(args++);
        }
        YT_VERIFY(index == sizeof...(TArgs));

        TCGIRBuilder builder(function);
        TCGBaseContext context(TCGIRBuilderPtr(&builder), cgModule);

        TApplyCallback<TIndexesPack>::template Do(std::forward<TBody>(body), context, argsArray);

        return function;
    }
};

template <class TSignature, class TBody>
Function* MakeFunction(const TCGModulePtr& cgModule, llvm::Twine name, TBody&& body)
{
    auto function = TFunctionDefiner<TSignature>::Do(
        cgModule,
        std::forward<TBody>(body),
        name);

    return function;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
