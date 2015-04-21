#pragma once

#include "cg_types.h"
#include "cg_ir_builder.h"
#include "plan_fragment_common.h"

#include <core/codegen/module.h>

namespace NYT {
namespace NQueryClient {

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
using llvm::TypeBuilder;
using llvm::Value;

using NCodegen::TCGModule;
using NCodegen::TCGModulePtr;

////////////////////////////////////////////////////////////////////////////////

class TCGContext
    : public TCGIRBuilder
{
    Value* ConstantsRow_;
    Value* ExecutionContextPtr_;

public:
    const TCGModulePtr Module;
    
    TCGContext(
        const TCGModulePtr module,
        Value* constantsRow,
        Value* executionContextPtr,
        llvm::BasicBlock* basicBlock)
        : TCGIRBuilder(basicBlock)        
        , ConstantsRow_(constantsRow)
        , ExecutionContextPtr_(executionContextPtr)
        , Module(std::move(module))
    { }

    TCGContext(
        llvm::Function* function,
        TCGContext* parent,
        llvm::Value* closurePtr)
        : TCGIRBuilder(function, parent, closurePtr)
        , ConstantsRow_(parent->ConstantsRow_)
        , ExecutionContextPtr_(parent->ExecutionContextPtr_)
        , Module(parent->Module)
    { }

    Value* GetConstantsRows()
    {
        return ViaClosure(ConstantsRow_, "constantsRow");
    }

    Value* GetExecutionContextPtr()
    {
        return ViaClosure(ExecutionContextPtr_, "executionContextPtr");
    }
};

typedef std::function<void(TCGContext& builder, Value* row)> TCodegenConsumer;
typedef std::function<void(TCGContext& builder, const TCodegenConsumer& codegenConsumer)> TCodegenSource;

Value* CodegenValuesPtrFromRow(TCGIRBuilder&, Value*);

typedef TypeBuilder<TValue, false> TTypeBuilder;
typedef TypeBuilder<TValueData, false> TDataTypeBuilder;

class TCGValue
{
private:
    Value* IsNull_;
    Value* Length_;
    Value* Data_;
    EValueType StaticType_;
    std::string Name_;

    TCGValue(Value* isNull, Value* length, Value* data, EValueType staticType, Twine name)
        : IsNull_(isNull)
        , Length_(length)
        , Data_(data)
        , StaticType_(staticType)
        , Name_(name.str())
    {
        YCHECK(
            StaticType_ == EValueType::Int64 ||
            StaticType_ == EValueType::Uint64 ||
            StaticType_ == EValueType::Double ||
            StaticType_ == EValueType::Boolean ||
            StaticType_ == EValueType::String);
    }

public:
    TCGValue(const TCGValue& other) = default;

    TCGValue(TCGValue&& other)
        : IsNull_(other.IsNull_)
        , Length_(other.Length_)
        , Data_(other.Data_)
        , StaticType_(other.StaticType_)
        , Name_(std::move(other.Name_))
    {
        other.Reset();
    }

    TCGValue& operator=(TCGValue&& other)
    {
        IsNull_ = other.IsNull_;
        Length_ = other.Length_;
        Data_ = other.Data_;
        StaticType_ = other.StaticType_;

        other.Reset();

        return *this;
    }

    TCGValue&& Steal()
    {
        return std::move(*this);
    }

    void Reset()
    {
        IsNull_ = nullptr;
        Length_ = nullptr;
        Data_ = nullptr;
        StaticType_ = EValueType::TheBottom;
    }

    EValueType GetStaticType() const
    {
        return StaticType_;
    }

    static TCGValue CreateFromValue(
        TCGIRBuilder& builder,
        Value* isNull,
        Value* length,
        Value* data,
        EValueType staticType,
        Twine name = Twine())
    {
        if (isNull) {
            YCHECK(isNull->getType() == builder.getInt1Ty());
        }
        if (length) {
            YCHECK(length->getType() == TTypeBuilder::TLength::get(builder.getContext()));
        }
        if (data) {
            YCHECK(data->getType() == TDataTypeBuilder::get(builder.getContext(), staticType));
        }
        return TCGValue(isNull, length, data, staticType, name);
    }

    static TCGValue CreateFromRow(
        TCGIRBuilder& builder,
        Value* row,
        int index,
        EValueType staticType,
        Twine name = Twine())
    {
        auto valuePtr = builder.CreateConstInBoundsGEP1_32(
            CodegenValuesPtrFromRow(builder, row),
            index,
            name + ".valuePtr");

        return CreateFromLlvmValue(
            builder,
            valuePtr,
            staticType,
            name);
    }

    static TCGValue CreateFromLlvmValue(
        TCGIRBuilder& builder,
        Value* valuePtr,
        EValueType staticType,
        Twine name = Twine())
    {
        auto type = builder.CreateLoad(
            builder.CreateStructGEP(valuePtr, TTypeBuilder::Type, name + ".typePtr"),
            name + ".type");
        auto length = builder.CreateLoad(
            builder.CreateStructGEP(valuePtr, TTypeBuilder::Length, name + ".lengthPtr"),
            name + ".length");
        auto data = builder.CreateLoad(
            builder.CreateStructGEP(valuePtr, TTypeBuilder::Data, name + ".dataPtr"),
            name + ".data");

        Type* targetType = TDataTypeBuilder::get(builder.getContext(), staticType);

        Value* castedData = nullptr;

        if (targetType->isPointerTy()) {
            castedData = builder.CreateIntToPtr(data,
                targetType,
                name + ".data");
        } else if (targetType->isFloatingPointTy()) {
            castedData = builder.CreateBitCast(data,
                targetType,
                name + ".data");
        } else {
            castedData = builder.CreateIntCast(data,
                targetType,
                false,
                name + ".data");
        }

        auto isNull = builder.CreateICmpEQ(
            type,
            builder.getInt16(static_cast<ui16>(EValueType::Null)),
            name + ".isNull");

        return CreateFromValue(builder, isNull, length, castedData, staticType, name);
    }

    static TCGValue CreateNull(
        TCGIRBuilder& builder,
        EValueType staticType,
        Twine name = Twine())
    {
        return CreateFromValue(
            builder,
            builder.getInt1(true),
            llvm::UndefValue::get(TTypeBuilder::TLength::get(builder.getContext())),
            llvm::UndefValue::get(TDataTypeBuilder::get(builder.getContext(), staticType)),
            staticType,
            name);
    }

    void StoreToRow(TCGIRBuilder& builder, Value* row, int index, ui16 id)
    {
        auto name = row->getName();
        auto nameTwine =
            (name.empty() ? Twine::createNull() : Twine(name).concat(".")) +
            Twine(".at.") +
            Twine(index);

        auto valuePtr = builder.CreateConstInBoundsGEP1_32(
            CodegenValuesPtrFromRow(builder, row),
            index,
            nameTwine);

        StoreToValue(builder, valuePtr, id);
    }

    void StoreToValue(TCGIRBuilder& builder, Value* valuePtr, ui16 id, Twine nameTwine = "")
    {
        builder.CreateStore(
            builder.getInt16(id),
            builder.CreateStructGEP(valuePtr, TTypeBuilder::Id, nameTwine + ".idPtr"));

        if (IsNull_) {
            builder.CreateStore(
                GetType(builder),
                builder.CreateStructGEP(valuePtr, TTypeBuilder::Type, nameTwine + ".typePtr"));
        }
        if (Length_) {
            builder.CreateStore(
                Length_,
                builder.CreateStructGEP(valuePtr, TTypeBuilder::Length, nameTwine + ".lengthPtr"));
        }
        if (Data_) {
            Value* data = nullptr;
            auto targetType = TDataTypeBuilder::get(builder.getContext());
                
            if (Data_->getType()->isPointerTy()) {
                data = builder.CreatePtrToInt(Data_, targetType);
            } else if (Data_->getType()->isFloatingPointTy()) {
                data = builder.CreateBitCast(Data_, targetType);
            } else {
                data = builder.CreateIntCast(Data_, targetType, false);
            }

            builder.CreateStore(
                data,
                builder.CreateStructGEP(valuePtr, TTypeBuilder::Data, nameTwine + ".dataPtr"));
        }
    }

    Value* IsNull()
    {
        YCHECK(IsNull_);
        return IsNull_;
    }

    Value* GetType(TCGIRBuilder& builder)
    {
        return builder.CreateSelect(
            IsNull(),
            builder.getInt16(static_cast<ui16>(EValueType::Null)),
            builder.getInt16(static_cast<ui16>(StaticType_)));
    }

    Value* GetLength()
    {
        return Length_;
    }

    Value* GetData()
    {
        return Data_;
    }

    TCGValue Cast(TCGIRBuilder& builder, EValueType dest, bool bitcast = false)
    {
        if (dest == StaticType_) {
            return *this;
        }

        auto value = GetData();

        Value* result;
        if (dest == EValueType::Int64) {
            auto destType = TDataTypeBuilder::TUint64::get(builder.getContext());
            if (bitcast) {
                result = builder.CreateBitCast(value, destType);
            } else if (StaticType_ == EValueType::Uint64) {
                result = builder.CreateIntCast(value, destType, false);
            } else if (StaticType_ == EValueType::Double) {
                result = builder.CreateFPToSI(value, destType);
            } else {
                YUNREACHABLE();
            }
        } else if (dest == EValueType::Uint64) {
            auto destType = TDataTypeBuilder::TUint64::get(builder.getContext());
            if (bitcast) {
                result = builder.CreateBitCast(value, destType);
            } if (StaticType_ == EValueType::Int64) {
                result = builder.CreateIntCast(value, destType, true);
            } else if (StaticType_ == EValueType::Double) {
                result = builder.CreateFPToUI(value, destType);
            } else {
                YUNREACHABLE();
            }
        } else if (dest == EValueType::Double) {
            auto destType = TDataTypeBuilder::TDouble::get(builder.getContext());
            if (bitcast) {
                result = builder.CreateBitCast(value, destType);
            } if (StaticType_ == EValueType::Uint64) {
                result = builder.CreateUIToFP(value, destType);
            } else if (StaticType_ == EValueType::Int64) {
                result = builder.CreateSIToFP(value, destType);
            } else {
                YUNREACHABLE();
            }
        } else {
            YUNREACHABLE();
        }

        return CreateFromValue(builder, IsNull(), GetLength(), result, dest);
    }
};

typedef std::function<Value* (TCGIRBuilder& builder)> TCodegenBlock;
typedef std::function<TCGValue(TCGContext& builder, Value* row)> TCodegenExpression;
typedef std::function<void(TCGContext& builder, Value* row, Value* newRow, int index)> TCodegenAggregate;

////////////////////////////////////////////////////////////////////////////////

TCodegenAggregate MakeCodegenAggregateFunction(
    const Stroka& aggregateFunction,
    EValueType type,
    Twine name = Twine());

TCodegenExpression MakeCodegenLiteralExpr(
    int index,
    EValueType type);

TCodegenExpression MakeCodegenReferenceExpr(
    int index,
    EValueType type,
    Stroka name);

TCodegenExpression MakeCodegenFunctionExpr(
    Stroka functionName,
    std::vector<TCodegenExpression> codegenArgs,
    EValueType type,
    Stroka name,
    const IFunctionRegistryPtr functionRegistry);

TCodegenExpression MakeCodegenUnaryOpExpr(
    EUnaryOp opcode,
    TCodegenExpression codegenOperand,
    EValueType type,
    Stroka name);

TCodegenExpression MakeCodegenBinaryOpExpr(
    EBinaryOp opcode,
    TCodegenExpression codegenLhs,
    TCodegenExpression codegenRhs,
    EValueType type,
    Stroka name);

TCodegenExpression MakeCodegenInOpExpr(
    std::vector<TCodegenExpression> codegenArgs,
    int arrayIndex);

////////////////////////////////////////////////////////////////////////////////

void CodegenScanOp(
    TCGContext& builder,
    const TCodegenConsumer& codegenConsumer);

TCodegenSource MakeCodegenFilterOp(
    TCodegenExpression codegenPredicate,
    TCodegenSource codegenSource);

TCodegenSource MakeCodegenJoinOp(
    std::vector<Stroka> joinColumns,
    TTableSchema sourceSchema,
    TCodegenSource codegenSource);

TCodegenSource MakeCodegenGroupOp(
    std::vector<TCodegenExpression> codegenGroupExprs,
    std::vector<std::pair<TCodegenExpression, TCodegenAggregate>> codegenAggregates,
    TCodegenSource codegenSource);

TCodegenSource MakeCodegenOrderOp(
    std::vector<Stroka> orderColumns,
    TTableSchema sourceSchema,
    TCodegenSource codegenSource);

TCodegenSource MakeCodegenProjectOp(
    std::vector<TCodegenExpression> codegenArgs,
    TCodegenSource codegenSource);

////////////////////////////////////////////////////////////////////////////////

TCGQueryCallback CodegenEvaluate(
    TCodegenSource codegenSource);

TCGExpressionCallback CodegenExpression(    
    TCodegenExpression codegenExpression);

////////////////////////////////////////////////////////////////////////////////

template <class TBuilder, class TResult>
TResult CodegenIf(
    TBuilder& builder,
    Value* condition,
    const std::function<TResult(TBuilder& builder)>& thenCodegen,
    const std::function<TResult(TBuilder& builder)>& elseCodegen,
    Twine name = Twine());

TCGValue MakeBinaryFunctionCall(
    Stroka routineName,
    std::vector<TCodegenExpression> codegenArgs,
    EValueType type,
    Stroka name,
    TCGContext& builder,
    Value* row);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

