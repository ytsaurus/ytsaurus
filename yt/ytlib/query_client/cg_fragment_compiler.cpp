#include "stdafx.h"
#include "cg_fragment_compiler.h"

#include "private.h"
#include "helpers.h"

#include "plan_fragment.h"
#include "plan_helpers.h"

#include "cg_routines.h"
#include "cg_ir_builder.h"

#include <core/codegen/public.h>
#include <core/codegen/module.h>

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/schema.h>

#include <core/logging/log.h>

#include <llvm/IR/Module.h>

// TODO(sandello):
//  - Implement basic logging & profiling within evaluation code
//  - Shadow innerBuilders everywhere
//  - Sometimes we can write through scratch space; some simple cases:
//    * int/double/null expressions only,
//    * string expressions with references (just need to copy string data)
//    It is possible to do better memory management here.
//  - TBAA is a king
//  - Capture pointers by value in ViaClosure

// FIXME: try to fix in new visual studio
#ifdef _win_
#define MOVE(name) name
#else
#define MOVE(name) name = std::move(name)
#endif

namespace NYT {
namespace NQueryClient {

using namespace NVersionedTableClient;
using namespace NConcurrency;

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
        TCGModulePtr module,
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

static Value* CodegenValuesPtrFromRow(TCGIRBuilder&, Value*);

typedef TypeBuilder<TValue, false> TTypeBuilder;
typedef TypeBuilder<TValueData, false> TDataTypeBuilder;

class TCGValue
{
private:
    Value* Type_;
    Value* Length_;
    Value* Data_;
    EValueType StaticType_;
    std::string Name_;

    TCGValue(Value* type, Value* length, Value* data, EValueType staticType, Twine name)
        : Type_(type)
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
        : Type_(other.Type_)
        , Length_(other.Length_)
        , Data_(other.Data_)
        , StaticType_(other.StaticType_)
        , Name_(std::move(other.Name_))
    {
        other.Reset();
    }

    TCGValue& operator=(TCGValue&& other)
    {
        Type_ = other.Type_;
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
        Type_ = nullptr;
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
        Value* type,
        Value* length,
        Value* data,
        EValueType staticType,
        Twine name = Twine())
    {
        if (type) {
            YCHECK(type->getType() == TTypeBuilder::TType::get(builder.getContext()));
        }
        if (length) {
            YCHECK(length->getType() == TTypeBuilder::TLength::get(builder.getContext()));
        }
        if (data) {
            YCHECK(data->getType() == TDataTypeBuilder::get(builder.getContext(), staticType));
        }
        return TCGValue(type, length, data, staticType, name);
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
                Twine(name) + ".data");
        } else if (targetType->isFloatingPointTy()) {
            castedData = builder.CreateBitCast(data,
                targetType,
                Twine(name) + ".data");
        } else {
            castedData = builder.CreateIntCast(data,
                targetType,
                false,
                Twine(name) + ".data");
        }

        return CreateFromValue(builder, type, length, castedData, staticType, name);
    }

    static TCGValue CreateNull(
        TCGIRBuilder& builder,
        EValueType staticType,
        Twine name = Twine())
    {
        return CreateFromValue(
            builder, 
            builder.getInt16(static_cast<ui16>(EValueType::Null)),
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

        if (Type_) {
            builder.CreateStore(
                Type_,
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

    Value* IsNull(TCGIRBuilder& builder)
    {
        // A little bit of manual constant folding.
        if (Type_ && llvm::isa<ConstantInt>(Type_)) {
            auto* constantType = llvm::cast<ConstantInt>(Type_);
            if (constantType->getZExtValue() == static_cast<ui16>(EValueType::Null)) {
                return builder.getFalse();
            }
        }
        return builder.CreateICmpEQ(
            Type_,
            builder.getInt16(static_cast<ui16>(EValueType::Null)),
            Twine(Name_) + ".isNull");
    }

    Value* GetType()
    {
        return Type_;
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

        return CreateFromValue(builder, GetType(), GetLength(), result, dest);
    }
};

typedef std::function<Value* (TCGIRBuilder& builder)> TCodegenBlock;
typedef std::function<TCGValue(TCGContext& builder, Value* row)> TCodegenExpression;
typedef std::function<void(TCGContext& builder, Value* row, Value* newRow)> TCodegenAggregate;

TCGValue MakePhi(
    TCGIRBuilder& builder,
    BasicBlock* thenBB,
    BasicBlock* elseBB,
    TCGValue thenValue,
    TCGValue elseValue,
    Twine name = Twine())
{
    Value* thenType = thenValue.GetType();
    Value* thenLength = thenValue.GetLength();
    Value* thenData = thenValue.GetData();
    
    Value* elseType = elseValue.GetType();
    Value* elseLength = elseValue.GetLength();
    Value* elseData = elseValue.GetData();

    PHINode* phiType = builder.CreatePHI(builder.getInt16Ty(), 2, name + ".phiType");
    phiType->addIncoming(thenType, thenBB);
    phiType->addIncoming(elseType, elseBB);

    YCHECK(thenValue.GetStaticType() == elseValue.GetStaticType());
    EValueType type = thenValue.GetStaticType();
    YCHECK(thenData->getType() == elseData->getType());

    PHINode* phiData = builder.CreatePHI(thenData->getType(), 2, name + ".phiData");
    phiData->addIncoming(thenData, thenBB);
    phiData->addIncoming(elseData, elseBB);

    PHINode* phiLength = nullptr;
    if (type == EValueType::String) {
        YCHECK(thenLength->getType() == elseLength->getType());

        phiLength = builder.CreatePHI(thenLength->getType(), 2, name + ".phiLength");
        phiLength->addIncoming(thenLength, thenBB);
        phiLength->addIncoming(elseLength, elseBB);
    }

    return TCGValue::CreateFromValue(builder, phiType, phiLength, phiData, type, name);
}

Value* MakePhi(
    TCGIRBuilder& builder,
    BasicBlock* thenBB,
    BasicBlock* elseBB,
    Value* thenValue,
    Value* elseValue,
    Twine name = Twine())
{
    PHINode* phiValue = builder.CreatePHI(thenValue->getType(), 2, name + ".phiValue");
    phiValue->addIncoming(thenValue, thenBB);
    phiValue->addIncoming(elseValue, elseBB);
    return phiValue;
}

template <class TBuilder, class TResult>
TResult CodegenIf(
    TBuilder& builder,
    Value* condition,
    const std::function<TResult(TBuilder& builder)>& thenCodegen,
    const std::function<TResult(TBuilder& builder)>& elseCodegen,
    Twine name = Twine())
{
    auto* thenBB = builder.CreateBBHere("then");
    auto* elseBB = builder.CreateBBHere("else");
    auto* endBB = builder.CreateBBHere("end");

    builder.CreateCondBr(condition, thenBB, elseBB);

    builder.SetInsertPoint(thenBB);
    auto thenValue = thenCodegen(builder);
    builder.CreateBr(endBB);
    thenBB = builder.GetInsertBlock();

    builder.SetInsertPoint(elseBB);
    auto elseValue = elseCodegen(builder);
    builder.CreateBr(endBB);
    elseBB = builder.GetInsertBlock();

    builder.SetInsertPoint(endBB);

    return MakePhi(builder, thenBB, elseBB, thenValue, elseValue, name);
}

template <class TBuilder>
void CodegenIf(
    TBuilder& builder,
    Value* condition,
    const std::function<void(TBuilder& builder)>& thenCodegen,
    const std::function<void(TBuilder& builder)>& elseCodegen)
{
    auto* thenBB = builder.CreateBBHere("then");
    auto* elseBB = builder.CreateBBHere("else");
    auto* endBB = builder.CreateBBHere("end");

    builder.CreateCondBr(condition, thenBB, elseBB);

    builder.SetInsertPoint(thenBB);
    thenCodegen(builder);
    builder.CreateBr(endBB);
    thenBB = builder.GetInsertBlock();

    builder.SetInsertPoint(elseBB);
    elseCodegen(builder);
    builder.CreateBr(endBB);
    elseBB = builder.GetInsertBlock();

    builder.SetInsertPoint(endBB);
}

////////////////////////////////////////////////////////////////////////////////
// Row manipulation helpers
//

static Value* CodegenValuesPtrFromRow(TCGIRBuilder& builder, Value* row)
{
    auto name = row->getName();
    auto namePrefix = name.empty() ? Twine::createNull() : Twine(name).concat(".");

    auto headerPtr = builder.CreateExtractValue(
        row,
        TypeBuilder<TRow, false>::Fields::Header,
        namePrefix + "headerPtr");
    auto valuesPtr = builder.CreatePointerCast(
        builder.CreateConstInBoundsGEP1_32(headerPtr, 1, "valuesPtrUncasted"),
        TypeBuilder<TValue*, false>::get(builder.getContext()),
        namePrefix + "valuesPtr");

    return valuesPtr;
}

////////////////////////////////////////////////////////////////////////////////
// Operator helpers
//

void CodegenAggregateFunction(
    TCGContext& builder,
    Value* aggregateRow,
    Value* newRow,
    EAggregateFunctions aggregateFunction,
    int index,
    ui16 id,
    EValueType type,
    Twine name = Twine())
{
    auto newValue = TCGValue::CreateFromRow(builder, newRow, index, type, name + ".new");

    CodegenIf<TCGContext>(
        builder,
        newValue.IsNull(builder),
        [&] (TCGContext& builder) { },
        [&] (TCGContext& builder) {
            auto aggregateValue = TCGValue::CreateFromRow(builder, aggregateRow, index, type, name + ".aggregate");

            CodegenIf<TCGContext, TCGValue>(
                builder,
                aggregateValue.IsNull(builder),
                [&] (TCGContext& builder) {
                    return newValue;
                },
                [&] (TCGContext& builder) {
                    Value* newData = newValue.GetData();
                    Value* aggregateData = aggregateValue.GetData();
                    Value* resultData = nullptr;

                    // TODO(lukyan): support other types

                    switch (aggregateFunction) {
                        case EAggregateFunctions::Sum:
                            switch (type) {
                                case EValueType::Int64:
                                case EValueType::Uint64:
                                    resultData = builder.CreateAdd(
                                        aggregateData,
                                        newData);
                                    break;
                                case EValueType::Double:
                                    resultData = builder.CreateFAdd(
                                        aggregateData,
                                        newData);
                                    break;
                                default:
                                    YUNIMPLEMENTED();
                            }
                            break;
                        case EAggregateFunctions::Min:{
                            Value* compareResult = nullptr;
                            switch (type) {
                                case EValueType::Int64:
                                    compareResult = builder.CreateICmpSLE(aggregateData, newData);
                                    break;
                                case EValueType::Uint64:
                                    compareResult = builder.CreateICmpULE(aggregateData, newData);
                                    break;
                                case EValueType::Double:
                                    compareResult = builder.CreateFCmpULE(aggregateData, newData);
                                    break;
                                default:
                                    YUNIMPLEMENTED();
                            }

                            resultData = builder.CreateSelect(
                                compareResult,
                                aggregateData,
                                newData);
                            break;
                        }
                        case EAggregateFunctions::Max:{
                            Value* compareResult = nullptr;
                            switch (type) {
                                case EValueType::Int64:
                                    compareResult = builder.CreateICmpSGE(aggregateData, newData);
                                    break;
                                case EValueType::Uint64:
                                    compareResult = builder.CreateICmpUGE(aggregateData, newData);
                                    break;
                                case EValueType::Double:
                                    compareResult = builder.CreateFCmpUGE(aggregateData, newData);
                                    break;
                                default:
                                    YUNIMPLEMENTED();
                            }

                            resultData = builder.CreateSelect(
                                compareResult,
                                aggregateData,
                                newData);
                            break;
                        }
                        default:
                            YUNIMPLEMENTED();
                    }

                    return TCGValue::CreateFromValue(
                        builder,
                        builder.getInt16(static_cast<ui16>(type)),
                        nullptr,
                        resultData,
                        type,
                        name);
                }).StoreToRow(builder, aggregateRow, index, id);
        });
}

void CodegenForEachRow(
    TCGContext& builder,
    Value* rows,
    Value* size,
    Value* stopFlag,
    const TCodegenConsumer& codegenConsumer)
{
    auto* loopBB = builder.CreateBBHere("loop");
    auto* condBB = builder.CreateBBHere("cond");
    auto* endloopBB = builder.CreateBBHere("endloop");

    // index = 0
    Value* indexPtr = builder.CreateAlloca(builder.getInt32Ty(), nullptr, "indexPtr");
    builder.CreateStore(builder.getInt32(0), indexPtr);

    builder.CreateBr(condBB);

    builder.SetInsertPoint(condBB);

    // if (index != size) ...
    Value* index = builder.CreateLoad(indexPtr, "index");
    Value* condition = builder.CreateAnd(
        builder.CreateICmpNE(index, size),
        builder.CreateICmpEQ(
            builder.CreateLoad(stopFlag, "stopFlag"),
            builder.getInt8(0)));
    builder.CreateCondBr(condition, loopBB, endloopBB);

    builder.SetInsertPoint(loopBB);

    // row = rows[index]; consume(row);
    Value* stackState = builder.CreateStackSave("stackState");
    Value* row = builder.CreateLoad(builder.CreateGEP(rows, index, "rowPtr"), "row");
    codegenConsumer(builder, row);
    builder.CreateStackRestore(stackState);
    // index = index + 1
    builder.CreateStore(builder.CreateAdd(index, builder.getInt32(1)), indexPtr);
    builder.CreateBr(condBB);

    builder.SetInsertPoint(endloopBB);
}

////////////////////////////////////////////////////////////////////////////////
// Expressions
//

Function* CodegenGroupComparerFunction(
    const std::vector<EValueType>& types,
    const TCGModule& module)
{
    auto llvmModule = module.GetModule();
    Function* function = Function::Create(
        TypeBuilder<char(TRow, TRow), false>::get(llvmModule->getContext()),
        Function::ExternalLinkage,
        "GroupComparer",
        llvmModule);

    auto args = function->arg_begin();
    Value* lhsRow = args; lhsRow->setName("lhsRow");
    Value* rhsRow = ++args; rhsRow->setName("rhsRow");
    YCHECK(++args == function->arg_end());

    TCGIRBuilder builder(BasicBlock::Create(llvmModule->getContext(), "entry", function));

    auto returnIf = [&] (Value* condition) {
        auto* thenBB = builder.CreateBBHere("then");
        auto* elseBB = builder.CreateBBHere("else");
        builder.CreateCondBr(condition, thenBB, elseBB);
        builder.SetInsertPoint(thenBB);
        builder.CreateRet(builder.getInt8(0));
        builder.SetInsertPoint(elseBB);
    };

    auto codegenEqualOp = [&] (size_t index) {
        auto lhsValue = TCGValue::CreateFromRow(
            builder,
            lhsRow,
            index,
            types[index]);

        auto rhsValue = TCGValue::CreateFromRow(
            builder,
            rhsRow,
            index,
            types[index]);

        CodegenIf<TCGIRBuilder>(
            builder,
            builder.CreateOr(lhsValue.IsNull(builder), rhsValue.IsNull(builder)),
            [&] (TCGIRBuilder& builder) {
                returnIf(builder.CreateICmpNE(lhsValue.GetType(), rhsValue.GetType()));
            },
            [&] (TCGIRBuilder& builder) {
                auto* lhsData = lhsValue.GetData();
                auto* rhsData = rhsValue.GetData();

                switch (types[index]) {
                    case EValueType::Boolean:
                    case EValueType::Int64:
                    case EValueType::Uint64:
                        returnIf(builder.CreateICmpNE(lhsData, rhsData));
                        break;

                    case EValueType::Double:
                        returnIf(builder.CreateFCmpUNE(lhsData, rhsData));
                        break;

                    case EValueType::String: {
                        Value* lhsLength = lhsValue.GetLength();
                        Value* rhsLength = rhsValue.GetLength();

                        Value* minLength = builder.CreateSelect(
                            builder.CreateICmpULT(lhsLength, rhsLength),
                            lhsLength,
                            rhsLength);

                        Value* cmpResult = builder.CreateCall3(
                            module.GetRoutine("memcmp"),
                            lhsData,
                            rhsData,
                            builder.CreateZExt(minLength, builder.getSizeType()));

                        returnIf(builder.CreateOr(
                            builder.CreateICmpNE(cmpResult, builder.getInt32(0)),
                            builder.CreateICmpNE(lhsLength, rhsLength)));
                        break;
                    }

                    default:
                        YUNREACHABLE();
                }
            });
    };

    YCHECK(!types.empty());

    for (size_t index = 0; index < types.size(); ++index) {
        codegenEqualOp(index);
    }

    builder.CreateRet(builder.getInt8(1));

    return function;
}

Function* CodegenGroupHasherFunction(
    const std::vector<EValueType>& types,
    const TCGModule& module)
{
    auto llvmModule = module.GetModule();
    Function* function = Function::Create(
        TypeBuilder<ui64(TRow), false>::get(llvmModule->getContext()),
        Function::ExternalLinkage,
        "GroupHasher",
        llvmModule);

    auto args = function->arg_begin();
    Value* row = args; row->setName("row");
    YCHECK(++args == function->arg_end());

    TCGIRBuilder builder(BasicBlock::Create(llvmModule->getContext(), "entry", function));

    auto codegenHashOp = [&] (size_t index, TCGIRBuilder& builder) -> Value* {
        auto value = TCGValue::CreateFromRow(
            builder,
            row,
            index,
            types[index]);

        auto* conditionBB = builder.CreateBBHere("condition");
        auto* thenBB = builder.CreateBBHere("then");
        auto* elseBB = builder.CreateBBHere("else");
        auto* endBB = builder.CreateBBHere("end");

        builder.CreateBr(conditionBB);

        builder.SetInsertPoint(conditionBB);
        builder.CreateCondBr(value.IsNull(builder), elseBB, thenBB);
        conditionBB = builder.GetInsertBlock();

        builder.SetInsertPoint(thenBB);

        Value* thenResult;

        switch (value.GetStaticType()) {
            case EValueType::Int64:
            case EValueType::Uint64:
            case EValueType::Double:
                thenResult = value.Cast(builder, EValueType::Uint64, true).GetData();
                break;

            case EValueType::String:
                thenResult = builder.CreateCall2(
                    module.GetRoutine("StringHash"),
                    value.GetData(),
                    value.GetLength());
                break;

            default:
                YUNIMPLEMENTED();
        }

        builder.CreateBr(endBB);
        thenBB = builder.GetInsertBlock();

        builder.SetInsertPoint(elseBB);
        auto* elseResult = builder.getInt64(0);
        builder.CreateBr(endBB);
        elseBB = builder.GetInsertBlock();

        builder.SetInsertPoint(endBB);

        PHINode* result = builder.CreatePHI(thenResult->getType(), 2);
        result->addIncoming(thenResult, thenBB);
        result->addIncoming(elseResult, elseBB);

        return result;
    };

    auto codegenHashCombine = [&] (TCGIRBuilder& builder, Value* first, Value* second) -> Value* {
        //first ^ (second + 0x9e3779b9 + (second << 6) + (second >> 2));
        return builder.CreateXor(
            first,
            builder.CreateAdd(
                builder.CreateAdd(
                    builder.CreateAdd(second, builder.getInt64(0x9e3779b9)),
                    builder.CreateLShr(second, builder.getInt64(2))),
                builder.CreateShl(second, builder.getInt64(6))));
    };

    YCHECK(!types.empty());
    auto result = codegenHashOp(0, builder);

    for (size_t index = 1; index < types.size(); ++index) {
        result = codegenHashCombine(builder, result, codegenHashOp(index, builder));
    }

    builder.CreateRet(result);

    return function;
}

Function* CodegenRowComparerFunction(
    const std::vector<EValueType>& types,
    const TCGModule& module)
{
    auto llvmModule = module.GetModule();
    Function* function = Function::Create(
        TypeBuilder<char(TRow, TRow), false>::get(llvmModule->getContext()),
        Function::ExternalLinkage,
        "RowComparer",
        llvmModule);

    auto args = function->arg_begin();
    Value* lhsRow = args; lhsRow->setName("lhsRow");
    Value* rhsRow = ++args; rhsRow->setName("rhsRow");
    YCHECK(++args == function->arg_end());

    TCGIRBuilder builder(BasicBlock::Create(llvmModule->getContext(), "entry", function));

    auto returnIf = [&] (Value* condition, const TCodegenBlock& codegenInner) {
        auto* thenBB = builder.CreateBBHere("then");
        auto* elseBB = builder.CreateBBHere("else");
        builder.CreateCondBr(condition, thenBB, elseBB);
        builder.SetInsertPoint(thenBB);
        builder.CreateRet(builder.CreateSelect(codegenInner(builder), builder.getInt8(1), builder.getInt8(0)));
        builder.SetInsertPoint(elseBB);
    };

    auto codegenEqualOrLessOp = [&] (size_t index) {
        auto lhsValue = TCGValue::CreateFromRow(
            builder,
            lhsRow,
            index,
            types[index]);

        auto rhsValue = TCGValue::CreateFromRow(
            builder,
            rhsRow,
            index,
            types[index]);

        CodegenIf<TCGIRBuilder>(
            builder,
            builder.CreateOr(lhsValue.IsNull(builder), rhsValue.IsNull(builder)),
            [&] (TCGIRBuilder& builder) {
                returnIf(
                    builder.CreateICmpNE(lhsValue.GetType(), rhsValue.GetType()),
                    [&] (TCGIRBuilder&) {
                        return builder.CreateICmpULT(lhsValue.GetType(), rhsValue.GetType());
                    });
            },
            [&] (TCGIRBuilder& builder) {
                auto* lhsData = lhsValue.GetData();
                auto* rhsData = rhsValue.GetData();

                switch (types[index]) {
                    case EValueType::Boolean:
                    case EValueType::Int64:
                        returnIf(
                            builder.CreateICmpNE(lhsData, rhsData),
                            [&] (TCGIRBuilder&) {
                                return builder.CreateICmpSLT(lhsData, rhsData);
                            });
                        break;

                    case EValueType::Uint64:
                        returnIf(
                            builder.CreateICmpNE(lhsData, rhsData),
                            [&] (TCGIRBuilder&) {
                                return builder.CreateICmpULT(lhsData, rhsData);
                            });
                        break;

                    case EValueType::Double:
                        returnIf(
                            builder.CreateFCmpUNE(lhsData, rhsData),
                            [&] (TCGIRBuilder&) {
                                return builder.CreateFCmpULT(lhsData, rhsData);
                            });
                        break;

                    case EValueType::String: {
                        Value* lhsLength = lhsValue.GetLength();
                        Value* rhsLength = rhsValue.GetLength();

                        Value* minLength = builder.CreateSelect(
                            builder.CreateICmpULT(lhsLength, rhsLength),
                            lhsLength,
                            rhsLength);
                    
                        Value* cmpResult = builder.CreateCall3(
                            module.GetRoutine("memcmp"),
                            lhsData,
                            rhsData,
                            builder.CreateZExt(minLength, builder.getSizeType()));

                        returnIf(
                            builder.CreateICmpNE(cmpResult, builder.getInt32(0)),
                            [&] (TCGIRBuilder&) {
                                return builder.CreateICmpSLT(cmpResult, builder.getInt32(0));
                            });

                        returnIf(
                            builder.CreateICmpNE(lhsLength, rhsLength),
                            [&] (TCGIRBuilder&) {
                                return builder.CreateICmpULT(lhsLength, rhsLength);
                            });

                        break;
                    }

                    default:
                        YUNREACHABLE();
                }
            });
    };

    YCHECK(!types.empty());

    for (size_t index = 0; index < types.size(); ++index) {
        codegenEqualOrLessOp(index);
    }

    builder.CreateRet(builder.getInt8(0));

    return function;
}

TCodegenExpression MakeCodegenFunctionExpr(
    Stroka functionName,
    std::vector<TCodegenExpression> codegenArgs,
    EValueType type,
    Stroka name)
{
    functionName.to_lower();

    return [
        MOVE(functionName),
        MOVE(codegenArgs),
        MOVE(type),
        MOVE(name)
    ] (TCGContext& builder, Value* row) {
        auto nameTwine = Twine(name.c_str());

        if (functionName == "if") {
            YCHECK(codegenArgs.size() == 3);
            auto condition = codegenArgs[0](builder, row);
            YCHECK(condition.GetStaticType() == EValueType::Boolean);

            return CodegenIf<TCGContext, TCGValue>(
                builder,
                condition.IsNull(builder),
                [&] (TCGContext& builder) {
                    return TCGValue::CreateNull(builder, type);
                },
                [&] (TCGContext& builder) {
                    return CodegenIf<TCGContext, TCGValue>(
                        builder,
                        builder.CreateICmpNE(
                            builder.CreateZExtOrBitCast(condition.GetData(), builder.getInt64Ty()),
                            builder.getInt64(0)),
                        [&] (TCGContext& builder) {
                            return codegenArgs[1](builder, row);
                        },
                        [&] (TCGContext& builder) {
                            return codegenArgs[2](builder, row);
                        });
                },
                nameTwine);
        } else if (functionName == "is_prefix") {
            YCHECK(codegenArgs.size() == 2);
            auto lhsValue = codegenArgs[0](builder, row);
            YCHECK(lhsValue.GetStaticType() == EValueType::String);
    
            return CodegenIf<TCGContext, TCGValue>(
                builder,
                lhsValue.IsNull(builder),
                [&] (TCGContext& builder) {
                    return TCGValue::CreateNull(builder, type);
                },
                [&] (TCGContext& builder) {
                    auto rhsValue = codegenArgs[1](builder, row);
                    YCHECK(rhsValue.GetStaticType() == EValueType::String);

                    return CodegenIf<TCGContext, TCGValue>(
                        builder,
                        rhsValue.IsNull(builder),
                        [&] (TCGContext& builder) {
                            return TCGValue::CreateNull(builder, type);
                        },
                        [&] (TCGContext& builder) {
                            Value* lhsData = lhsValue.GetData();
                            Value* lhsLength = lhsValue.GetLength();
                            Value* rhsData = rhsValue.GetData();
                            Value* rhsLength = rhsValue.GetLength();

                            Value* result = builder.CreateCall4(
                                builder.Module->GetRoutine("IsPrefix"),
                                lhsData, lhsLength, rhsData, rhsLength);

                            return TCGValue::CreateFromValue(
                                builder,
                                builder.getInt16(static_cast<ui16>(type)),
                                nullptr,
                                result,
                                type);
                        });
                },
                nameTwine);
        } else if (functionName == "lower") {
            YCHECK(codegenArgs.size() == 1);
            auto argValue = codegenArgs[0](builder, row);
            YCHECK(argValue.GetStaticType() == EValueType::String);

            return CodegenIf<TCGContext, TCGValue>(
                builder,
                argValue.IsNull(builder),
                [&] (TCGContext& builder) {
                    return TCGValue::CreateNull(builder, type);
                },
                [&] (TCGContext& builder) {
                    Value* argData = argValue.GetData();
                    Value* argLength = argValue.GetLength();

                    Value* result = builder.CreateCall3(
                        builder.Module->GetRoutine("ToLower"),
                        builder.GetExecutionContextPtr(),
                        argData,
                        argLength);

                    return TCGValue::CreateFromValue(
                        builder,
                        builder.getInt16(static_cast<ui16>(type)),
                        argLength,
                        result,
                        type);
                },
                nameTwine);
        } else if (functionName == "is_null") {
            YCHECK(codegenArgs.size() == 1);
            auto argValue = codegenArgs[0](builder, row);

            return TCGValue::CreateFromValue(
                builder,
                builder.getInt16(static_cast<ui16>(type)),
                nullptr,            
                builder.CreateZExtOrBitCast(
                    argValue.IsNull(builder),
                    TDataTypeBuilder::TBoolean::get(builder.getContext())),
                type);
        } else if (functionName == "int64" || functionName == "uint64" || functionName == "double") {
            YCHECK(codegenArgs.size() == 1);
            return codegenArgs[0](builder, row).Cast(builder, type);
        }

        YUNIMPLEMENTED();
    };
}

TCodegenExpression MakeCodegenUnaryOpExpr(
    EUnaryOp opcode,
    TCodegenExpression codegenOperand,
    EValueType type,
    Stroka name)
{
    return [
        MOVE(opcode),
        MOVE(codegenOperand),
        MOVE(type),
        MOVE(name)
    ] (TCGContext& builder, Value* row) {

        auto operandValue = codegenOperand(builder, row);

        return CodegenIf<TCGContext, TCGValue>(
            builder,
            operandValue.IsNull(builder),
            [&] (TCGIRBuilder& builder) {
                return TCGValue::CreateNull(builder, type);
            },
            [&] (TCGIRBuilder& builder) {
                auto operandType = operandValue.GetStaticType();
                Value* operandData = operandValue.GetData();
                Value* evalData = nullptr;

                switch(opcode) {
                    case EUnaryOp::Plus:
                        evalData = operandData;
                        break;

                    case EUnaryOp::Minus:
                        switch (operandType) {
                            case EValueType::Int64:
                            case EValueType::Uint64:
                                evalData = builder.CreateSub(builder.getInt64(0), operandData);
                                break;
                            case EValueType::Double:
                                evalData = builder.CreateFSub(ConstantFP::get(builder.getDoubleTy(), 0.0), operandData);
                                break;
                            default:
                                YUNREACHABLE();
                        }

                        break;
                    default:
                        YUNREACHABLE();
                }

                return TCGValue::CreateFromValue(
                    builder,
                    builder.getInt16(static_cast<ui16>(type)),
                    nullptr,
                    evalData,
                    type);
            },
            Twine(name.c_str()));
    };
}

TCodegenExpression MakeCodegenBinaryOpExpr(
    EBinaryOp opcode,
    TCodegenExpression codegenLhs,
    TCodegenExpression codegenRhs,
    EValueType type,
    Stroka name)
{
    return [
        MOVE(opcode),
        MOVE(codegenLhs),
        MOVE(codegenRhs),
        MOVE(type),
        MOVE(name)
    ] (TCGContext& builder, Value* row) {
        auto nameTwine = Twine(name.c_str());

        auto lhsValue = codegenLhs(builder, row);

        return CodegenIf<TCGContext, TCGValue>(
            builder,
            lhsValue.IsNull(builder),
            [&] (TCGContext& builder) {
                return TCGValue::CreateNull(builder, type);
            },
            [&] (TCGContext& builder) {
                auto rhsValue = codegenRhs(builder, row);

                return CodegenIf<TCGContext, TCGValue>(
                    builder,
                    rhsValue.IsNull(builder),
                    [&] (TCGContext& builder) {
                        return TCGValue::CreateNull(builder, type);
                    },
                    [&] (TCGContext& builder) {
                        YCHECK(lhsValue.GetStaticType() == rhsValue.GetStaticType());
                        auto operandType = lhsValue.GetStaticType();

                        Value* lhsData = lhsValue.GetData();
                        Value* rhsData = rhsValue.GetData();
                        Value* evalData = nullptr;

                        #define OP(opcode, optype) \
                            case EBinaryOp::opcode: \
                                evalData = builder.Create##optype(lhsData, rhsData); \
                                break;

                        #define CMP_OP(opcode, optype) \
                            case EBinaryOp::opcode: \
                                evalData = builder.CreateZExtOrBitCast( \
                                    builder.Create##optype(lhsData, rhsData), \
                                    TDataTypeBuilder::TBoolean::get(builder.getContext())); \
                                break;

                        switch (operandType) {

                            case EValueType::Boolean:
                            case EValueType::Int64:
                                switch (opcode) {
                                    OP(Plus, Add)
                                    OP(Minus, Sub)
                                    OP(Multiply, Mul)
                                    OP(Divide, SDiv)
                                    OP(Modulo, SRem)
                                    OP(And, And)
                                    OP(Or, Or)
                                    CMP_OP(Equal, ICmpEQ)
                                    CMP_OP(NotEqual, ICmpNE)
                                    CMP_OP(Less, ICmpSLT)
                                    CMP_OP(LessOrEqual, ICmpSLE)
                                    CMP_OP(Greater, ICmpSGT)
                                    CMP_OP(GreaterOrEqual, ICmpSGE)
                                    default:
                                        YUNREACHABLE();
                                }
                                break;
                            case EValueType::Uint64:
                                switch (opcode) {
                                    OP(Plus, Add)
                                    OP(Minus, Sub)
                                    OP(Multiply, Mul)
                                    OP(Divide, UDiv)
                                    OP(Modulo, URem)
                                    OP(And, And)
                                    OP(Or, Or)
                                    CMP_OP(Equal, ICmpEQ)
                                    CMP_OP(NotEqual, ICmpNE)
                                    CMP_OP(Less, ICmpULT)
                                    CMP_OP(LessOrEqual, ICmpULE)
                                    CMP_OP(Greater, ICmpUGT)
                                    CMP_OP(GreaterOrEqual, ICmpUGE)
                                    default:
                                        YUNREACHABLE();
                                }
                                break;
                            case EValueType::Double:
                                switch (opcode) {
                                    OP(Plus, FAdd)
                                    OP(Minus, FSub)
                                    OP(Multiply, FMul)
                                    OP(Divide, FDiv)
                                    CMP_OP(Equal, FCmpUEQ)
                                    CMP_OP(NotEqual, FCmpUNE)
                                    CMP_OP(Less, FCmpULT)
                                    CMP_OP(LessOrEqual, FCmpULE)
                                    CMP_OP(Greater, FCmpUGT)
                                    CMP_OP(GreaterOrEqual, FCmpUGE)
                                    default:
                                        YUNREACHABLE();
                                }
                                break;
                            case EValueType::String: {
                                Value* lhsLength = lhsValue.GetLength();
                                Value* rhsLength = rhsValue.GetLength();

                                auto codegenEqual = [&] () {
                                    return CodegenIf<TCGContext, Value*>(
                                        builder,
                                        builder.CreateICmpEQ(lhsLength, rhsLength),
                                        [&] (TCGContext& builder) {
                                            Value* minLength = builder.CreateSelect(
                                                builder.CreateICmpULT(lhsLength, rhsLength),
                                                lhsLength,
                                                rhsLength);

                                            Value* cmpResult = builder.CreateCall3(
                                                builder.Module->GetRoutine("memcmp"),
                                                lhsData,
                                                rhsData,
                                                builder.CreateZExt(minLength, builder.getSizeType()));

                                            return builder.CreateICmpEQ(cmpResult, builder.getInt32(0));
                                        },
                                        [&] (TCGContext& builder) {
                                            return builder.getFalse();
                                        });
                                };

                                auto codegenLexicographicalCompare = [&] (Value* lhsData, Value* lhsLength, Value* rhsData, Value* rhsLength) {
                                    Value* lhsLengthIsLess = builder.CreateICmpULT(lhsLength, rhsLength);
                                    Value* minLength = builder.CreateSelect(
                                        lhsLengthIsLess,
                                        lhsLength,
                                        rhsLength);

                                    Value* cmpResult = builder.CreateCall3(
                                        builder.Module->GetRoutine("memcmp"),
                                        lhsData,
                                        rhsData,
                                        builder.CreateZExt(minLength, builder.getSizeType()));

                                    return builder.CreateOr(
                                        builder.CreateICmpSLT(cmpResult, builder.getInt32(0)),
                                        builder.CreateAnd(
                                            builder.CreateICmpEQ(cmpResult, builder.getInt32(0)),
                                            lhsLengthIsLess));
                                };

                                switch (opcode) {
                                    case EBinaryOp::Equal:
                                        evalData = codegenEqual();
                                        break;
                                    case EBinaryOp::NotEqual:
                                        evalData = builder.CreateNot(codegenEqual());
                                        break;
                                    case EBinaryOp::Less:
                                        evalData = codegenLexicographicalCompare(lhsData, lhsLength, rhsData, rhsLength);
                                        break;
                                    case EBinaryOp::Greater:
                                        evalData = codegenLexicographicalCompare(rhsData, rhsLength, lhsData, lhsLength);
                                        break;
                                    case EBinaryOp::LessOrEqual:
                                        evalData =  builder.CreateNot(
                                            codegenLexicographicalCompare(rhsData, rhsLength, lhsData, lhsLength));
                                        break;
                                    case EBinaryOp::GreaterOrEqual:
                                        evalData = builder.CreateNot(
                                            codegenLexicographicalCompare(lhsData, lhsLength, rhsData, rhsLength));
                                        break;
                                    default:
                                        YUNREACHABLE();
                                }

                                evalData = builder.CreateZExtOrBitCast(
                                    evalData,
                                    TDataTypeBuilder::TBoolean::get(builder.getContext()));
                                break;
                            }
                            default:
                                YUNREACHABLE();
                        }

                        #undef OP
                        #undef CMP_OP

                        return TCGValue::CreateFromValue(
                            builder,
                            builder.getInt16(static_cast<ui16>(type)),
                            nullptr,
                            evalData,
                            type);
                    });
           },
            nameTwine);
    };
}

TCodegenExpression MakeCodegenInOpExpr(
    std::vector<TCodegenExpression> codegenArgs,
    int arrayIndex)
{
    return [
        MOVE(codegenArgs),
        MOVE(arrayIndex)
    ] (TCGContext& builder, Value* row) {
        size_t keySize = codegenArgs.size();

        Value* newRowPtr = builder.CreateAlloca(TypeBuilder<TRow, false>::get(builder.getContext()));
        Value* executionContextPtrRef = builder.GetExecutionContextPtr();

        builder.CreateCall3(
            builder.Module->GetRoutine("AllocateRow"),
            executionContextPtrRef,
            builder.getInt32(keySize),
            newRowPtr);

        Value* newRowRef = builder.CreateLoad(newRowPtr);

        std::vector<EValueType> keyTypes;
        for (int index = 0; index < keySize; ++index) {
            auto id = index;
            auto value = codegenArgs[index](builder, row);
            keyTypes.push_back(value.GetStaticType());        
            value.StoreToRow(builder, newRowRef, index, id);
        }

        Value* result = builder.CreateCall4(
            builder.Module->GetRoutine("IsRowInArray"),
            executionContextPtrRef,
            CodegenRowComparerFunction(keyTypes, *builder.Module),
            newRowRef,
            builder.getInt32(arrayIndex));

        return TCGValue::CreateFromValue(
            builder,
            builder.getInt16(static_cast<ui16>(EValueType::Boolean)),
            nullptr,
            result,
            EValueType::Boolean);
    };
}

TCodegenExpression MakeCodegenExpr(
    const TConstExpressionPtr& expr,
    const TCGBinding& binding,
    const TTableSchema& schema)
{
    if (auto literalExpr = expr->As<TLiteralExpression>()) {
        auto it = binding.NodeToConstantIndex.find(literalExpr);
        YCHECK(it != binding.NodeToConstantIndex.end());
        auto index = it->second;
        return [index, literalExpr] (TCGContext& builder, Value* row) {
            return TCGValue::CreateFromRow(
                builder,
                builder.GetConstantsRows(),
                index,
                literalExpr->Type,
                "literal." + Twine(index))
                .Steal();
        };
    } else if (auto referenceExpr = expr->As<TReferenceExpression>()) {
        auto column = referenceExpr->ColumnName;
        return [
            referenceExpr,
            column,
            index = schema.GetColumnIndexOrThrow(column)            
        ] (TCGContext& builder, Value* row) {
            return TCGValue::CreateFromRow(
            builder,
            row,
            index,
            referenceExpr->Type,
            "reference." + Twine(column.c_str()));
        };
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        std::vector<TCodegenExpression> codegenArgs;
        for (const auto& argument : functionExpr->Arguments) {
            codegenArgs.push_back(MakeCodegenExpr(argument, binding, schema));
        }

        return MakeCodegenFunctionExpr(
            functionExpr->FunctionName,
            std::move(codegenArgs),
            functionExpr->Type,
            "{" + functionExpr->GetName() + "}");
    } else if (auto unaryOpExpr = expr->As<TUnaryOpExpression>()) {
        return MakeCodegenUnaryOpExpr(
            unaryOpExpr->Opcode,
            MakeCodegenExpr(unaryOpExpr->Operand, binding, schema),
            unaryOpExpr->Type,
            "{" + unaryOpExpr->GetName() + "}");
    } else if (auto binaryOpExpr = expr->As<TBinaryOpExpression>()) {
        return MakeCodegenBinaryOpExpr(
            binaryOpExpr->Opcode,
            MakeCodegenExpr(binaryOpExpr->Lhs, binding, schema),
            MakeCodegenExpr(binaryOpExpr->Rhs, binding, schema),
            binaryOpExpr->Type,
            "{" + binaryOpExpr->GetName() + "}");
    } else if (auto inOpExpr = expr->As<TInOpExpression>()) {
        std::vector<TCodegenExpression> codegenArgs;
        for (const auto& argument : inOpExpr->Arguments) {
            codegenArgs.push_back(MakeCodegenExpr(argument, binding, schema));
        }

        auto it = binding.NodeToRows.find(inOpExpr);
        YCHECK(it != binding.NodeToRows.end());

        return MakeCodegenInOpExpr(codegenArgs, it->second);
    } else {
        YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////
// Operators
//

void CodegenScanOp(
    TCGContext& builder,
    const TCodegenConsumer& codegenConsumer)
{
    auto module = builder.Module->GetModule();

    // See ScanOpHelper.
    Function* function = Function::Create(
        TypeBuilder<void(void**, TRow*, int, char*), false>::get(builder.getContext()),
        Function::ExternalLinkage,
        "ScanOpInner",
        module);

    auto args = function->arg_begin();
    Value* closure = args; closure->setName("closure");
    Value* rows = ++args; rows->setName("rows");
    Value* size = ++args; size->setName("size");
    Value* stopFlag = ++args; stopFlag->setName("stopFlag");
    YCHECK(++args == function->arg_end());

    TCGContext innerBuilder(
        function,
        &builder,
        closure);

    CodegenForEachRow(innerBuilder, rows, size, stopFlag, codegenConsumer);

    innerBuilder.CreateRetVoid();

    int dataSplitsIndex = 0;

    builder.CreateCall4(
        builder.Module->GetRoutine("ScanOpHelper"),
        builder.GetExecutionContextPtr(),
        builder.getInt32(dataSplitsIndex),
        innerBuilder.GetClosure(),
        function);
}

TCodegenSource MakeCodegenJoinOp(
    std::vector<Stroka> joinColumns,
    TTableSchema sourceTableSchema,
    TCodegenSource codegenSource)
{
    return [
        MOVE(joinColumns),
        MOVE(sourceTableSchema),
        codegenSource = std::move(codegenSource)
    ] (TCGContext& builder, const TCodegenConsumer& codegenConsumer) {
        auto module = builder.Module->GetModule();

        // See JoinOpHelper.
        Function* collectRows = Function::Create(
            TypeBuilder<void(void**, void*, void*, void*), false>::get(builder.getContext()),
            Function::ExternalLinkage,
            "CollectRows",
            module);

        auto collectRowsArgs = collectRows->arg_begin();
        Value* closure = collectRowsArgs; closure->setName("closure");
        Value* keys = ++collectRowsArgs; keys->setName("keys");
        Value* keysLookup = ++collectRowsArgs; keysLookup->setName("keysLookup");
        Value* allRows = ++collectRowsArgs; allRows->setName("allRows");
        YCHECK(++collectRowsArgs == collectRows->arg_end());

        TCGContext collectBuilder(
            collectRows,
            &builder,
            closure);

        int joinKeySize = joinColumns.size();

        Value* newRowPtr = collectBuilder.CreateAlloca(TypeBuilder<TRow, false>::get(builder.getContext()));

        collectBuilder.CreateCall3(
            builder.Module->GetRoutine("AllocatePermanentRow"),
            collectBuilder.GetExecutionContextPtr(),
            builder.getInt32(joinKeySize),
            newRowPtr);

        codegenSource(
            collectBuilder,
            [&] (TCGContext& builder, Value* row) {
                Value* executionContextPtrRef = builder.GetExecutionContextPtr();
                Value* keysRef = builder.ViaClosure(keys);
                Value* allRowsRef = builder.ViaClosure(allRows);
                Value* keysLookupRef = builder.ViaClosure(keysLookup);
                Value* newRowPtrRef = builder.ViaClosure(newRowPtr);
                Value* newRowRef = builder.CreateLoad(newRowPtrRef);

                builder.CreateCall3(
                    builder.Module->GetRoutine("SaveJoinRow"),
                    executionContextPtrRef,
                    allRowsRef,
                    row);

                for (int index = 0; index < joinKeySize; ++index) {
                    auto id = index;

                    auto columnName = joinColumns[index];
                    auto column = sourceTableSchema.GetColumnOrThrow(columnName);

                    auto columnIndex = sourceTableSchema.GetColumnIndexOrThrow(columnName);
                    TCGValue::CreateFromRow(
                        builder,
                        row,
                        columnIndex,
                        column.Type,
                        "reference." + Twine(columnName.c_str()))
                        .StoreToRow(builder, newRowRef, index, id);                
                }

                // Add row to rows and lookup;

                builder.CreateCall5(
                    builder.Module->GetRoutine("InsertJoinRow"),
                    executionContextPtrRef,
                    keysLookupRef,
                    keysRef,
                    newRowPtrRef,
                    builder.getInt32(joinKeySize));

            });

        collectBuilder.CreateRetVoid();

        // See JoinOpHelper.
        Function* consumeJoinedRows = Function::Create(
            TypeBuilder<void(void**, void*, char*), false>::get(builder.getContext()),
            Function::ExternalLinkage,
            "JoinOpInner",
            module);

        auto consumeJoinedRowsArgs = consumeJoinedRows->arg_begin();
        Value* consumeClosure = consumeJoinedRowsArgs; consumeClosure->setName("consumeClosure");
        Value* joinedRows = ++consumeJoinedRowsArgs; joinedRows->setName("joinedRows");
        Value* stopFlag = ++consumeJoinedRowsArgs; stopFlag->setName("stopFlag");
        YCHECK(++consumeJoinedRowsArgs == consumeJoinedRows->arg_end());

        TCGContext consumeBuilder(
            consumeJoinedRows,
            &builder,
            consumeClosure);

        CodegenForEachRow(
            consumeBuilder,
            consumeBuilder.CreateCall(builder.Module->GetRoutine("GetRowsData"), joinedRows),
            consumeBuilder.CreateCall(builder.Module->GetRoutine("GetRowsSize"), joinedRows),
            stopFlag,
            codegenConsumer);

        consumeBuilder.CreateRetVoid();
 
        std::vector<EValueType> keyTypes;
        for (int index = 0; index < joinColumns.size(); ++index) {
            keyTypes.push_back(sourceTableSchema.FindColumn(joinColumns[index])->Type);
        }

        builder.CreateCallWithArgs(
            builder.Module->GetRoutine("JoinOpHelper"),
            {
                builder.GetExecutionContextPtr(),
                CodegenGroupHasherFunction(keyTypes, *builder.Module),
                CodegenGroupComparerFunction(keyTypes, *builder.Module),

                collectBuilder.GetClosure(),
                collectRows,

                consumeBuilder.GetClosure(),
                consumeJoinedRows
            });
    };
}

TCodegenSource MakeCodegenFilterOp(
    TCodegenExpression codegenPredicate,
    TCodegenSource codegenSource)
{
    return [
        MOVE(codegenPredicate), 
        codegenSource = std::move(codegenSource)
    ] (TCGContext& builder, const TCodegenConsumer& codegenConsumer) {
        codegenSource(
            builder,
            [&] (TCGContext& builder, Value* row) {
                auto predicateResult = codegenPredicate(builder, row);

                Value* result = builder.CreateZExtOrBitCast(
                    predicateResult.GetData(),
                    builder.getInt64Ty());

                auto* ifBB = builder.CreateBBHere("if");
                auto* endifBB = builder.CreateBBHere("endif");

                builder.CreateCondBr(
                    builder.CreateICmpNE(result, builder.getInt64(0)),
                    ifBB,
                    endifBB);

                builder.SetInsertPoint(ifBB);
                codegenConsumer(builder, row);
                builder.CreateBr(endifBB);

                builder.SetInsertPoint(endifBB);
            });
    };
}

TCodegenSource MakeCodegenProjectOp(
    std::vector<TCodegenExpression> codegenArgs,
    TCodegenSource codegenSource)
{
    return [
        MOVE(codegenArgs),
        codegenSource = std::move(codegenSource)        
    ] (TCGContext& builder, const TCodegenConsumer& codegenConsumer) {
        int projectionCount = codegenArgs.size();

        codegenSource(
            builder,
            [&] (TCGContext& builder, Value* row) {
                Value* newRowPtr = builder.CreateAlloca(TypeBuilder<TRow, false>::get(builder.getContext()));

                builder.CreateCall3(
                    builder.Module->GetRoutine("AllocateRow"),
                    builder.GetExecutionContextPtr(),
                    builder.getInt32(projectionCount),
                    newRowPtr);

                Value* newRow = builder.CreateLoad(newRowPtr);

                for (int index = 0; index < projectionCount; ++index) {
                    auto id = index;
                
                    codegenArgs[index](builder, row)
                        .StoreToRow(builder, newRow, index, id);
                }

                codegenConsumer(builder, newRow);
            });
    };
}

TCodegenSource MakeCodegenGroupOp(
    std::vector<TCodegenExpression> codegenGroupExprs,
    std::vector<TCodegenExpression> codegenAggregateExprs,
    std::vector<TCodegenAggregate> codegenAggregates,
    TCodegenSource codegenSource)
{
    return [
        MOVE(codegenGroupExprs),
        MOVE(codegenAggregateExprs),
        MOVE(codegenAggregates),
        codegenSource = std::move(codegenSource)
    ] (TCGContext& builder, const TCodegenConsumer& codegenConsumer) {
        auto module = builder.Module->GetModule();

        // See GroupOpHelper.
        Function* collect = Function::Create(
            TypeBuilder<void(void**, void*, void*), false>::get(builder.getContext()),
            Function::ExternalLinkage,
            "CollectGroups",
            module);

        auto collectArgs = collect->arg_begin();
        Value* collectClosure = collectArgs; collectClosure->setName("closure");
        Value* groupedRows = ++collectArgs; groupedRows->setName("groupedRows");
        Value* lookup = ++collectArgs; lookup->setName("lookup");
        YCHECK(++collectArgs == collect->arg_end());

        TCGContext collectBuilder(
            collect,
            &builder,
            collectClosure);

        int keySize = codegenGroupExprs.size();
        int aggregatesCount = codegenAggregateExprs.size();

        Value* newRowPtr = collectBuilder.CreateAlloca(TypeBuilder<TRow, false>::get(builder.getContext()));

        collectBuilder.CreateCall3(
            builder.Module->GetRoutine("AllocatePermanentRow"),
            collectBuilder.GetExecutionContextPtr(),
            builder.getInt32(keySize + aggregatesCount),
            newRowPtr);

        std::vector<EValueType> keyTypes;

        codegenSource(
            collectBuilder,
            [&] (TCGContext& builder, Value* row) {
                Value* executionContextPtrRef = builder.GetExecutionContextPtr();
                Value* groupedRowsRef = builder.ViaClosure(groupedRows);
                Value* lookupRef = builder.ViaClosure(lookup);
                Value* newRowPtrRef = builder.ViaClosure(newRowPtr);
                Value* newRowRef = builder.CreateLoad(newRowPtrRef);

                for (int index = 0; index < keySize; ++index) {
                    auto id = index;

                    auto value = codegenGroupExprs[index](builder, row);
                    keyTypes.push_back(value.GetStaticType());
                    value.StoreToRow(builder, newRowRef, index, id);
                }

                std::vector<EValueType> aggregateTypes;
                for (int index = 0; index < aggregatesCount; ++index) {
                    auto id = keySize + index;

                    codegenAggregateExprs[index](builder, row)
                        .StoreToRow(builder, newRowRef, keySize + index, id);
                }

                Value* foundRowPtr = builder.CreateCall5(
                    builder.Module->GetRoutine("InsertGroupRow"),
                    executionContextPtrRef,
                    lookupRef,
                    groupedRowsRef,
                    newRowPtrRef,
                    builder.getInt32(keySize + aggregatesCount));

                Value* condition = builder.CreateICmpNE(
                    foundRowPtr,
                    llvm::ConstantPointerNull::get(newRowRef->getType()->getPointerTo()));

                CodegenIf<TCGContext>(
                    builder,
                    condition,
                    [&] (TCGContext& builder) {
                        Value* foundRow = builder.CreateLoad(foundRowPtr);
                        for (int index = 0; index < aggregatesCount; ++index) {
                            codegenAggregates[index](builder, foundRow, newRowRef);
                        }
                    },
                    [&] (TCGContext& builder) { });
            });

        collectBuilder.CreateRetVoid();

        Function* consume = Function::Create(
            TypeBuilder<void(void**, void*, char*), false>::get(builder.getContext()),
            Function::ExternalLinkage,
            "Consume",
            module);

        auto consumeArgs = consume->arg_begin();
        Value* consumeClosure = consumeArgs; consumeClosure->setName("closure");
        Value* finalGroupedRows = ++consumeArgs; finalGroupedRows->setName("finalGroupedRows");
        Value* stopFlag = ++consumeArgs; stopFlag->setName("stopFlag");
        YCHECK(++consumeArgs == consume->arg_end());

        TCGContext consumeBuilder(
            consume,
            &builder,
            consumeClosure);

        CodegenForEachRow(
            consumeBuilder,
            consumeBuilder.CreateCall(builder.Module->GetRoutine("GetRowsData"), finalGroupedRows),
            consumeBuilder.CreateCall(builder.Module->GetRoutine("GetRowsSize"), finalGroupedRows),
            stopFlag,
            codegenConsumer);

        consumeBuilder.CreateRetVoid();

        builder.CreateCallWithArgs(
            builder.Module->GetRoutine("GroupOpHelper"),
            {
                builder.GetExecutionContextPtr(),
                CodegenGroupHasherFunction(keyTypes, *builder.Module),
                CodegenGroupComparerFunction(keyTypes, *builder.Module),

                collectBuilder.GetClosure(),
                collect,

                consumeBuilder.GetClosure(),
                consume,
            });

    };
}

TCGQueryCallback CodegenEvaluate(
    const TConstQueryPtr& query,
    const TCGBinding& binding)
{
    auto module = TCGModule::Create(GetQueryRoutineRegistry());

    TTableSchema sourceSchema = query->TableSchema;
    
    TCodegenSource codegenSource = &CodegenScanOp;
    
    if (auto joinClause = query->JoinClause.GetPtr()) {
        if (binding.SelfJoinPredicate) {
            codegenSource = MakeCodegenFilterOp(MakeCodegenExpr(binding.SelfJoinPredicate, binding, joinClause->SelfTableSchema), std::move(codegenSource));
        }

        codegenSource = MakeCodegenJoinOp(joinClause->JoinColumns, joinClause->SelfTableSchema, std::move(codegenSource));
    }

    if (auto predicate = query->Predicate.Get()) {
        codegenSource = MakeCodegenFilterOp(MakeCodegenExpr(predicate, binding, sourceSchema), std::move(codegenSource));
    }

    if (auto groupClause = query->GroupClause.GetPtr()) {
        std::vector<TCodegenExpression> codegenGroupExprs;
        std::vector<TCodegenExpression> codegenAggregateExprs;
        std::vector<TCodegenAggregate> codegenAggregates;

        for (const auto& groupItem : groupClause->GroupItems) {
            codegenGroupExprs.push_back(MakeCodegenExpr(groupItem.Expression, binding, sourceSchema));
        }

        int aggregateIndex = codegenGroupExprs.size();

        for (const auto& aggregateItem : groupClause->AggregateItems) {
            codegenAggregateExprs.push_back(MakeCodegenExpr(aggregateItem.Expression, binding, sourceSchema));

            codegenAggregates.push_back([aggregateItem, aggregateIndex] (TCGContext& builder, Value* foundRow, Value* newRowRef) {
                CodegenAggregateFunction(
                    builder,
                    foundRow,
                    newRowRef,
                    aggregateItem.AggregateFunction,
                    aggregateIndex,
                    aggregateIndex,
                    aggregateItem.Expression->Type,
                    aggregateItem.Name.c_str());
            });
            ++aggregateIndex;
        }

        codegenSource = MakeCodegenGroupOp(
            std::move(codegenGroupExprs),
            std::move(codegenAggregateExprs),
            std::move(codegenAggregates),
            std::move(codegenSource));

        sourceSchema = query->GroupClause->GetTableSchema();
    }

    if (auto projectClause = query->ProjectClause.GetPtr()) {
        std::vector<TCodegenExpression> codegenProjectExprs;

        for (const auto& item : projectClause->Projections) {
            codegenProjectExprs.push_back(MakeCodegenExpr(item.Expression, binding, sourceSchema));
        }

        codegenSource = MakeCodegenProjectOp(std::move(codegenProjectExprs), std::move(codegenSource));
        sourceSchema = query->ProjectClause->GetTableSchema();
    }

    auto& context = module->GetContext();

    auto entryFunctionName = Stroka("Evaluate");

    Function* function = Function::Create(
        TypeBuilder<TCGQuerySignature, false>::get(context),
        Function::ExternalLinkage,
        entryFunctionName.c_str(),
        module->GetModule());

    auto args = function->arg_begin();
    Value* constants = args; constants->setName("constants");
    Value* executionContextPtr = ++args; executionContextPtr->setName("passedFragmentParamsPtr");
    YCHECK(++args == function->arg_end());

    TCGContext builder(module, constants, executionContextPtr, BasicBlock::Create(context, "entry", function));

    codegenSource(
        builder,
        [&] (TCGContext& builder, Value* row) {
            builder.CreateCall2(module->GetRoutine("WriteRow"), row, builder.GetExecutionContextPtr());
        });

    builder.CreateRetVoid();

    return module->GetCompiledFunction<TCGQuerySignature>(entryFunctionName);
}

TCGExpressionCallback CodegenExpression(
    const TConstExpressionPtr& expression,
    const TTableSchema& tableSchema,
    const TCGBinding& binding)
{
    auto module = TCGModule::Create(GetQueryRoutineRegistry());
    auto& context = module->GetContext();

    auto entryFunctionName = Stroka("EvaluateExpression");

    Function* function = Function::Create(
        TypeBuilder<TCGExpressionSignature, false>::get(context),
        Function::ExternalLinkage,
        entryFunctionName.c_str(),
        module->GetModule());

    auto args = function->arg_begin();
    Value* resultPtr = args; resultPtr->setName("resultPtr");
    Value* inputRow = ++args; inputRow->setName("inputRow");
    Value* constants = ++args; constants->setName("constants");
    Value* executionContextPtr = ++args; executionContextPtr->setName("passedFragmentParamsPtr");
    YCHECK(++args == function->arg_end());

    TCGContext builder(module, constants, executionContextPtr, BasicBlock::Create(context, "entry", function));

    auto result = MakeCodegenExpr(expression, binding, tableSchema)(builder, inputRow);

    result.StoreToValue(builder, resultPtr, 0, "writeResult");

    builder.CreateRetVoid();

    return module->GetCompiledFunction<TCGExpressionSignature>(entryFunctionName);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

