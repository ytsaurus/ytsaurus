#include "stdafx.h"
#include "cg_fragment_compiler.h"

#include "private.h"
#include "helpers.h"

#include "plan_fragment.h"

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

typedef std::function<void(TCGIRBuilder& builder, Value* row)> TCodegenConsumer;
typedef std::function<void(TCGIRBuilder& builder, const TCodegenConsumer& codegenConsumer)> TCodegenSource;

static Value* CodegenValuesPtrFromRow(TCGIRBuilder&, Value*);

typedef TypeBuilder<TValue, false> TTypeBuilder;
typedef TypeBuilder<TValueData, false> TDataTypeBuilder;

class TCGValue
{
private:
    // TODO: move builder from here
    TCGIRBuilder& Builder_;
    Value* Type_;
    Value* Length_;
    Value* Data_;
    EValueType StaticType_;
    std::string Name_;

    TCGValue(TCGIRBuilder& builder, Value* type, Value* length, Value* data, EValueType staticType, Twine name)
        : Builder_(builder)
        , Type_(type)
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

        if (Type_) {
            YCHECK(Type_->getType() == TTypeBuilder::TType::get(Builder_.getContext()));
        }
        if (Length_) {
            YCHECK(Length_->getType() == TTypeBuilder::TLength::get(Builder_.getContext()));
        }
        if (Data_) {
            YCHECK(Data_->getType() == TTypeBuilder::TData::get(Builder_.getContext()));
        }
    }

public:
    TCGValue(const TCGValue& other) = default;

    TCGValue(TCGValue&& other)
        : Builder_(other.Builder_)
        , Type_(other.Type_)
        , Length_(other.Length_)
        , Data_(other.Data_)
        , StaticType_(other.StaticType_)
        , Name_(std::move(other.Name_))
    {
        other.Reset();
    }

    TCGValue& operator=(TCGValue&& other)
    {
        YCHECK(&Builder_ == &other.Builder_);

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
        if (data) {
            auto targetType = TDataTypeBuilder::get(builder.getContext());

            if (data->getType()->isPointerTy()) {
                data = builder.CreatePtrToInt(data, targetType);
            } else if (data->getType()->isFloatingPointTy()) {
                data = builder.CreateBitCast(data, targetType);
            } else {
                data = builder.CreateIntCast(data, targetType, false);
            }
        }

        return TCGValue(builder, type, length, data, staticType, name);
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
        return TCGValue(builder, type, length, data, staticType, name);
    }

    static TCGValue CreateNull(
        TCGIRBuilder& builder,
        EValueType staticType,
        Twine name = Twine())
    {
        return TCGValue(
            builder,
            builder.getInt16(EValueType::Null),
            llvm::UndefValue::get(TTypeBuilder::TLength::get(builder.getContext())),
            llvm::UndefValue::get(TTypeBuilder::TData::get(builder.getContext())),
            staticType,
            name);
    }

    void StoreToRow(Value* row, int index, ui16 id)
    {
        auto name = row->getName();
        auto nameTwine =
            (name.empty() ? Twine::createNull() : Twine(name).concat(".")) +
            Twine(".at.") +
            Twine(index);

        auto valuePtr = Builder_.CreateConstInBoundsGEP1_32(
            CodegenValuesPtrFromRow(Builder_, row),
            index,
            nameTwine);

        Builder_.CreateStore(
            Builder_.getInt16(id),
            Builder_.CreateStructGEP(valuePtr, TTypeBuilder::Id, nameTwine + ".idPtr"));

        if (Type_) {
            Builder_.CreateStore(
                Type_,
                Builder_.CreateStructGEP(valuePtr, TTypeBuilder::Type, nameTwine + ".typePtr"));
        }
        if (Length_) {
            Builder_.CreateStore(
                Length_,
                Builder_.CreateStructGEP(valuePtr, TTypeBuilder::Length, nameTwine + ".lengthPtr"));
        }
        if (Data_) {
            Builder_.CreateStore(
                Data_,
                Builder_.CreateStructGEP(valuePtr, TTypeBuilder::Data, nameTwine + ".dataPtr"));
        }
    }

    Value* IsNull()
    {
        // A little bit of manual constant folding.
        if (Type_ && llvm::isa<ConstantInt>(Type_)) {
            auto* constantType = llvm::cast<ConstantInt>(Type_);
            if (constantType->getZExtValue() == EValueType::Null) {
                return Builder_.getFalse();
            }
        }
        return Builder_.CreateICmpEQ(
            Type_,
            Builder_.getInt16(EValueType::Null),
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
        Type* targetType;

        switch (StaticType_) {
            case EValueType::Boolean:
                targetType = TDataTypeBuilder::TBoolean::get(Builder_.getContext());
                break;
            case EValueType::Int64:
                targetType = TDataTypeBuilder::TInt64::get(Builder_.getContext());
                break;
            case EValueType::Uint64:
                targetType = TDataTypeBuilder::TUint64::get(Builder_.getContext());
                break;
            case EValueType::Double:
                targetType = TDataTypeBuilder::TDouble::get(Builder_.getContext());
                break;
            case EValueType::String:
                targetType = TDataTypeBuilder::TString::get(Builder_.getContext());
                break;
            default:
                YUNREACHABLE();
        }

        if (targetType->isPointerTy()) {
            return Builder_.CreateIntToPtr(Data_,
                targetType,
                Twine(Name_) + ".data");
        } else if (targetType->isFloatingPointTy()) {
            return Builder_.CreateBitCast(Data_,
                targetType,
                Twine(Name_) + ".data");        
        } else {
            return Builder_.CreateIntCast(Data_,
                targetType,
                false,
                Twine(Name_) + ".data");
        }
    }

    TCGValue Cast(EValueType dest, bool bitcast = false)
    {
        if (dest == StaticType_) {
            return *this;
        }

        auto value = GetData();

        Value* result;
        if (dest == EValueType::Int64) {
            auto destType = TDataTypeBuilder::TUint64::get(Builder_.getContext());
            if (bitcast) {
                result = Builder_.CreateBitCast(value, destType);
            } else if (StaticType_ == EValueType::Uint64) {
                result = Builder_.CreateIntCast(value, destType, false);
            } else if (StaticType_ == EValueType::Double) {
                result = Builder_.CreateFPToSI(value, destType);
            } else {
                YUNREACHABLE();
            }
        } else if (dest == EValueType::Uint64) {
            auto destType = TDataTypeBuilder::TUint64::get(Builder_.getContext());
            if (bitcast) {
                result = Builder_.CreateBitCast(value, destType);
            } if (StaticType_ == EValueType::Int64) {
                result = Builder_.CreateIntCast(value, destType, true);
            } else if (StaticType_ == EValueType::Double) {
                result = Builder_.CreateFPToUI(value, destType);
            } else {
                YUNREACHABLE();
            }
        } else if (dest == EValueType::Double) {
            auto destType = TDataTypeBuilder::TDouble::get(Builder_.getContext());
            if (bitcast) {
                result = Builder_.CreateBitCast(value, destType);
            } if (StaticType_ == EValueType::Uint64) {
                result = Builder_.CreateUIToFP(value, destType);
            } else if (StaticType_ == EValueType::Int64) {
                result = Builder_.CreateSIToFP(value, destType);
            } else {
                YUNREACHABLE();
            }
        } else {
            YUNREACHABLE();
        }

        return CreateFromValue(Builder_, GetType(), GetLength(), result, dest);
    }
};

typedef std::function<Value* (TCGIRBuilder& builder)> TCodegenBlock;
typedef std::function<TCGValue(TCGIRBuilder& builder)> TCodegenValueBlock;
typedef std::function<void(TCGIRBuilder& builder)> TCodegenVoidBlock;

class TCGContext
{
public:
    static TCGQueryCallback CodegenEvaluate(
        const TConstQueryPtr& query,
        const TCGBinding& binding);

private:
    const TCGModulePtr Module_;
    const TCGBinding& Binding_;
    Value* ConstantsRow_;
    Value* ExecutionContextPtr_;

    TCGContext(
        const TCGModulePtr module,
        const TCGBinding& binding,
        Value* constantsRow,
        Value* executionContextPtr)
        : Module_(std::move(module))
        , Binding_(binding)
        , ConstantsRow_(constantsRow)
        , ExecutionContextPtr_(executionContextPtr)
    { }

    Value* GetConstantsRows(TCGIRBuilder& builder) const
    {
        return builder.ViaClosure(ConstantsRow_, "constantsRow");
    }

    Value* GetExecutionContextPtr(TCGIRBuilder& builder) const
    {
        return builder.ViaClosure(ExecutionContextPtr_, "executionContextPtr");
    }

    TCGValue DoCodegenFunctionExpr(
        TCGIRBuilder& builder,
        Stroka functionName,
        const std::vector<TCodegenValueBlock>& codegenArgs,
        EValueType type,
        Stroka name);

    TCGValue DoCodegenBinaryOpExpr(
        TCGIRBuilder& builder,
        EBinaryOp opcode,
        const TCodegenValueBlock& codegenLhs,
        const TCodegenValueBlock& codegenRhs,
        EValueType type,
        Stroka name);

    TCGValue DoCodegenInOpExpr(
        TCGIRBuilder& builder,
        const std::vector<TCodegenValueBlock>& codegenArgs,
        int arrayIndex);

    Function* CodegenGroupComparerFunction(
        const std::vector<EValueType>& types);

    Function* CodegenGroupHasherFunction(
        const std::vector<EValueType>& types);

    TCGValue CodegenExpr(
        TCGIRBuilder& builder,
        const TConstExpressionPtr& expr,
        const TTableSchema& schema,
        Value* row);

    TCGValue CodegenFunctionExpr(
        TCGIRBuilder& builder,
        const TFunctionExpression* expr,
        const TTableSchema& schema,
        Value* row);

    TCGValue CodegenBinaryOpExpr(
        TCGIRBuilder& builder,
        const TBinaryOpExpression* expr,
        const TTableSchema& schema,
        Value* row);

    TCGValue CodegenInOpExpr(
        TCGIRBuilder& builder,
        const TInOpExpression* expr,
        const TTableSchema& schema,
        Value* row);

    void CodegenScanOp(
        TCGIRBuilder& builder,
        const TCodegenConsumer& codegenConsumer);

    void CodegenFilterOp(
        TCGIRBuilder& builder,
        const TConstExpressionPtr& predicate,
        const TTableSchema& sourceTableSchema,
        const TCodegenSource& codegenSource,
        const TCodegenConsumer& codegenConsumer);

    void CodegenProjectOp(
        TCGIRBuilder& builder,
        const TProjectClause& op,
        const TTableSchema& sourceTableSchema,
        const TCodegenSource& codegenSource,
        const TCodegenConsumer& codegenConsumer);

    void CodegenGroupOp(
        TCGIRBuilder& builder,
        const TGroupClause& op,
        const TTableSchema& sourceTableSchema,
        const TCodegenSource& codegenSource,
        const TCodegenConsumer& codegenConsumer);
};

void CodegenIf(
    TCGIRBuilder& builder,
    const TCodegenBlock& conditionCodegen,
    const TCodegenVoidBlock& thenCodegen,
    const TCodegenVoidBlock& elseCodegen)
{
    auto* conditionBB = builder.CreateBBHere("condition");
    auto* thenBB = builder.CreateBBHere("then");
    auto* elseBB = builder.CreateBBHere("else");
    auto* endBB = builder.CreateBBHere("end");

    builder.CreateBr(conditionBB);

    builder.SetInsertPoint(conditionBB);
    builder.CreateCondBr(conditionCodegen(builder), thenBB, elseBB);
    conditionBB = builder.GetInsertBlock();

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

TCGValue CodegenIfValue(
    TCGIRBuilder& builder,
    const TCodegenBlock& conditionCodegen,
    const TCodegenValueBlock& thenCodegen,
    const TCodegenValueBlock& elseCodegen,
    Twine name = Twine())
{
    auto* conditionBB = builder.CreateBBHere("condition");
    auto* thenBB = builder.CreateBBHere("then");
    auto* elseBB = builder.CreateBBHere("else");
    auto* endBB = builder.CreateBBHere("end");

    builder.CreateBr(conditionBB);

    builder.SetInsertPoint(conditionBB);
    builder.CreateCondBr(conditionCodegen(builder), thenBB, elseBB);
    conditionBB = builder.GetInsertBlock();

    builder.SetInsertPoint(thenBB);
    auto thenValue = thenCodegen(builder);
    Value* thenType = thenValue.GetType();
    Value* thenLength = thenValue.GetLength();
    Value* thenData = thenValue.GetData();
    builder.CreateBr(endBB);
    thenBB = builder.GetInsertBlock();

    builder.SetInsertPoint(elseBB);
    auto elseValue = elseCodegen(builder);
    Value* elseType = elseValue.GetType();
    Value* elseLength = elseValue.GetLength();
    Value* elseData = elseValue.GetData();
    builder.CreateBr(endBB);
    elseBB = builder.GetInsertBlock();

    builder.SetInsertPoint(endBB);
    PHINode* phiType = builder.CreatePHI(builder.getInt16Ty(), 2, name + ".phiType");
    //phiType->addIncoming(builder.getInt16(EValueType::Null), conditionBB);
    phiType->addIncoming(thenType, thenBB);
    phiType->addIncoming(elseType, elseBB);

    YCHECK(thenValue.GetStaticType() == elseValue.GetStaticType());
    EValueType type = thenValue.GetStaticType();
    YCHECK(thenData->getType() == elseData->getType());

    PHINode* phiData = builder.CreatePHI(thenData->getType(), 2, name + ".phiData");
    //phiData->addIncoming(llvm::UndefValue::get(thenData->getType()), conditionBB);
    phiData->addIncoming(thenData, thenBB);
    phiData->addIncoming(elseData, elseBB);

    PHINode* phiLength = nullptr;
    if (type == EValueType::String) {
        YCHECK(thenLength->getType() == elseLength->getType());

        phiLength = builder.CreatePHI(thenLength->getType(), 2, name + ".phiLength");
        //phiLength->addIncoming(llvm::UndefValue::get(thenLength->getType()), conditionBB);
        phiLength->addIncoming(thenLength, thenBB);
        phiLength->addIncoming(elseLength, elseBB);
    }

    return TCGValue::CreateFromValue(builder, phiType, phiLength, phiData, type, name);
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
    TCGIRBuilder& builder,
    Value* aggregateRow,
    Value* newRow,
    EAggregateFunctions aggregateFunction,
    int index,
    ui16 id,
    EValueType type,
    Twine name = Twine())
{
    auto newValue = TCGValue::CreateFromRow(builder, newRow, index, type, name + ".new");

    CodegenIf(builder, [&] (TCGIRBuilder& builder) {
        return newValue.IsNull();
    }, [&] (TCGIRBuilder& builder) {

    }, [&] (TCGIRBuilder& builder) {
        auto aggregateValue = TCGValue::CreateFromRow(builder, aggregateRow, index, type, name + ".aggregate");

        CodegenIfValue(builder, [&] (TCGIRBuilder& builder) {
            return aggregateValue.IsNull();
        }, [&] (TCGIRBuilder& builder) {
            return newValue;
        }, [&] (TCGIRBuilder& builder) {
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

            return TCGValue::CreateFromValue(builder, builder.getInt16(type), nullptr, resultData, type, name);
        }).StoreToRow(aggregateRow, index, id);
    });
}

void CodegenForEachRow(
    TCGIRBuilder& builder,
    Value* rows,
    Value* size,
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
    builder.CreateCondBr(builder.CreateICmpNE(index, size), loopBB, endloopBB);

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

TCGValue TCGContext::DoCodegenFunctionExpr(
    TCGIRBuilder& builder,
    Stroka functionName,
    const std::vector<TCodegenValueBlock>& codegenArgs,
    EValueType type,
    Stroka name)
{
    functionName.to_lower();
    auto nameTwine = Twine(name.c_str());

    if (functionName == "if") {
        YCHECK(codegenArgs.size() == 3);
        auto condition = codegenArgs[0](builder);
        YCHECK(condition.GetStaticType() == EValueType::Boolean);

        return CodegenIfValue(builder, [&] (TCGIRBuilder& builder) {
            return condition.IsNull();
        }, [&] (TCGIRBuilder& builder) {
            return TCGValue::CreateNull(builder, type);
        }, [&] (TCGIRBuilder& builder) {
            return CodegenIfValue(builder, [&] (TCGIRBuilder& builder) {
                return builder.CreateICmpNE(
                    builder.CreateZExtOrBitCast(condition.GetData(), builder.getInt64Ty()),
                    builder.getInt64(0));            
            }, [&] (TCGIRBuilder& builder) {
                return codegenArgs[1](builder);
            }, [&] (TCGIRBuilder& builder) {
                return codegenArgs[2](builder);
            });
        }, nameTwine);
    } else if (functionName == "is_prefix") {
        YCHECK(codegenArgs.size() == 2);
        auto lhsValue = codegenArgs[0](builder);
        YCHECK(lhsValue.GetStaticType() == EValueType::String);
    
        return CodegenIfValue(builder, [&] (TCGIRBuilder& builder) {
            return lhsValue.IsNull();
        }, [&] (TCGIRBuilder& builder) {
            return TCGValue::CreateNull(builder, type);
        }, [&] (TCGIRBuilder& builder) {
            auto rhsValue = codegenArgs[1](builder);
            YCHECK(rhsValue.GetStaticType() == EValueType::String);

            return CodegenIfValue(builder, [&] (TCGIRBuilder& builder) {
                return rhsValue.IsNull();            
            }, [&] (TCGIRBuilder& builder) {
                return TCGValue::CreateNull(builder, type);
            }, [&] (TCGIRBuilder& builder) {
                Value* lhsData = lhsValue.GetData();
                Value* lhsLength = lhsValue.GetLength();
                Value* rhsData = rhsValue.GetData();
                Value* rhsLength = rhsValue.GetLength();

                Value* result = builder.CreateCall4(
                    Module_->GetRoutine("IsPrefix"),
                    lhsData, lhsLength, rhsData, rhsLength);

                return TCGValue::CreateFromValue(builder, builder.getInt16(type), nullptr, result, type);
            });
        }, nameTwine);
    } else if (functionName == "lower") {
        YCHECK(codegenArgs.size() == 1);
        auto argValue = codegenArgs[0](builder);
        YCHECK(argValue.GetStaticType() == EValueType::String);

        return CodegenIfValue(builder, [&] (TCGIRBuilder& builder) {
            return argValue.IsNull();
        }, [&] (TCGIRBuilder& builder) {
            return TCGValue::CreateNull(builder, type);
        }, [&] (TCGIRBuilder& builder) {
            Value* argData = argValue.GetData();
            Value* argLength = argValue.GetLength();

            Value* result = builder.CreateCall3(
                Module_->GetRoutine("ToLower"),
                GetExecutionContextPtr(builder),
                argData,
                argLength);

            return TCGValue::CreateFromValue(builder, builder.getInt16(type), argLength, result, type);
        }, nameTwine);
    } else if (functionName == "is_null") {
        YCHECK(codegenArgs.size() == 1);
        auto argValue = codegenArgs[0](builder);

        return TCGValue::CreateFromValue(
            builder,
            builder.getInt16(type),
            nullptr,            
            builder.CreateZExtOrBitCast(
                argValue.IsNull(),
                TDataTypeBuilder::TBoolean::get(builder.getContext())),
            type);
    } else if (functionName == "int64" || functionName == "uint64" || functionName == "double") {
        YCHECK(codegenArgs.size() == 1);
        return codegenArgs[0](builder).Cast(type);
    }

    YUNIMPLEMENTED();
}

TCGValue TCGContext::CodegenFunctionExpr(
    TCGIRBuilder& builder,
    const TFunctionExpression* expr,
    const TTableSchema& schema,
    Value* row)
{
    std::vector<TCodegenValueBlock> codegenArgs;
    for (const auto& argument : expr->Arguments) {
        codegenArgs.push_back([&] (TCGIRBuilder& builder) {
            return CodegenExpr(builder, argument, schema, row);
        });
    }
        
    auto name = "{" + expr->GetName() + "}";
    return DoCodegenFunctionExpr(builder, expr->FunctionName, codegenArgs, expr->Type, name);
}

TCGValue TCGContext::DoCodegenBinaryOpExpr(
    TCGIRBuilder& builder,
    EBinaryOp opcode,
    const TCodegenValueBlock& codegenLhs,
    const TCodegenValueBlock& codegenRhs,
    EValueType type,
    Stroka name)
{
    auto nameTwine = Twine(name.c_str());

    auto lhsValue = codegenLhs(builder);

    return CodegenIfValue(builder, [&] (TCGIRBuilder& builder) {
            return lhsValue.IsNull();
        }, [&] (TCGIRBuilder& builder) {
            return TCGValue::CreateNull(builder, type);
        }, [&] (TCGIRBuilder& builder) {
            auto rhsValue = codegenRhs(builder);

            return CodegenIfValue(builder, [&] (TCGIRBuilder& builder) {
                return rhsValue.IsNull();            
            }, [&] (TCGIRBuilder& builder) {
                return TCGValue::CreateNull(builder, type);
            }, [&] (TCGIRBuilder& builder) {
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

                        switch (opcode) {
                            case EBinaryOp::Equal:
                                evalData = builder.CreateCall4(
                                    Module_->GetRoutine("Equal"),
                                    lhsData, lhsLength, rhsData, rhsLength);
                                break;
                            case EBinaryOp::NotEqual:
                                evalData = builder.CreateCall4(
                                    Module_->GetRoutine("NotEqual"),
                                    lhsData, lhsLength, rhsData, rhsLength);
                                break;
                            case EBinaryOp::Less:
                                evalData = builder.CreateCall4(
                                    Module_->GetRoutine("LexicographicalCompare"),
                                    lhsData, lhsLength, rhsData, rhsLength);
                                break;
                            case EBinaryOp::Greater:
                                evalData = builder.CreateCall4(
                                    Module_->GetRoutine("LexicographicalCompare"),
                                    rhsData, rhsLength, lhsData, lhsLength);
                                break;
                            default:
                                YUNREACHABLE();
                        }
                        break;
                    }
                    default:
                        YUNREACHABLE();
                }

                #undef OP
                #undef CMP_OP

                return TCGValue::CreateFromValue(builder, builder.getInt16(type), nullptr, evalData, type);
            });
        }, nameTwine);
}

TCGValue TCGContext::CodegenBinaryOpExpr(
    TCGIRBuilder& builder,
    const TBinaryOpExpression* expr,
    const TTableSchema& schema,
    Value* row)
{
    auto name = "{" + expr->GetName() + "}";
    
    return DoCodegenBinaryOpExpr(builder, expr->Opcode, 
        [&] (TCGIRBuilder& builder) {
            return CodegenExpr(builder, expr->Lhs, schema, row);
        },
        [&] (TCGIRBuilder& builder) {
            return CodegenExpr(builder, expr->Rhs, schema, row);
        },
        expr->Type,
        name);
}

Function* TCGContext::CodegenGroupComparerFunction(
    const std::vector<EValueType>& types)
{
    auto module = Module_->GetModule();

    Function* function = Function::Create(
        TypeBuilder<char(TRow, TRow), false>::get(module->getContext()),
        Function::ExternalLinkage,
        "GroupComparer",
        module);

    auto args = function->arg_begin();
    Value* lhsRow = args; lhsRow->setName("lhsRow");
    Value* rhsRow = ++args; rhsRow->setName("rhsRow");
    YCHECK(++args == function->arg_end());

    TCGIRBuilder builder(BasicBlock::Create(module->getContext(), "entry", function));

    auto codegenEqualOp = [&] (size_t index) -> Value* {
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

        auto* conditionBB = builder.CreateBBHere("condition");
        auto* thenBB = builder.CreateBBHere("then");
        auto* elseBB = builder.CreateBBHere("else");
        auto* endBB = builder.CreateBBHere("end");
        builder.CreateBr(conditionBB);

        builder.SetInsertPoint(conditionBB);
        builder.CreateCondBr(builder.CreateOr(lhsValue.IsNull(), rhsValue.IsNull()), elseBB, thenBB);
        conditionBB = builder.GetInsertBlock();

        builder.SetInsertPoint(thenBB);

        Value* thenResult;

        auto* lhsData = lhsValue.GetData();
        auto* rhsData = rhsValue.GetData();

        switch (types[index]) {
            case EValueType::Boolean:
            case EValueType::Int64:
            case EValueType::Uint64:
                thenResult = builder.CreateZExtOrBitCast( 
                    builder.CreateICmpEQ(lhsData, rhsData), 
                    TDataTypeBuilder::TBoolean::get(builder.getContext()));
                break;

            case EValueType::Double:
                thenResult = builder.CreateZExtOrBitCast( 
                    builder.CreateFCmpUEQ(lhsData, rhsData), 
                    TDataTypeBuilder::TBoolean::get(builder.getContext()));
                break;

            case EValueType::String: {
                Value* lhsLength = lhsValue.GetLength();
                Value* rhsLength = rhsValue.GetLength();

                thenResult = builder.CreateCall4(
                            Module_->GetRoutine("Equal"),
                            lhsData, lhsLength, rhsData, rhsLength);
                break;
            }

            default:
                YUNREACHABLE();
        }

        builder.CreateBr(endBB);
        thenBB = builder.GetInsertBlock();

        builder.SetInsertPoint(elseBB);
        auto* elseResult = builder.CreateZExtOrBitCast(
            builder.CreateICmpEQ(lhsValue.IsNull(), rhsValue.IsNull()),
            TDataTypeBuilder::TBoolean::get(builder.getContext()));

        builder.CreateBr(endBB);
        elseBB = builder.GetInsertBlock();

        builder.SetInsertPoint(endBB);

        PHINode* result = builder.CreatePHI(thenResult->getType(), 2);
        result->addIncoming(thenResult, thenBB);
        result->addIncoming(elseResult, elseBB);

        return result;
    };

    YCHECK(types.size() >= 1);
    auto result = codegenEqualOp(0);

    for (size_t index = 1; index < types.size(); ++index) {
        result = builder.CreateAnd(result, codegenEqualOp(index));
    }

    builder.CreateRet(result);

    return function;
}

Function* TCGContext::CodegenGroupHasherFunction(
    const std::vector<EValueType>& types)
{
    auto module = Module_->GetModule();

    Function* function = Function::Create(
        TypeBuilder<ui64(TRow), false>::get(module->getContext()),
        Function::ExternalLinkage,
        "GroupHasher",
        module);

    auto args = function->arg_begin();
    Value* row = args; row->setName("row");
    YCHECK(++args == function->arg_end());

    TCGIRBuilder builder(BasicBlock::Create(module->getContext(), "entry", function));

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
        builder.CreateCondBr(value.IsNull(), elseBB, thenBB);
        conditionBB = builder.GetInsertBlock();

        builder.SetInsertPoint(thenBB);

        Value* thenResult;

        switch (value.GetStaticType()) {
            case EValueType::Int64:
            case EValueType::Uint64:
            case EValueType::Double:
                thenResult = value.Cast(EValueType::Uint64, true).GetData();
                break;

            case EValueType::String:
                thenResult = builder.CreateCall2(
                    Module_->GetRoutine("StringHash"),
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

    YCHECK(types.size() >= 1);
    auto result = codegenHashOp(0, builder);

    for (size_t index = 1; index < types.size(); ++index) {
        result = codegenHashCombine(builder, result, codegenHashOp(index, builder));
    }

    builder.CreateRet(result);

    return function;
}

TCGValue TCGContext::DoCodegenInOpExpr(
    TCGIRBuilder& builder,
    const std::vector<TCodegenValueBlock>& codegenArgs,
    int arrayIndex)
{  
    size_t keySize = codegenArgs.size();

    Value* newRowPtr = builder.CreateAlloca(TypeBuilder<TRow, false>::get(builder.getContext()));
    Value* executionContextPtrRef = GetExecutionContextPtr(builder);

    builder.CreateCall3(
        Module_->GetRoutine("AllocateRow"),
        executionContextPtrRef,
        builder.getInt32(keySize),
        newRowPtr);

    Value* newRowRef = builder.CreateLoad(newRowPtr);

    for (int index = 0; index < keySize; ++index) {
        auto id = index;
        codegenArgs[index](builder).StoreToRow(newRowRef, index, id);
    }

    Value* result = builder.CreateCall3(
        Module_->GetRoutine("IsRowInArray"),
        executionContextPtrRef,
        newRowRef,
        builder.getInt32(arrayIndex));

    return TCGValue::CreateFromValue(
        builder,
        builder.getInt16(EValueType::Boolean),
        nullptr,
        result,
        EValueType::Boolean);
}

TCGValue TCGContext::CodegenInOpExpr(
    TCGIRBuilder& builder,
    const TInOpExpression* expr,
    const TTableSchema& schema,
    Value* row)
{
    std::vector<TCodegenValueBlock> codegenArgs;
    for (const auto& argument : expr->Arguments) {
        codegenArgs.push_back([&] (TCGIRBuilder& builder) {
            return CodegenExpr(builder, argument, schema, row);
        });
    }

    auto it = Binding_.NodeToRows.find(expr);
    YCHECK(it != Binding_.NodeToRows.end());

    return DoCodegenInOpExpr(builder, codegenArgs, it->second);
}

TCGValue TCGContext::CodegenExpr(
    TCGIRBuilder& builder,
    const TConstExpressionPtr& expr,
    const TTableSchema& schema,
    Value* row)
{
    if (auto literalExpr = expr->As<TLiteralExpression>()) {
        auto it = Binding_.NodeToConstantIndex.find(literalExpr);
        YCHECK(it != Binding_.NodeToConstantIndex.end());
        auto index = it->second;
        return TCGValue::CreateFromRow(
            builder,
            GetConstantsRows(builder),
            index,
            literalExpr->Type,
            "literal." + Twine(index))
            .Steal();
    } else if (auto referenceExpr = expr->As<TReferenceExpression>()) {
        auto column = referenceExpr->ColumnName;
        auto index = schema.GetColumnIndexOrThrow(column);
        return TCGValue::CreateFromRow(
            builder,
            row,
            index,
            referenceExpr->Type,
            "reference." + Twine(column.c_str()));
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        return CodegenFunctionExpr(
            builder,
            functionExpr,
            schema,
            row);
    } else if (auto binaryOpExpr = expr->As<TBinaryOpExpression>()) {
        return CodegenBinaryOpExpr(
            builder,
            binaryOpExpr,
            schema,
            row);
    } else if (auto inOpExpr = expr->As<TInOpExpression>()) {
        return CodegenInOpExpr(
            builder,
            inOpExpr,
            schema,
            row);
    } else {
        YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////
// Operators
//

void TCGContext::CodegenScanOp(
    TCGIRBuilder& builder,
    const TCodegenConsumer& codegenConsumer)
{
    auto module = Module_->GetModule();

    // See ScanOpHelper.
    Function* function = Function::Create(
        TypeBuilder<void(void**, TRow*, int), false>::get(builder.getContext()),
        Function::ExternalLinkage,
        "ScanOpInner",
        module);

    auto args = function->arg_begin();
    Value* closure = args; closure->setName("closure");
    Value* rows = ++args; rows->setName("rows");
    Value* size = ++args; size->setName("size");
    YCHECK(++args == function->arg_end());

    TCGIRBuilder innerBuilder(
        function,
        &builder,
        closure);

    CodegenForEachRow(innerBuilder, rows, size, codegenConsumer);

    innerBuilder.CreateRetVoid();

    Value* executionContextPtr = GetExecutionContextPtr(builder);

    int dataSplitsIndex = 0;

    builder.CreateCall4(
        Module_->GetRoutine("ScanOpHelper"),
        executionContextPtr,
        builder.getInt32(dataSplitsIndex),
        innerBuilder.GetClosure(),
        function);
}

void TCGContext::CodegenFilterOp(
    TCGIRBuilder& builder,
    const TConstExpressionPtr& predicate,
    const TTableSchema& sourceTableSchema,
    const TCodegenSource& codegenSource,
    const TCodegenConsumer& codegenConsumer)
{
    codegenSource(builder, [&] (TCGIRBuilder& innerBuilder, Value* row) {
            auto predicateResult = CodegenExpr(
                innerBuilder,
                predicate,
                sourceTableSchema,
                row);

            Value* result = innerBuilder.CreateZExtOrBitCast(
                predicateResult.GetData(),
                builder.getInt64Ty());

            auto* ifBB = innerBuilder.CreateBBHere("if");
            auto* endifBB = innerBuilder.CreateBBHere("endif");

            innerBuilder.CreateCondBr(
                innerBuilder.CreateICmpNE(result, builder.getInt64(0)),
                ifBB,
                endifBB);

            innerBuilder.SetInsertPoint(ifBB);
            codegenConsumer(innerBuilder, row);
            innerBuilder.CreateBr(endifBB);

            innerBuilder.SetInsertPoint(endifBB);
    });
}

void TCGContext::CodegenProjectOp(
    TCGIRBuilder& builder,
    const TProjectClause& clause,
    const TTableSchema& sourceTableSchema,
    const TCodegenSource& codegenSource,
    const TCodegenConsumer& codegenConsumer)
{
    int projectionCount = clause.Projections.size();

    codegenSource(builder, [&] (TCGIRBuilder& innerBuilder, Value* row) {
            Value* newRowPtr = innerBuilder.CreateAlloca(TypeBuilder<TRow, false>::get(builder.getContext()));

            innerBuilder.CreateCall3(
                Module_->GetRoutine("AllocateRow"),
                GetExecutionContextPtr(innerBuilder),
                builder.getInt32(projectionCount),
                newRowPtr);

            Value* newRow = innerBuilder.CreateLoad(newRowPtr);

            for (int index = 0; index < projectionCount; ++index) {
                const auto& expr = clause.Projections[index].Expression;
                auto id = index;

                CodegenExpr(innerBuilder, expr, sourceTableSchema, row)
                    .StoreToRow(newRow, index, id);
            }

            codegenConsumer(innerBuilder, newRow);
    });
}

void TCGContext::CodegenGroupOp(
    TCGIRBuilder& builder,
    const TGroupClause& clause,
    const TTableSchema& sourceTableSchema,
    const TCodegenSource& codegenSource,
    const TCodegenConsumer& codegenConsumer)
{
    auto module = Module_->GetModule();

    // See GroupOpHelper.
    Function* function = Function::Create(
        TypeBuilder<void(void**, void*, void*), false>::get(builder.getContext()),
        Function::ExternalLinkage,
        "GroupOpInner",
        module);

    auto args = function->arg_begin();
    Value* closure = args; closure->setName("closure");
    Value* groupedRows = ++args; groupedRows->setName("groupedRows");
    Value* rows = ++args; rows->setName("rows");
    YCHECK(++args == function->arg_end());

    TCGIRBuilder innerBuilder(
        function,
        &builder,
        closure);

    int keySize = clause.GroupItems.size();
    int aggregateItemCount = clause.AggregateItems.size();

    Value* newRowPtr = innerBuilder.CreateAlloca(TypeBuilder<TRow, false>::get(builder.getContext()));

    innerBuilder.CreateCall3(
        Module_->GetRoutine("AllocatePersistentRow"),
        GetExecutionContextPtr(innerBuilder),
        builder.getInt32(keySize + aggregateItemCount),
        newRowPtr);

    codegenSource(innerBuilder, [&] (TCGIRBuilder& innerBuilder, Value* row) {
        Value* executionContextPtrRef = GetExecutionContextPtr(innerBuilder);
        Value* groupedRowsRef = innerBuilder.ViaClosure(groupedRows);
        Value* rowsRef = innerBuilder.ViaClosure(rows);
        Value* newRowPtrRef = innerBuilder.ViaClosure(newRowPtr);
        Value* newRowRef = innerBuilder.CreateLoad(newRowPtrRef);

        for (int index = 0; index < keySize; ++index) {
            const auto& expr = clause.GroupItems[index].Expression;
            auto id = index;

            CodegenExpr(innerBuilder, expr, sourceTableSchema, row)
                .StoreToRow(newRowRef, index, id);
        }

        for (int index = 0; index < aggregateItemCount; ++index) {
            const auto& item = clause.AggregateItems[index];
            const auto& expr = item.Expression;

            auto id = keySize + index;

            CodegenExpr(innerBuilder, expr, sourceTableSchema, row)
                .StoreToRow(newRowRef, keySize + index, id);
        }

        Value* foundRowPtr = innerBuilder.CreateCall5(
            Module_->GetRoutine("InsertGroupRow"),
            executionContextPtrRef,
            rowsRef,
            groupedRowsRef,
            newRowPtrRef,
            builder.getInt32(keySize + aggregateItemCount));

        CodegenIf(innerBuilder, [&] (TCGIRBuilder& innerBuilder) {
            return innerBuilder.CreateICmpNE(
                foundRowPtr,
                llvm::ConstantPointerNull::get(newRowRef->getType()->getPointerTo()));
        }, [&] (TCGIRBuilder& innerBuilder) {
            Value* foundRow = innerBuilder.CreateLoad(foundRowPtr);
            for (int index = 0; index < aggregateItemCount; ++index) {
                const auto& item = clause.AggregateItems[index];
                const auto& name = item.Name;

                auto id = keySize + index;
                auto type = item.Expression->Type;
                auto fn = item.AggregateFunction;

                CodegenAggregateFunction(innerBuilder, foundRow, newRowRef, fn, keySize + index, id, type, name.c_str());
            }
        }, [&] (TCGIRBuilder& innerBuilder) {

        });
    });

    CodegenForEachRow(
        innerBuilder,
        innerBuilder.CreateCall(Module_->GetRoutine("GetRowsData"), groupedRows),
        innerBuilder.CreateCall(Module_->GetRoutine("GetRowsSize"), groupedRows),
        codegenConsumer);

    innerBuilder.CreateRetVoid();

    std::vector<EValueType> keyTypes;
    for (int index = 0; index < keySize; ++index) {
        keyTypes.push_back(clause.GroupItems[index].Expression->Type);
    }

#ifdef YT_USE_CODEGENED_HASH
    builder.CreateCall4(
        Module_->GetRoutine("GroupOpHelper"),
        innerBuilder.GetClosure(),
        function,
        CodegenGroupHasherFunction(keyTypes),
        CodegenGroupComparerFunction(keyTypes));
#else
    builder.CreateCall4(
        Module_->GetRoutine("GroupOpHelper"),
        builder.getInt32(keySize),
        builder.getInt32(aggregateItemCount),
        innerBuilder.GetClosure(),
        function);
#endif

}

TCGQueryCallback TCGContext::CodegenEvaluate(
    const TConstQueryPtr& query,
    const TCGBinding& binding)
{
    auto module = TCGModule::Create(GetQueryRoutineRegistry());
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

    TCGIRBuilder builder(BasicBlock::Create(context, "entry", function));

    TCGContext ctx(module, binding, constants, executionContextPtr);

    TCodegenSource codegenSource = [&] (TCGIRBuilder& builder, const TCodegenConsumer& codegenConsumer) {
        ctx.CodegenScanOp(builder, codegenConsumer);
    };
    TTableSchema sourceSchema = query->TableSchema;

    if (query->Predicate) {
        codegenSource = [&, codegenSource, sourceSchema] (TCGIRBuilder& builder, const TCodegenConsumer& codegenConsumer) {
            ctx.CodegenFilterOp(builder, query->Predicate, sourceSchema, codegenSource, codegenConsumer);
        };
    }

    if (query->GroupClause) {
        codegenSource = [&, codegenSource, sourceSchema] (TCGIRBuilder& builder, const TCodegenConsumer& codegenConsumer) {
            ctx.CodegenGroupOp(builder, query->GroupClause.Get(), sourceSchema, codegenSource, codegenConsumer);
        };
        sourceSchema = query->GroupClause->GetTableSchema();
    }

    if (query->ProjectClause) {
        codegenSource = [&, codegenSource, sourceSchema] (TCGIRBuilder& builder, const TCodegenConsumer& codegenConsumer) {
            ctx.CodegenProjectOp(builder, query->ProjectClause.Get(), sourceSchema, codegenSource, codegenConsumer);
        };
        sourceSchema = query->ProjectClause->GetTableSchema();
    }

    codegenSource(builder, [&] (TCGIRBuilder& innerBuilder, Value* row) {
        Value* executionContextPtrRef = innerBuilder.ViaClosure(executionContextPtr);
        innerBuilder.CreateCall2(module->GetRoutine("WriteRow"), row, executionContextPtrRef);
    });

    builder.CreateRetVoid();

    return module->GetCompiledFunction<TCGQuerySignature>(entryFunctionName);
}

TCGFragmentCompiler CreateFragmentCompiler()
{
    using namespace std::placeholders;
    return std::bind(&TCGContext::CodegenEvaluate, _1, _2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

