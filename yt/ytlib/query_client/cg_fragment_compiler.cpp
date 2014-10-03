#include "stdafx.h"
#include "cg_fragment_compiler.h"

#include "private.h"
#include "helpers.h"

#include "plan_fragment.h"

#include "cg_fragment.h"
#include "cg_routines.h"
#include "cg_ir_builder.h"

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

////////////////////////////////////////////////////////////////////////////////

typedef std::function<void(TCGIRBuilder& builder, Value* row)> TCodegenConsumer;

static Value* CodegenValuesPtrFromRow(TCGIRBuilder&, Value*);

typedef TypeBuilder<TValue, false> TTypeBuilder;
typedef TypeBuilder<TValueData, false> TDataTypeBuilder;

class TCGValue
{
private:
    TCGIRBuilder& Builder_;
    Value* Type_;
    Value* Length_;
    Value* Data_;
    std::string Name_;

    TCGValue(TCGIRBuilder& builder, Value* type, Value* length, Value* data, Twine name)
        : Builder_(builder)
        , Type_(type)
        , Length_(length)
        , Data_(data)
        , Name_(name.str())
    {
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
    }

    static TCGValue CreateFromValue(
        TCGIRBuilder& builder,
        Value* type,
        Value* length,
        Value* data,
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

        return TCGValue(builder, type, length, data, name);
    }

    static TCGValue CreateFromRow(
        TCGIRBuilder& builder,
        Value* row,
        int index,
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
        return TCGValue(builder, type, length, data, name);
    }

    static TCGValue CreateNull(
        TCGIRBuilder& builder,
        Twine name = Twine())
    {
        return TCGValue(
            builder,
            builder.getInt16(EValueType::Null),
            llvm::UndefValue::get(TTypeBuilder::TLength::get(builder.getContext())),
            llvm::UndefValue::get(TTypeBuilder::TData::get(builder.getContext())),
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

    Value* GetData(EValueType type)
    {
        Type* targetType;

        switch (type) {
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

    TCGValue& SetType(EValueType type)
    {
        Type_ = Builder_.getInt16(type);
        return *this;
    }

    TCGValue& SetTypeIfNotNull(EValueType type)
    {
        Type_ = Builder_.CreateSelect(
            IsNull(),
            Builder_.getInt16(EValueType::Null),
            Builder_.getInt16(type));
        return *this;
    }

    TCGValue& SetData(Value* data)
    {
        Data_ = Builder_.CreateBitCast(
            data,
            TDataTypeBuilder::get(Builder_.getContext()),
            Twine(Name_) + ".data");
        return *this;
    }

    TCGValue Cast(EValueType source, EValueType dest)
    {
        if (dest == source) {
            return *this;
        }

        auto value = GetData(source);

        Value* result;
        if (dest == EValueType::Int64) {
            auto destType = TDataTypeBuilder::TUint64::get(Builder_.getContext());
            if (source == EValueType::Uint64) {
                result = Builder_.CreateIntCast(value, destType, false);
            } else if (source == EValueType::Double) {
                result = Builder_.CreateFPToSI(value, destType);
            } else {
                YUNREACHABLE();
            }
        } else if (dest == EValueType::Uint64) {
            auto destType = TDataTypeBuilder::TUint64::get(Builder_.getContext());
            if (source == EValueType::Int64) {
                result = Builder_.CreateIntCast(value, destType, true);
            } else if (source == EValueType::Double) {
                result = Builder_.CreateFPToUI(value, destType);
            } else {
                YUNREACHABLE();
            }
        } else if (dest == EValueType::Double) {
            auto destType = TDataTypeBuilder::TDouble::get(Builder_.getContext());
            if (source == EValueType::Uint64) {
                result = Builder_.CreateUIToFP(value, destType);
            } else if (source == EValueType::Int64) {
                result = Builder_.CreateSIToFP(value, destType);
            } else {
                YUNREACHABLE();
            }
        } else {
            YUNREACHABLE();
        }

        return CreateFromValue(Builder_, GetType(), GetLength(), result);
    }
};

typedef std::function<Value* (TCGIRBuilder& builder)> TCodegenBlock;
typedef std::function<TCGValue(TCGIRBuilder& builder)> TCodegenValueBlock;
typedef std::function<void(TCGIRBuilder& builder)> TCodegenVoidBlock;

class TCGContext
{
public:
    static Function* CodegenEvaluate(
        const TConstPlanFragmentPtr& planFragment,
        const TCGFragment& cgFragment,
        const TCGBinding& binding);

private:
    const TCGFragment& Fragment_;
    const TCGBinding& Binding_;
    Value* ConstantsRow_;
    Value* ExecutionContextPtr_;

    TCGContext(
        const TCGFragment& cgFragment,
        const TCGBinding& binding,
        Value* constantsRow,
        Value* executionContextPtr)
        : Fragment_(cgFragment)
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

    void CodegenOp(
        TCGIRBuilder& builder,
        const TConstOperatorPtr& op,
        const TCodegenConsumer& codegenConsumer);

    void CodegenScanOp(
        TCGIRBuilder& builder,
        const TScanOperator* op,
        const TCodegenConsumer& codegenConsumer);

    void CodegenFilterOp(
        TCGIRBuilder& builder,
        const TFilterOperator* op,
        const TCodegenConsumer& codegenConsumer);

    void CodegenProjectOp(
        TCGIRBuilder& builder,
        const TProjectOperator* op,
        const TCodegenConsumer& codegenConsumer);

    void CodegenGroupOp(
        TCGIRBuilder& builder,
        const TGroupOperator* op,
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
    EValueType type,
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
    Value* thenData = thenValue.GetData(type);
    builder.CreateBr(endBB);
    thenBB = builder.GetInsertBlock();

    builder.SetInsertPoint(elseBB);
    auto elseValue = elseCodegen(builder);
    Value* elseType = elseValue.GetType();
    Value* elseLength = elseValue.GetLength();
    Value* elseData = elseValue.GetData(type);
    builder.CreateBr(endBB);
    elseBB = builder.GetInsertBlock();

    builder.SetInsertPoint(endBB);
    PHINode* phiType = builder.CreatePHI(builder.getInt16Ty(), 2, name + ".phiType");
    //phiType->addIncoming(builder.getInt16(EValueType::Null), conditionBB);
    phiType->addIncoming(thenType, thenBB);
    phiType->addIncoming(elseType, elseBB);

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

    return TCGValue::CreateFromValue(builder, phiType, phiLength, phiData, name);
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
    auto newValue = TCGValue::CreateFromRow(builder, newRow, index, name + ".new");

    CodegenIf(builder, [&] (TCGIRBuilder& builder) {
        return newValue.IsNull();
    }, [&] (TCGIRBuilder& builder) {

    }, [&] (TCGIRBuilder& builder) {
        auto aggregateValue = TCGValue::CreateFromRow(builder, aggregateRow, index, name + ".aggregate");

        CodegenIfValue(builder, [&] (TCGIRBuilder& builder) {
            return aggregateValue.IsNull();
        }, [&] (TCGIRBuilder& builder) {
            return newValue;
        }, [&] (TCGIRBuilder& builder) {
            Value* newData = newValue.GetData(type);
            Value* aggregateData = aggregateValue.GetData(type);
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

            return TCGValue::CreateFromValue(builder, builder.getInt16(type), nullptr, resultData, name);
        }, type).StoreToRow(aggregateRow, index, id);
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

TCGValue TCGContext::CodegenFunctionExpr(
    TCGIRBuilder& builder,
    const TFunctionExpression* expr,
    const TTableSchema& schema,
    Value* row)
{
    Stroka functionName(expr->FunctionName);
    functionName.to_lower();

    auto name = "{" + expr->GetName() + "}";
    auto type = expr->Type;
    auto nameTwine = Twine(name.c_str());

    if (functionName == "if") {
        YCHECK(expr->Arguments.size() == 3);
        const auto& condExpr = expr->Arguments[0];
        const auto& thenExpr = expr->Arguments[1];
        const auto& elseExpr = expr->Arguments[2];

        auto condition = CodegenExpr(builder, condExpr, schema, row);

        return CodegenIfValue(builder, [&] (TCGIRBuilder& builder) {
            return condition.IsNull();
        }, [&] (TCGIRBuilder& builder) {
            return TCGValue::CreateNull(builder);
        }, [&] (TCGIRBuilder& builder) {
            return CodegenIfValue(builder, [&] (TCGIRBuilder& builder) {
                return builder.CreateICmpNE(
                    builder.CreateZExtOrBitCast(condition.GetData(EValueType::Boolean), builder.getInt64Ty()),
                    builder.getInt64(0));            
            }, [&] (TCGIRBuilder& builder) {
                return CodegenExpr(builder, thenExpr, schema, row);
            }, [&] (TCGIRBuilder& builder) {
                return CodegenExpr(builder, elseExpr, schema, row);
            }, type);
        }, type, nameTwine);
    } else if (functionName == "is_prefix") {
        YCHECK(expr->Arguments.size() == 2);
        const auto& lhsExpr = expr->Arguments[0];
        const auto& rhsExpr = expr->Arguments[1];

        YCHECK(lhsExpr->Type == EValueType::String);
        YCHECK(rhsExpr->Type == EValueType::String);

        auto lhsValue = CodegenExpr(builder, lhsExpr, schema, row);
    
        return CodegenIfValue(builder, [&] (TCGIRBuilder& builder) {
            return lhsValue.IsNull();
        }, [&] (TCGIRBuilder& builder) {
            return TCGValue::CreateNull(builder);
        }, [&] (TCGIRBuilder& builder) {
            auto rhsValue = CodegenExpr(builder, rhsExpr, schema, row);

            return CodegenIfValue(builder, [&] (TCGIRBuilder& builder) {
                return rhsValue.IsNull();            
            }, [&] (TCGIRBuilder& builder) {
                return TCGValue::CreateNull(builder);
            }, [&] (TCGIRBuilder& builder) {
                Value* lhsData = lhsValue.GetData(EValueType::String);
                Value* lhsLength = lhsValue.GetLength();
                Value* rhsData = rhsValue.GetData(EValueType::String);
                Value* rhsLength = rhsValue.GetLength();

                Value* result = builder.CreateCall4(
                    Fragment_.GetRoutine("IsPrefix"),
                    lhsData, lhsLength, rhsData, rhsLength);

                return TCGValue::CreateFromValue(builder, builder.getInt16(type), nullptr, result);
            }, type);
        }, type, nameTwine);
    } else if (functionName == "lower") {
        YCHECK(expr->Arguments.size() == 1);
        const auto& argExpr = expr->Arguments[0];

        auto argValue = CodegenExpr(builder, argExpr, schema, row);

        return CodegenIfValue(builder, [&] (TCGIRBuilder& builder) {
            return argValue.IsNull();
        }, [&] (TCGIRBuilder& builder) {
            return TCGValue::CreateNull(builder);
        }, [&] (TCGIRBuilder& builder) {
            Value* argData = argValue.GetData(EValueType::String);
            Value* argLength = argValue.GetLength();

            Value* result = builder.CreateCall3(
                Fragment_.GetRoutine("ToLower"),
                GetExecutionContextPtr(builder),
                argData,
                argLength);

            return TCGValue::CreateFromValue(builder, builder.getInt16(type), argLength, result);
        }, type, nameTwine);
    } else if (functionName == "is_null") {
        YCHECK(expr->Arguments.size() == 1);
        const auto& argExpr = expr->Arguments[0];

        auto argValue = CodegenExpr(builder, argExpr, schema, row);

        return TCGValue::CreateFromValue(
            builder,
            builder.getInt16(type),
            nullptr,            
            builder.CreateZExtOrBitCast(
                argValue.IsNull(),
                TDataTypeBuilder::TBoolean::get(builder.getContext())));
    } else if (functionName == "int64" || functionName == "uint64" || functionName == "double") {
        YCHECK(expr->Arguments.size() == 1);
        const auto& argExpr = expr->Arguments[0];
        return CodegenExpr(builder, argExpr, schema, row).Cast(argExpr->Type, type);
    }

    YUNIMPLEMENTED();
}

TCGValue TCGContext::CodegenBinaryOpExpr(
    TCGIRBuilder& builder,
    const TBinaryOpExpression* expr,
    const TTableSchema& schema,
    Value* row)
{
    auto name = "{" + expr->GetName() + "}";
    auto type = expr->Type;
    
    auto nameTwine = Twine(name.c_str());
    auto lhsValue = CodegenExpr(builder, expr->Lhs, schema, row);

    return CodegenIfValue(builder, [&] (TCGIRBuilder& builder) {
            return lhsValue.IsNull();
        }, [&] (TCGIRBuilder& builder) {
            return TCGValue::CreateNull(builder);
        }, [&] (TCGIRBuilder& builder) {
            auto rhsValue = CodegenExpr(builder, expr->Rhs, schema, row);

            return CodegenIfValue(builder, [&] (TCGIRBuilder& builder) {
                return rhsValue.IsNull();            
            }, [&] (TCGIRBuilder& builder) {
                return TCGValue::CreateNull(builder);
            }, [&] (TCGIRBuilder& builder) {
                auto lhsType = expr->Lhs->Type;
                auto rhsType = expr->Rhs->Type;

                YCHECK(lhsType == rhsType);
                auto operandType = lhsType;

                Value* lhsData = lhsValue.GetData(operandType);
                Value* rhsData = rhsValue.GetData(operandType);
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
                        switch (expr->Opcode) {
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
                        switch (expr->Opcode) {
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
                        switch (expr->Opcode) {
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

                        switch (expr->Opcode) {
                            case EBinaryOp::Equal:
                                evalData = builder.CreateCall4(
                                    Fragment_.GetRoutine("Equal"),
                                    lhsData, lhsLength, rhsData, rhsLength);
                                break;
                            case EBinaryOp::NotEqual:
                                evalData = builder.CreateCall4(
                                    Fragment_.GetRoutine("NotEqual"),
                                    lhsData, lhsLength, rhsData, rhsLength);
                                break;
                            case EBinaryOp::Less:
                                evalData = builder.CreateCall4(
                                    Fragment_.GetRoutine("LexicographicalCompare"),
                                    lhsData, lhsLength, rhsData, rhsLength);
                                break;
                            case EBinaryOp::Greater:
                                evalData = builder.CreateCall4(
                                    Fragment_.GetRoutine("LexicographicalCompare"),
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

                return TCGValue::CreateFromValue(builder, builder.getInt16(type), nullptr, evalData);
            }, type);
        }, type, nameTwine);
}


TCGValue TCGContext::CodegenInOpExpr(
    TCGIRBuilder& builder,
    const TInOpExpression* expr,
    const TTableSchema& schema,
    Value* row)
{    
    auto type = expr->Type;
    size_t keySize = expr->Arguments.size();

    Value* newRowPtr = builder.CreateAlloca(TypeBuilder<TRow, false>::get(builder.getContext()));
    Value* executionContextPtrRef = GetExecutionContextPtr(builder);

    builder.CreateCall3(
        Fragment_.GetRoutine("AllocateRow"),
        executionContextPtrRef,
        builder.getInt32(keySize),
        newRowPtr);

    Value* newRowRef = builder.CreateLoad(newRowPtr);

    for (int index = 0; index < keySize; ++index) {
        const auto& argExpr = expr->Arguments[index];
        auto id = index;
        auto type = argExpr->Type;

        CodegenExpr(builder, argExpr, schema, row)
            .SetTypeIfNotNull(type)
            .StoreToRow(newRowRef, index, id);
    }

    auto it = Binding_.NodeToRows.find(expr);
    YCHECK(it != Binding_.NodeToRows.end());
    auto index = it->second;

    Value* result = builder.CreateCall3(
        Fragment_.GetRoutine("IsRowInArray"),
        executionContextPtrRef,
        newRowRef,
        builder.getInt32(index));

    return TCGValue::CreateFromValue(builder, builder.getInt16(type), nullptr, result);
}

TCGValue TCGContext::CodegenExpr(
    TCGIRBuilder& builder,
    const TConstExpressionPtr& expr,
    const TTableSchema& schema,
    Value* row)
{
    if (auto literalExpr = expr->As<TLiteralExpression>()) {
        auto it = Binding_.NodeToConstantIndex.find(expr.Get());
        YCHECK(it != Binding_.NodeToConstantIndex.end());
        auto index = it->second;
        return TCGValue::CreateFromRow(
            builder,
            GetConstantsRows(builder),
            index,
            "literal." + Twine(index))
            .SetType(expr->Type) // Force type as constants are non-NULL.
            .Steal();
    } else if (auto referenceExpr = expr->As<TReferenceExpression>()) {
        auto column = referenceExpr->ColumnName;
        auto index = schema.GetColumnIndexOrThrow(column);
        return TCGValue::CreateFromRow(
            builder,
            row,
            index,
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

void TCGContext::CodegenOp(
    TCGIRBuilder& builder,
    const TConstOperatorPtr& op,
    const TCodegenConsumer& codegenConsumer)
{
    if (auto scanOp = op->As<TScanOperator>()) {
        return CodegenScanOp(builder, scanOp, codegenConsumer);
    } else if (auto filterOp = op->As<TFilterOperator>()) {
        return CodegenFilterOp(builder, filterOp, codegenConsumer);
    } else if (auto projectOp = op->As<TProjectOperator>()) {
        return CodegenProjectOp(builder, projectOp, codegenConsumer);
    } else if (auto groupOp = op->As<TGroupOperator>()) {
        return CodegenGroupOp(builder, groupOp, codegenConsumer);
    } else {
        YUNREACHABLE();
    }
}

void TCGContext::CodegenScanOp(
    TCGIRBuilder& builder,
    const TScanOperator* op,
    const TCodegenConsumer& codegenConsumer)
{
    auto module = builder.GetInsertBlock()->getParent()->getParent();

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

    auto it = Binding_.ScanOpToDataSplits.find(op);
    YCHECK(it != Binding_.ScanOpToDataSplits.end());
    int dataSplitsIndex = it->second;

    builder.CreateCall4(
        Fragment_.GetRoutine("ScanOpHelper"),
        executionContextPtr,
        builder.getInt32(dataSplitsIndex),
        innerBuilder.GetClosure(),
        function);
}

void TCGContext::CodegenFilterOp(
    TCGIRBuilder& builder,
    const TFilterOperator* op,
    const TCodegenConsumer& codegenConsumer)
{
    auto sourceSchema = op->GetTableSchema();

    CodegenOp(builder, op->Source,
        [&] (TCGIRBuilder& innerBuilder, Value* row) {
            auto predicateResult = CodegenExpr(
                innerBuilder,
                op->Predicate,
                sourceSchema,
                row);

            Value* result = innerBuilder.CreateZExtOrBitCast(
                predicateResult.GetData(EValueType::Boolean),
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
    const TProjectOperator* op,
    const TCodegenConsumer& codegenConsumer)
{
    int projectionCount = op->Projections.size();
    auto sourceTableSchema = op->Source->GetTableSchema();

    CodegenOp(builder, op->Source,
        [&] (TCGIRBuilder& innerBuilder, Value* row) {
            Value* newRowPtr = innerBuilder.CreateAlloca(TypeBuilder<TRow, false>::get(builder.getContext()));

            innerBuilder.CreateCall3(
                Fragment_.GetRoutine("AllocateRow"),
                GetExecutionContextPtr(innerBuilder),
                builder.getInt32(projectionCount),
                newRowPtr);

            Value* newRow = innerBuilder.CreateLoad(newRowPtr);

            for (int index = 0; index < projectionCount; ++index) {
                const auto& expr = op->Projections[index].Expression;
                auto id = index;
                auto type = expr->Type;

                CodegenExpr(innerBuilder, expr, sourceTableSchema, row)
                    .SetTypeIfNotNull(type)
                    .StoreToRow(newRow, index, id);
            }

            codegenConsumer(innerBuilder, newRow);
    });
}

void TCGContext::CodegenGroupOp(
    TCGIRBuilder& builder,
    const TGroupOperator* op,
    const TCodegenConsumer& codegenConsumer)
{
    auto module = builder.GetInsertBlock()->getParent()->getParent();

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

    int keySize = op->GroupItems.size();
    int aggregateItemCount = op->AggregateItems.size();
    auto sourceTableSchema = op->Source->GetTableSchema();

    Value* newRowPtr = innerBuilder.CreateAlloca(TypeBuilder<TRow, false>::get(builder.getContext()));

    CodegenOp(innerBuilder, op->Source, [&] (TCGIRBuilder& innerBuilder, Value* row) {
        Value* executionContextPtrRef = GetExecutionContextPtr(innerBuilder);
        Value* groupedRowsRef = innerBuilder.ViaClosure(groupedRows);
        Value* rowsRef = innerBuilder.ViaClosure(rows);
        Value* newRowPtrRef = innerBuilder.ViaClosure(newRowPtr);

        innerBuilder.CreateCall3(
            Fragment_.GetRoutine("AllocateRow"),
            executionContextPtrRef,
            builder.getInt32(keySize + aggregateItemCount),
            newRowPtrRef);

        Value* newRowRef = innerBuilder.CreateLoad(newRowPtrRef);

        for (int index = 0; index < keySize; ++index) {
            const auto& expr = op->GroupItems[index].Expression;
            auto id = index;
            auto type = expr->Type;

            CodegenExpr(innerBuilder, expr, sourceTableSchema, row)
                .SetTypeIfNotNull(type)
                .StoreToRow(newRowRef, index, id);
        }

        for (int index = 0; index < aggregateItemCount; ++index) {
            const auto& item = op->AggregateItems[index];
            const auto& expr = item.Expression;

            auto id = keySize + index;
            auto type = expr->Type;

            CodegenExpr(innerBuilder, expr, sourceTableSchema, row)
                .SetTypeIfNotNull(type)
                .StoreToRow(newRowRef, keySize + index, id);
        }

        Value* foundRowPtr = innerBuilder.CreateCall3(
            Fragment_.GetRoutine("FindRow"),
            executionContextPtrRef,
            rowsRef,
            newRowRef);


        CodegenIf(innerBuilder, [&] (TCGIRBuilder& innerBuilder) {
            return innerBuilder.CreateICmpNE(
                foundRowPtr,
                llvm::ConstantPointerNull::get(newRowRef->getType()->getPointerTo()));
        }, [&] (TCGIRBuilder& innerBuilder) {
            Value* foundRow = innerBuilder.CreateLoad(foundRowPtr);
            for (int index = 0; index < aggregateItemCount; ++index) {
                const auto& item = op->AggregateItems[index];
                const auto& name = item.Name;

                auto id = keySize + index;
                auto type = item.Expression->Type;
                auto fn = item.AggregateFunction;

                CodegenAggregateFunction(innerBuilder, foundRow, newRowRef, fn, keySize + index, id, type, name.c_str());
            }
        }, [&] (TCGIRBuilder& innerBuilder) {
            innerBuilder.CreateCall5(
                Fragment_.GetRoutine("AddRow"),
                executionContextPtrRef,
                rowsRef,
                groupedRowsRef,
                newRowPtrRef,
                builder.getInt32(keySize + aggregateItemCount));
        });
    });

    CodegenForEachRow(
        innerBuilder,
        innerBuilder.CreateCall(Fragment_.GetRoutine("GetRowsData"), groupedRows),
        innerBuilder.CreateCall(Fragment_.GetRoutine("GetRowsSize"), groupedRows),
        codegenConsumer);

    innerBuilder.CreateRetVoid();

    builder.CreateCall4(
        Fragment_.GetRoutine("GroupOpHelper"),
        builder.getInt32(keySize),
        builder.getInt32(aggregateItemCount),
        innerBuilder.GetClosure(),
        function);
}

Function* TCGContext::CodegenEvaluate(
    const TConstPlanFragmentPtr& planFragment,
    const TCGFragment& cgFragment,
    const TCGBinding& binding)
{
    auto* module = cgFragment.GetModule();
    auto& context = module->getContext();

    // See TCGFunction.
    Function* function = Function::Create(
        TypeBuilder<TCgFunctionSignature, false>::get(context),
        Function::ExternalLinkage,
        "Evaluate",
        module);

    auto args = function->arg_begin();
    Value* constants = args; constants->setName("constants");
    Value* executionContextPtr = ++args; executionContextPtr->setName("passedFragmentParamsPtr");
    YCHECK(++args == function->arg_end());

    TCGIRBuilder builder(BasicBlock::Create(context, "entry", function));

    TCGContext ctx(cgFragment, binding, constants, executionContextPtr);

    ctx.CodegenOp(builder, planFragment->Head,
        [&] (TCGIRBuilder& innerBuilder, Value* row) {
            Value* executionContextPtrRef = innerBuilder.ViaClosure(executionContextPtr);
            innerBuilder.CreateCall2(cgFragment.GetRoutine("WriteRow"), row, executionContextPtrRef);
        });

    builder.CreateRetVoid();

    return function;
}

TCGFragmentCompiler CreateFragmentCompiler()
{
    using namespace std::placeholders;
    return std::bind(&TCGContext::CodegenEvaluate, _1, _2, _3);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

