#include "cg_ir_builder.h"

#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/TypeBuilder.h>

#include <yt/core/misc/assert.h>

#include <yt/core/codegen/llvm_migrate_helpers.h>

namespace NYT {
namespace NQueryClient {

using llvm::Function;
using llvm::BasicBlock;
using llvm::TypeBuilder;
using llvm::Value;
using llvm::Type;
using llvm::Twine;
using llvm::Module;

////////////////////////////////////////////////////////////////////////////////

static const unsigned int MaxClosureSize = 32;

////////////////////////////////////////////////////////////////////////////////

TCGIRBuilder::TCGIRBuilder(
    llvm::Function* function,
    TCGIRBuilder* parent,
    Value* closurePtr)
    : TBase(BasicBlock::Create(function->getContext(), "entry", function))
    , Parent_(parent)
    , ClosurePtr_(closurePtr)
{
    for (auto it = function->arg_begin(); it != function->arg_end(); ++it) {
        ValuesInContext_.insert(ConvertToPointer(it));
    }
    EntryBlock_ = GetInsertBlock();
}

TCGIRBuilder::TCGIRBuilder(
    Function* function)
    : TCGIRBuilder(function, nullptr, nullptr)
{ }

TCGIRBuilder::~TCGIRBuilder()
{ }

Value* TCGIRBuilder::ViaClosure(Value* value, Twine name)
{
    // If |value| belongs to the current context, then we can use it directly.
    if (ValuesInContext_.count(value) > 0) {
        return value;
    }

    auto valueName = value->getName();
    Twine resultingName(name.isTriviallyEmpty() ? valueName : name);

    // Otherwise, capture |value| in the parent context.
    YCHECK(Parent_);
    YCHECK(ClosurePtr_);

    Value* valueInParent = Parent_->ViaClosure(value, resultingName);

    // Check if we have already captured this value.
    auto it = Mapping_.find(valueInParent);

    if (it != Mapping_.end()) {
        return it->second.first;
    } else {
        int indexInClosure = Mapping_.size();
        YCHECK(indexInClosure < MaxClosureSize);

        InsertPoint currentIP = saveIP();
        SetInsertPoint(EntryBlock_, EntryBlock_->begin());

        Types_.push_back(value->getType());
        Type* closureType = llvm::StructType::get(Parent_->getContext(), Types_);

        // Load the value to the current context through the closure.
        Value* result = CreateLoad(
            CreateStructGEP(
                nullptr,
                CreatePointerCast(
                    ClosurePtr_,
                    llvm::PointerType::getUnqual(closureType),
                    "castedClosure"),
                indexInClosure,
                resultingName + ".inParentPtr"),
            resultingName);

        restoreIP(currentIP);

        Mapping_[valueInParent] = std::make_pair(result, indexInClosure);

        return result;
    }
}

Value* TCGIRBuilder::GetClosure()
{
    // Save all values into the closure in the parent context.

    Type* closureType = llvm::StructType::get(Parent_->getContext(), Types_);

    Value* closure = llvm::UndefValue::get(closureType);

    for (auto& value : Mapping_) {
        Value* valueInParent = value.first;
        int indexInClosure = value.second.second;

        closure = Parent_->CreateInsertValue(
            closure,
            valueInParent,
            indexInClosure);
    }

    Value* closurePtr = Parent_->CreateAlloca(
        closureType,
        nullptr,
        "closure");

    Parent_->CreateStore(
        closure,
        closurePtr);

    return Parent_->CreatePointerCast(
        closurePtr,
        TypeBuilder<void**, false>::get(getContext()),
        "uncastedClosure");
}

BasicBlock* TCGIRBuilder::CreateBBHere(const Twine& name)
{
    return BasicBlock::Create(getContext(), name, GetInsertBlock()->getParent());
}

Value* TCGIRBuilder::CreateStackSave(const Twine& name)
{
    Module* module = GetInsertBlock()->getParent()->getParent();
    return CreateCall(
        llvm::Intrinsic::getDeclaration(module, llvm::Intrinsic::stacksave),
        {},
        name);
}

void TCGIRBuilder::CreateStackRestore(Value* ptr)
{
    Module* module = GetInsertBlock()->getParent()->getParent();
    CreateCall(llvm::Intrinsic::getDeclaration(module, llvm::Intrinsic::stackrestore), ptr);
}

Type* TCGIRBuilder::getSizeType() const
{
    return TypeBuilder<size_t, false>::get(getContext());
}

llvm::AllocaInst* TCGIRBuilder::CreateAlignedAlloca(
    Type *type,
    unsigned align,
    Value* arraySize,
    const llvm::Twine& name)
{
#if LLVM_VERSION_GE(5, 0)
    const llvm::DataLayout &DL = BB->getParent()->getParent()->getDataLayout();
    return Insert(new llvm::AllocaInst(type, DL.getAllocaAddrSpace(), arraySize, align), name);
#else
    return Insert(new llvm::AllocaInst(type, arraySize, align), name);
#endif
}

llvm::Value* TCGIRBuilder::CreateOr(llvm::Value* lhs, llvm::Value* rhs, const llvm::Twine& name)
{
    if (llvm::Constant* lc = llvm::dyn_cast<llvm::Constant>(lhs)) {
        if (lc->isNullValue()) {
            return rhs;
        }

        if (lc->isAllOnesValue()) {
            return lhs;
        }
    }

    if (llvm::Constant *rc = llvm::dyn_cast<llvm::Constant>(rhs)) {
        if (rc->isNullValue()) {
            return lhs;
        }

        if (rc->isAllOnesValue()) {
            return rhs;
        }
    }
    return TBase::CreateOr(lhs, rhs, name);
}

llvm::Value* TCGIRBuilder::CreateAnd(llvm::Value* lhs, llvm::Value* rhs, const llvm::Twine& name)
{
    if (llvm::Constant* lc = llvm::dyn_cast<llvm::Constant>(lhs)) {
        if (lc->isNullValue()) {
            return lhs;
        }

        if (lc->isAllOnesValue()) {
            return rhs;
        }
    }

    if (llvm::Constant *rc = llvm::dyn_cast<llvm::Constant>(rhs)) {
        if (rc->isNullValue()) {
            return rhs;
        }

        if (rc->isAllOnesValue()) {
            return lhs;
        }
    }
    return TBase::CreateAnd(lhs, rhs, name);
}

llvm::Value* TCGIRBuilder::CreateSelect(
    llvm::Value* condition,
    llvm::Value* trueValue,
    llvm::Value* falseValue,
    const llvm::Twine& name)
{
    if (llvm::Constant* constantCondition = llvm::dyn_cast<llvm::Constant>(condition)) {
        if (constantCondition->isNullValue()) {
            return falseValue;
        }

        if (constantCondition->isAllOnesValue()) {
            return trueValue;
        }
    }

    return TBase::CreateSelect(condition, trueValue, falseValue, name);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

