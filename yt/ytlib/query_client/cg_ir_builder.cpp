#include "stdafx.h"
#include "cg_ir_builder.h"

#include <llvm/IR/Module.h>
#include <llvm/IR/Intrinsics.h>

namespace NYT {
namespace NQueryClient {

using llvm::Function;
using llvm::BasicBlock;
using llvm::TypeBuilder;
using llvm::Value;
using llvm::Twine;
using llvm::Module;

////////////////////////////////////////////////////////////////////////////////

static const unsigned int MaxClosureSize = 16;

////////////////////////////////////////////////////////////////////////////////

TCGIRBuilder::TCGIRBuilder(
    Function* function,
    TCGIRBuilder* parent,
    Value* closurePtr)
    : TBase(BasicBlock::Create(parent->getContext(), "entry", function))
    , Parent_(parent)
    , ClosurePtr_(closurePtr)
{
    for (auto it = function->arg_begin(); it != function->arg_end(); ++it) {
        ValuesInContext_.insert(it);
    }

    EntryBlock_ = GetInsertBlock();
}

TCGIRBuilder::~TCGIRBuilder()
{ }

TCGIRBuilder::TCGIRBuilder(BasicBlock* basicBlock)
    : TBase(basicBlock)
    , Parent_(nullptr)
    , ClosurePtr_(nullptr)
{
    auto* function = basicBlock->getParent();
    for (auto it = function->arg_begin(); it != function->arg_end(); ++it) {
        ValuesInContext_.insert(it);
    }
}

Value* TCGIRBuilder::ViaClosure(Value* value, Twine name)
{
    // If |value| belongs to the current context, then we can use it directly.
    if (ValuesInContext_.count(value) > 0) {
        return value;
    }

    Twine resultingName(name.isTriviallyEmpty() ? value->getName() : name);

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
        CHECK(indexInClosure < MaxClosureSize);

        InsertPoint currentIP = saveIP();
        SetInsertPoint(EntryBlock_, EntryBlock_->begin());

        // Load the value to the current context through the closure.
        Value* result = CreateLoad(
            CreateLoad(
                CreatePointerCast(
                    CreateConstGEP1_32(ClosurePtr_, indexInClosure),
                    value->getType()->getPointerTo()->getPointerTo(),
                    resultingName + ".closureSlotPtr"
                ),
                resultingName + ".inParentPtr"
            ),
            resultingName);

        restoreIP(currentIP);

        Mapping_[valueInParent] = std::make_pair(result, indexInClosure);
        return result;
    }
}

Value* TCGIRBuilder::GetClosure()
{
    // Save all values into the closure in the parent context.

    Value* closure = Parent_->CreateAlloca(
        TypeBuilder<void*, false>::get(getContext()),
        getInt32(Mapping_.size()),
        "closure");

    for (auto & value : Mapping_) {
        Value* valueInParent = value.first;
        int indexInClosure = value.second.second;
        auto name = value.second.first->getName();

        Value* valueInParentPtr = Parent_->CreateAlloca(
            valueInParent->getType(),
            nullptr,
            name + ".inParentPtr");

        Parent_->CreateStore(
            valueInParent,
            valueInParentPtr);
        Parent_->CreateStore(
            valueInParentPtr,
            Parent_->CreatePointerCast(
                Parent_->CreateConstGEP1_32(closure, indexInClosure),
                valueInParentPtr->getType()->getPointerTo(),
                name + ".closureSlotPtr"
            )
        );
    }

    return closure;
}

BasicBlock* TCGIRBuilder::CreateBBHere(const Twine& name)
{
    return BasicBlock::Create(getContext(), name, GetInsertBlock()->getParent());
}

Value* TCGIRBuilder::CreateStackSave(const Twine& name)
{
    Module* module = GetInsertBlock()->getParent()->getParent();
    return CreateCall(llvm::Intrinsic::getDeclaration(module, llvm::Intrinsic::stacksave), name);
}

void TCGIRBuilder::CreateStackRestore(llvm::Value* ptr)
{
    Module* module = GetInsertBlock()->getParent()->getParent();
    CreateCall(llvm::Intrinsic::getDeclaration(module, llvm::Intrinsic::stackrestore), ptr);
}

llvm::CallInst* TCGIRBuilder::CreateCallWithArgs(llvm::Value* callee, std::initializer_list<llvm::Value*> args, const llvm::Twine& name)
{
    llvm::ArrayRef<llvm::Value *> argsArray(args.begin(), args.end());
    return Insert(llvm::CallInst::Create(callee, argsArray), name);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

