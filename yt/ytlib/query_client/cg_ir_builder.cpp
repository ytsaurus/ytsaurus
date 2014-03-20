#include "stdafx.h"
#include "cg_ir_builder.h"

#include <llvm/IR/Module.h>

namespace NYT {
namespace NQueryClient {

using llvm::BasicBlock;
using llvm::TypeBuilder;
using llvm::Value;

static const unsigned int MaxClosureSize = 32;

////////////////////////////////////////////////////////////////////////////////

TCGIRBuilder::TCGIRBuilder(
    llvm::BasicBlock* basicBlock,
    TCGIRBuilder* parent,
    Value* closurePtr)
    : TBase(basicBlock)
    , Parent_(parent)
    , ClosurePtr_(closurePtr)
{
    auto* function = basicBlock->getParent();
    for (auto it = function->arg_begin(); it != function->arg_end(); ++it) {
        ValuesInContext_.insert(it);
    }

    Closure_ = Parent_->CreateAlloca(
        TypeBuilder<void*, false>::get(basicBlock->getContext()),
        getInt32(MaxClosureSize),
        "closure");
}

TCGIRBuilder::~TCGIRBuilder()
{ }

TCGIRBuilder::TCGIRBuilder(llvm::BasicBlock* basicBlock)
    : TBase(basicBlock)
    , Parent_(nullptr)
    , ClosurePtr_(nullptr)
    , Closure_(nullptr)
{
    auto* function = basicBlock->getParent();
    for (auto it = function->arg_begin(); it != function->arg_end(); ++it) {
        ValuesInContext_.insert(it);
    }
}

Value* TCGIRBuilder::ViaClosure(Value* value, llvm::Twine name)
{
    // If |value| belongs to the current context, then we can use it directly.
    if (ValuesInContext_.count(value) > 0) {
        return value;
    }

    if (name.isTriviallyEmpty()) {
        name = value->getName();
    }

    // Otherwise, capture |value| in the parent context.
    YCHECK(Parent_);
    YCHECK(ClosurePtr_);

    Value* valueInParent = Parent_->ViaClosure(value, name);

    // Check if we have already captured this value.
    auto insertResult = Mapping_.insert(
        std::make_pair(valueInParent, Mapping_.size()));
    auto indexInClosure = insertResult.first->second;
    YCHECK(indexInClosure < MaxClosureSize);

    if (insertResult.second) {
        // If it is a fresh value we have to save it
        // into the closure in the parent context.
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
                Parent_->CreateConstGEP1_32(Closure_, indexInClosure),
                valueInParentPtr->getType()->getPointerTo(),
                name + ".closureSlotPtr"
            )
        );
    }

    // Load the value to the current context through the closure.
    return
        CreateLoad(
            CreateLoad(
                CreatePointerCast(
                    CreateConstGEP1_32(ClosurePtr_, indexInClosure),
                    value->getType()->getPointerTo()->getPointerTo(),
                    name + ".closureSlotPtr"
                ),
                name + ".inParentPtr"
            ),
            name);
}

llvm::Value* TCGIRBuilder::GetClosure() const
{
    return Closure_;
}

llvm::BasicBlock* TCGIRBuilder::CreateBBHere(const llvm::Twine& name)
{
    return llvm::BasicBlock::Create(getContext(), name, GetInsertBlock()->getParent());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

