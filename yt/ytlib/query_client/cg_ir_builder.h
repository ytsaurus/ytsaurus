#pragma once

#include "cg_types.h"

#include <llvm/IR/IRBuilder.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TContextTrackingInserter
{
protected:
    mutable std::unordered_set<llvm::Value*> ValuesInContext_;

    void InsertHelper(
        llvm::Instruction* instruction,
        const llvm::Twine& name,
        llvm::BasicBlock* basicBlock,
        llvm::BasicBlock::iterator insertPoint) const
    {
        ValuesInContext_.insert(static_cast<llvm::Value*>(instruction));

        if (basicBlock) {
            basicBlock->getInstList().insert(insertPoint, instruction);
        }

        instruction->setName(name);
    }

};

class TContextIRBuilder
    : public llvm::IRBuilder<true, llvm::ConstantFolder, TContextTrackingInserter>
{
private:
    typedef llvm::IRBuilder<true, llvm::ConstantFolder, TContextTrackingInserter> TBase;

    //! Builder associated with the parent context.
    TContextIRBuilder* Parent_;

    //! Pointer to the closure.
    //! Note that this value belongs to the current context.
    llvm::Value* ClosurePtr_;

    //! Closure itself.
    //! Note that this value belongs to the parent context.
    llvm::Value* Closure_;

    //! Translates captured values in the parent context into their indexes in the closure.
    std::unordered_map<llvm::Value*, int> Mapping_;

public:
    TContextIRBuilder(
        llvm::BasicBlock* basicBlock);

    TContextIRBuilder(
        llvm::BasicBlock* basicBlock,
        TContextIRBuilder* parent,
        llvm::Value* closurePtr);

    ~TContextIRBuilder();

    //! Captures and passes a value from the parent context via the closure.
    llvm::Value* ViaClosure(llvm::Value* value, llvm::Twine name = llvm::Twine());

    //! Returns the closure in the parent context.
    llvm::Value* GetClosure() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

