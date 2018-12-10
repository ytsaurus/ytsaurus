#pragma once

#include <llvm/IR/IRBuilder.h>

#include <unordered_map>
#include <unordered_set>

#include <yt/core/codegen/llvm_migrate_helpers.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TContextTrackingInserter
{
protected:
    mutable std::unordered_set<llvm::Value*> ValuesInContext_;

public:
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

class TCGIRBuilder
#if LLVM_VERSION_GE(3, 9)
    : public llvm::IRBuilder<llvm::ConstantFolder, TContextTrackingInserter>
#else
    : public llvm::IRBuilder<true, llvm::ConstantFolder, TContextTrackingInserter>
#endif
{
private:
#if LLVM_VERSION_GE(3, 9)
    typedef llvm::IRBuilder<llvm::ConstantFolder, TContextTrackingInserter> TBase;
#else
    typedef llvm::IRBuilder<true, llvm::ConstantFolder, TContextTrackingInserter> TBase;
#endif

    //! Builder associated with the parent context.
    TCGIRBuilder* Parent_;

    //! Pointer to the closure.
    //! Note that this value belongs to the current context.
    llvm::Value* ClosurePtr_;

    //! Translates captured values in the parent context into their indexes in the closure.
    std::unordered_map<llvm::Value*, std::pair<llvm::Value*, int>> Mapping_;

    std::vector<llvm::Type*> Types_;

    llvm::BasicBlock* EntryBlock_;

public:
    TCGIRBuilder(
        llvm::Function* function,
        TCGIRBuilder* parent,
        llvm::Value* closurePtr);

    explicit TCGIRBuilder(
        llvm::Function* function);

    ~TCGIRBuilder();

    //! Captures and passes a value from the parent context via the closure.
    llvm::Value* ViaClosure(llvm::Value* value, llvm::Twine name = llvm::Twine());

    //! Returns the closure in the parent context.
    llvm::Value* GetClosure();

    //! Creates a new basic block within the current function.
    llvm::BasicBlock* CreateBBHere(const llvm::Twine& name);

    //! Saves current stack state.
    llvm::Value* CreateStackSave(const llvm::Twine& name);

    //! Restores given stack state.
    void CreateStackRestore(llvm::Value* ptr);

    llvm::Type* getSizeType() const;

    llvm::AllocaInst* CreateAlignedAlloca(
        llvm::Type *type,
        unsigned align,
        llvm::Value* arraySize = nullptr,
        const llvm::Twine& name = "");

    llvm::Value* CreateOr(llvm::Value* lhs, llvm::Value* rhs, const llvm::Twine& name = "");

    llvm::Value* CreateAnd(llvm::Value* lhs, llvm::Value* rhs, const llvm::Twine& name = "");

    llvm::Value* CreateSelect(
        llvm::Value* condition,
        llvm::Value* trueValue,
        llvm::Value* falseValue,
        const llvm::Twine& name = "");
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

