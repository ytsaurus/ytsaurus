#pragma once

#include "module.h"

#include <llvm/IR/IRBuilder.h>

namespace NYT::NCodegen {

////////////////////////////////////////////////////////////////////////////////

class TIRBuilderBase
    : public llvm::IRBuilder<>
{
public:
    explicit TIRBuilderBase(TCGModulePtr module);

    llvm::BasicBlock* CreateBB(const llvm::Twine& name = "");
    llvm::BasicBlock* CreateBB(const llvm::Twine& name, llvm::Function* function);

    template <class T>
        requires std::is_integral_v<T>
    llvm::ConstantInt* ConstantIntValue(T value);

    template <class T>
        requires std::is_enum_v<T>
    llvm::ConstantInt* ConstantEnumValue(T value);

public:
    const TCGModulePtr Module;
    llvm::LLVMContext& Context;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen


#define BUILDER_BASE_INL_H_
#include "builder_base-inl.h"
#undef BUILDER_BASE_INL_H_
