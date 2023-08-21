#include "builder_base.h"

namespace NYT::NCodegen {

////////////////////////////////////////////////////////////////////////////////

using namespace llvm;

TIRBuilderBase::TIRBuilderBase(TCGModulePtr module)
    : IRBuilder(module->GetContext())
    , Module(std::move(module))
    , Context(Module->GetContext())
{ }

BasicBlock* TIRBuilderBase::CreateBB(const Twine& name)
{
    auto* current_function = GetInsertBlock()->getParent();
    return CreateBB(name, current_function);
}

BasicBlock* TIRBuilderBase::CreateBB(const Twine& name, Function* function)
{
    return BasicBlock::Create(Context, name, function);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen
