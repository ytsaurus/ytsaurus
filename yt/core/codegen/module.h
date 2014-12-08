#pragma once

#include "public.h"
#include "function.h"
#include "routine_registry.h"

#include <core/misc/intrusive_ptr.h>

#include <llvm/IR/Module.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/TypeBuilder.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>

#include <memory>

namespace NYT {
namespace NCodegen {

////////////////////////////////////////////////////////////////////////////////

class TCGModule
    : public TRefCounted
{
private:
    class TImpl;

public:
    static TCGModulePtr Create(TRoutineRegistry* routineRegistry, const Stroka& moduleName = "module");

    explicit TCGModule(std::unique_ptr<TImpl>&& impl);
    ~TCGModule();

    llvm::LLVMContext& GetContext();

    llvm::Module* GetModule() const;

    llvm::Function* GetRoutine(const Stroka& symbol) const;

    template <class TSignature>
    TCGFunction<TSignature> GetCompiledFunction(const Stroka& name);

private:
    std::unique_ptr<TImpl> Impl_;

    uint64_t GetFunctionAddress(const Stroka& name);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCodegen
} // namespace NYT


#define CODEGEN_MODULE_INL_H_
#include "module-inl.h"
#undef CODEGEN_MODULE_INL_H_

