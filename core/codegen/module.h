#pragma once

#include "public.h"
#include "function.h"
#include "routine_registry.h"

#include <yt/core/misc/ref.h>

#include <memory>

#ifdef DEBUG
#  define DEFINED_DEBUG
#  undef DEBUG
#endif

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/TypeBuilder.h>

#ifdef DEFINED_DEBUG
#  ifdef DEBUG
#    undef DEBUG
#    define DEBUG
#  endif
#  undef DEFINED_DEBUG
#endif

namespace NYT {
namespace NCodegen {

////////////////////////////////////////////////////////////////////////////////

class TCGModule
    : public TRefCounted
{
public:
    static TCGModulePtr Create(TRoutineRegistry* routineRegistry, const TString& moduleName = "module");

    ~TCGModule();

    llvm::LLVMContext& GetContext();

    llvm::Module* GetModule() const;

    llvm::Constant* GetRoutine(const TString& symbol) const;

    void ExportSymbol(const TString& name);

    template <class TSignature>
    TCGFunction<TSignature> GetCompiledFunction(const TString& name);

    void AddObjectFile(std::unique_ptr<llvm::object::ObjectFile> sharedObject);

    bool IsSymbolLoaded(const TString& symbol) const;
    void AddLoadedSymbol(const TString& symbol);

    bool IsFunctionLoaded(const TString& function) const;
    void AddLoadedFunction(const TString& function);

    bool IsModuleLoaded(TRef data) const;
    void AddLoadedModule(TRef data);

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;

    DECLARE_NEW_FRIEND();

    explicit TCGModule(std::unique_ptr<TImpl> impl);
    uint64_t GetFunctionAddress(const TString& name);
};

DEFINE_REFCOUNTED_TYPE(TCGModule)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCodegen
} // namespace NYT

#define CODEGEN_MODULE_INL_H_
#include "module-inl.h"
#undef CODEGEN_MODULE_INL_H_

