#pragma once

#include "execution_backend.h"
#include "public.h"
#include "routine_registry.h"
#include "type_builder.h"

#include <yt/yt/core/actions/callback.h>

#include <library/cpp/yt/memory/ref.h>

#include <memory>

#ifdef DEBUG
#  define DEFINED_DEBUG
#  undef DEBUG
#endif

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/Module.h>

#ifdef DEFINED_DEBUG
#  ifdef DEBUG
#    undef DEBUG
#    define DEBUG
#  endif
#  undef DEFINED_DEBUG
#endif

namespace NYT::NCodegen {

////////////////////////////////////////////////////////////////////////////////

class TCGModule
    : public TRefCounted
{
public:
    static TCGModulePtr Create(
        TRoutineRegistry* routineRegistry,
        EExecutionBackend backend = EExecutionBackend::Native,
        const TString& moduleName = "module");

    ~TCGModule();

    llvm::LLVMContext& GetContext();

    llvm::Module* GetModule() const;

    llvm::FunctionCallee GetRoutine(const TString& symbol) const;

    void ExportSymbol(const TString& name);

    template <class TSignature>
    TCallback<TSignature> GetCompiledFunction(const TString& name);

    void AddObjectFile(std::unique_ptr<llvm::object::ObjectFile> sharedObject);

    bool IsSymbolLoaded(const TString& symbol) const;
    void AddLoadedSymbol(const TString& symbol);

    bool IsFunctionLoaded(const TString& function) const;
    void AddLoadedFunction(const TString& function);

    bool IsModuleLoaded(TRef data) const;
    void AddLoadedModule(TRef data);

    void BuildWebAssembly();
    TRef GetWebAssemblyBytecode() const;

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;

    DECLARE_NEW_FRIEND()

    explicit TCGModule(std::unique_ptr<TImpl> impl);
    uint64_t GetFunctionAddress(const TString& name);

    EExecutionBackend GetBackend() const;
};

DEFINE_REFCOUNTED_TYPE(TCGModule)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen

#define CODEGEN_MODULE_INL_H_
#include "module-inl.h"
#undef CODEGEN_MODULE_INL_H_
