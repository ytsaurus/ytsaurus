#pragma once

#include "public.h"
#include "routine_registry.h"
#include "type_builder.h"

#include <yt/yt/library/codegen_api/execution_backend.h>

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
        const std::string& moduleName = "module");

    ~TCGModule();

    llvm::LLVMContext& GetContext();

    llvm::Module* GetModule() const;

    llvm::FunctionCallee GetRoutine(const std::string& symbol) const;

    void ExportSymbol(const std::string& name);

    template <class TSignature>
    TCallback<TSignature> GetCompiledFunction(const std::string& name);

    void AddObjectFile(std::unique_ptr<llvm::object::ObjectFile> sharedObject);

    bool IsSymbolLoaded(const std::string& symbol) const;
    void AddLoadedSymbol(const std::string& symbol);

    bool IsFunctionLoaded(const std::string& function) const;
    void AddLoadedFunction(const std::string& function);

    bool IsModuleLoaded(TRef data) const;
    void AddLoadedModule(TRef data);

    void BuildWebAssembly();
    TRef GetWebAssemblyBytecode() const;

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;

    DECLARE_NEW_FRIEND()

    explicit TCGModule(std::unique_ptr<TImpl> impl);
    uint64_t GetFunctionAddress(const std::string& name);

    EExecutionBackend GetBackend() const;
};

DEFINE_REFCOUNTED_TYPE(TCGModule)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen

#define CODEGEN_MODULE_INL_H_
#include "module-inl.h"
#undef CODEGEN_MODULE_INL_H_
