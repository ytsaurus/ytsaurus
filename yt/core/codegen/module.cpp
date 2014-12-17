#include "stdafx.h"
#include "private.h"
#include "init.h"
#include "module.h"
#include "routine_registry.h"

#include <llvm/ADT/Triple.h>

#include <llvm/IR/DiagnosticInfo.h>
#include <llvm/IR/DiagnosticPrinter.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Verifier.h>

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/MCJIT.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>

#include <llvm/PassManager.h>

#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>

#include <llvm/Support/raw_ostream.h>
#include <llvm/Support/Host.h>

namespace NYT {
namespace NCodegen {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CodegenLogger;

static bool DumpIR()
{
    static bool result = (getenv("DUMP_IR") != nullptr);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TCGMemoryManager
    : public llvm::SectionMemoryManager
{
public:
    TCGMemoryManager(TRoutineRegistry* RoutineRegistry)
        : RoutineRegistry(RoutineRegistry)
    { }

    ~TCGMemoryManager()
    { }

    virtual uint64_t getSymbolAddress(const std::string& name) override
    {
        uint64_t address = 0;

        address = llvm::SectionMemoryManager::getSymbolAddress(name);
        if (address) {
            return address;
        }

        return RoutineRegistry->GetAddress(name.c_str());
    }

    // RoutineRegistry is supposed to be a static object.
    TRoutineRegistry* RoutineRegistry;
};

class TCGModule::TImpl
{
public:
    TImpl(TRoutineRegistry* routineRegistry, const Stroka& moduleName)
        : RoutineRegistry_(routineRegistry)
    {
        InitializeCodegen();

        Context_.setDiagnosticHandler(&TImpl::DiagnosticHandler, nullptr);

        // Infer host parameters.
        auto hostCpu = llvm::sys::getHostCPUName();
        auto hostTriple = llvm::Triple::normalize(
            llvm::sys::getProcessTriple()
#ifdef _win_
            + "-elf"
#endif
        );

        // Create module.
        auto module = std::make_unique<llvm::Module>(moduleName.c_str(), Context_);
        module->setTargetTriple(hostTriple);
        Module_ = module.get();

        // Create engine.
        std::string what;
        Engine_.reset(llvm::EngineBuilder(module.get())
            .setEngineKind(llvm::EngineKind::JIT)
            .setOptLevel(llvm::CodeGenOpt::Default)
            .setUseMCJIT(true)
            .setMCJITMemoryManager(new TCGMemoryManager(RoutineRegistry_))
            .setMCPU(hostCpu)
            .setErrorStr(&what)
            .create());

        if (!Engine_) {
            THROW_ERROR_EXCEPTION("Could not create llvm::ExecutionEngine")
                << TError(Stroka(what));
        }

        // Now engine holds the module.
        module.release();

        Module_->setDataLayout(Engine_->getDataLayout()->getStringRepresentation());
    }

    llvm::LLVMContext& GetContext()
    {
        return Context_;
    }

    llvm::Module* GetModule() const
    {
        return Module_;
    }

    llvm::Function* GetRoutine(const Stroka& symbol) const
    {
        auto type = RoutineRegistry_->GetTypeBuilder(symbol)(
            const_cast<llvm::LLVMContext&>(Context_));

        auto it = CachedRoutines_.find(symbol);
        if (it == CachedRoutines_.end()) {
            auto routine = llvm::Function::Create(
                type,
                llvm::Function::ExternalLinkage,
                symbol.c_str(),
                Module_);

            it = CachedRoutines_.insert(std::make_pair(symbol, routine)).first;
        }

        YCHECK(it->second->getFunctionType() == type);
        return it->second;
    }

    uint64_t GetFunctionAddress(const Stroka& name)
    {
        if (!Compiled_) {
            Finalize();
        }
        return Engine_->getFunctionAddress(name.c_str());
    }

private:
    void Finalize()
    {
        YCHECK(Compiled_ == false);
        Compile();
        Compiled_ = true;
    }

    void Compile()
    {
        if (DumpIR()) {
            llvm::errs() << "\n******** Before Optimization ***********************************\n";
            Module_->dump();
            llvm::errs() << "\n****************************************************************\n";
        }

        YCHECK(!llvm::verifyModule(*Module_, &llvm::errs()));

        llvm::PassManagerBuilder passManagerBuilder;
        passManagerBuilder.OptLevel = 2;
        passManagerBuilder.SizeLevel = 0;
        passManagerBuilder.Inliner = llvm::createFunctionInliningPass();

        std::unique_ptr<llvm::FunctionPassManager> functionPassManager_;
        std::unique_ptr<llvm::PassManager> modulePassManager_;

        functionPassManager_ = std::make_unique<llvm::FunctionPassManager>(Module_);
        functionPassManager_->add(new llvm::DataLayoutPass(Module_));
        passManagerBuilder.populateFunctionPassManager(*functionPassManager_);

        functionPassManager_->doInitialization();
        for (auto it = Module_->begin(), jt = Module_->end(); it != jt; ++it) {
            if (!it->isDeclaration()) {
                functionPassManager_->run(*it);
            }
        }
        functionPassManager_->doFinalization();

        modulePassManager_ = std::make_unique<llvm::PassManager>();
        modulePassManager_->add(new llvm::DataLayoutPass(Module_));
        passManagerBuilder.populateModulePassManager(*modulePassManager_);

        modulePassManager_->run(*Module_);

        if (DumpIR()) {
            llvm::errs() << "\n******** After Optimization ************************************\n";
            Module_->dump();
            llvm::errs() << "\n****************************************************************\n";
        }

        Engine_->finalizeObject();

        // TODO(sandello): Clean module here.
    }

    static void DiagnosticHandler(const llvm::DiagnosticInfo& info, void* /*opaque*/)
    {
        if (info.getSeverity() != llvm::DS_Error && info.getSeverity() != llvm::DS_Warning) {
            return;
        }

        std::string what;
        llvm::raw_string_ostream os(what);
        llvm::DiagnosticPrinterRawOStream printer(os);

        info.print(printer);

        LOG_INFO("LLVM has triggered a message: %s/%s: %s",
            DiagnosticSeverityToString(info.getSeverity()),
            DiagnosticKindToString((llvm::DiagnosticKind)info.getKind()),
            os.str().c_str());
    }

    static const char* DiagnosticKindToString(llvm::DiagnosticKind kind)
    {
        switch (kind) {
            case llvm::DK_InlineAsm:
                return "DK_InlineAsm";
            case llvm::DK_StackSize:
                return "DK_StackSize";
            case llvm::DK_DebugMetadataVersion:
                return "DK_DebugMetadataVersion";
            case llvm::DK_SampleProfile:
                return "DK_SampleProfile";
            case llvm::DK_OptimizationRemark:
                return "DK_OptimizationRemark";
            case llvm::DK_OptimizationRemarkMissed:
                return "DK_OptimizationRemarkMissed";
            case llvm::DK_OptimizationRemarkAnalysis:
                return "DK_OptimizationRemarkAnalysis";
            case llvm::DK_FirstPluginKind:
                return "DK_FirstPluginKind";
            default:
                return "DK_(?)";
        }
        YUNREACHABLE();
    }

    static const char* DiagnosticSeverityToString(llvm::DiagnosticSeverity severity)
    {
        switch (severity) {
            case llvm::DS_Error:
                return "DS_Error";
            case llvm::DS_Warning:
                return "DS_Warning";
            case llvm::DS_Note:
                return "DS_Note";
            default:
                return "DS_(?)";
        }
        YUNREACHABLE();
    }

private:
    llvm::LLVMContext Context_;
    llvm::Module* Module_;

    std::unique_ptr<llvm::ExecutionEngine> Engine_;

    mutable yhash_map<Stroka, llvm::Function*> CachedRoutines_;

    bool Compiled_ = false;

    // RoutineRegistry is supposed to be a static object.
    TRoutineRegistry* RoutineRegistry_;
};

////////////////////////////////////////////////////////////////////////////////

TCGModulePtr TCGModule::Create(TRoutineRegistry* routineRegistry, const Stroka& moduleName)
{
    return New<TCGModule>(std::make_unique<TCGModule::TImpl>(routineRegistry, moduleName));
}

TCGModule::TCGModule(std::unique_ptr<TImpl>&& impl)
    : Impl_(std::move(impl))
{ }

TCGModule::~TCGModule()
{ }

llvm::Module* TCGModule::GetModule() const
{
    return Impl_->GetModule();
}

llvm::Function* TCGModule::GetRoutine(const Stroka& symbol) const
{
    return Impl_->GetRoutine(symbol);
}

llvm::LLVMContext& TCGModule::GetContext()
{
    return Impl_->GetContext();
}

uint64_t TCGModule::GetFunctionAddress(const Stroka& name)
{
    return Impl_->GetFunctionAddress(name);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCodegen
} // namespace NYT

