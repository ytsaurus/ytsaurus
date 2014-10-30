#include "stdafx.h"
#include "cg_fragment.h"
#include "cg_routine_registry.h"

#include "private.h"

#include <core/misc/lazy_ptr.h>

#include <core/concurrency/action_queue.h>

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
namespace NQueryClient {

static const auto& Logger = QueryClientLogger;

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
    TCGMemoryManager()
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

        address = TRoutineRegistry::Get()->GetAddress(name.c_str());
        if (address) {
            return address;
        }

        return 0;
    }
};

class TCGFragment::TImpl
{
public:
    TImpl()
        : CompiledBody_(nullptr)
    {
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
        auto module = std::make_unique<llvm::Module>("cgfragment", Context_);
        module->setTargetTriple(hostTriple);

        // Create engine.
        std::string what;
        Engine_.reset(llvm::EngineBuilder(module.get())
            .setEngineKind(llvm::EngineKind::JIT)
            .setOptLevel(llvm::CodeGenOpt::Default)
            .setUseMCJIT(true)
            .setMCJITMemoryManager(new TCGMemoryManager())
            .setMCPU(hostCpu)
            .setErrorStr(&what)
            .create());

        if (!Engine_) {
            THROW_ERROR_EXCEPTION("Could not create llvm::ExecutionEngine")
                << TError(Stroka(what));
        }

        Module_ = module.release();
        Module_->setDataLayout(Engine_->getDataLayout()->getStringRepresentation());
    }

    llvm::Module* GetModule() const
    {
        return Module_;
    }

    llvm::Function* GetRoutine(const Stroka& symbol) const
    {
        auto type = TRoutineRegistry::Get()->GetTypeBuilder(symbol)(
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

    void Embody(llvm::Function* body)
    {
        YCHECK(!CompiledBody_);

        auto parent = Module_;
        auto type = llvm::TypeBuilder<TCGFunction, false>::get(Context_);

        YCHECK(body->getParent() == parent);
        YCHECK(body->getType() == type);

        CompiledBody_ = reinterpret_cast<TCGFunction>(Compile(body));

        // TODO(sandello): Clean module here.
    }

    TCGFunction GetCompiledBody()
    {
        return CompiledBody_;
    }

private:
    void* Compile(llvm::Function* body)
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

        return Engine_->getPointerToFunction(body);
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

    TCGFunction CompiledBody_;

    mutable yhash_map<Stroka, llvm::Function*> CachedRoutines_;

};

TCGFragment::TCGFragment()
    : Impl_(std::make_unique<TImpl>())
{ }

TCGFragment::~TCGFragment()
{ }

llvm::Module* TCGFragment::GetModule() const
{
    return Impl_->GetModule();
}

llvm::Function* TCGFragment::GetRoutine(const Stroka& symbol) const
{
    return Impl_->GetRoutine(symbol);
}

void TCGFragment::Embody(llvm::Function* body)
{
    Impl_->Embody(body);
}

TCGFunction TCGFragment::GetCompiledBody()
{
    return Impl_->GetCompiledBody();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

