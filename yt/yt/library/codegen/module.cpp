#include "module.h"
#include "private.h"
#include "init.h"
#include "routine_registry.h"
#include "llvm_migrate_helpers.h"
#include "msan.h"

#include <llvm/ADT/Triple.h>

#if !LLVM_VERSION_GE(3, 7)
#error "LLVM 3.7 or 3.9 or 4.0 is required."
#endif

#include <llvm/Analysis/CGSCCPassManager.h>
#include <llvm/Analysis/LoopAnalysisManager.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/MCJIT.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/IR/DiagnosticInfo.h>
#include <llvm/IR/DiagnosticPrinter.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/PassManager.h>
#include <llvm/IR/Verifier.h>
#include <llvm/MC/MCContext.h>
#include <llvm/Object/ObjectFile.h>
#include <llvm/Passes/PassBuilder.h>
#include <llvm/Support/Host.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Support/SmallVectorMemoryBuffer.h>
#include <llvm/CodeGen/Passes.h>
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/Transforms/Instrumentation.h>
#include <llvm/Transforms/Instrumentation/MemorySanitizer.h>

#define PRINT_PASSES_TIME
#ifdef PRINT_PASSES_TIME
#include <llvm/Pass.h>
#include <llvm/Support/Timer.h>
#include <llvm/Transforms/Scalar.h>
#endif

#include <util/system/compiler.h>
#include <util/system/sanitizers.h>

#include <mutex>

#ifdef _linux_
    #include <link.h>
    #include <dlfcn.h>
#endif

#include "msan.h"

struct __emutls_control;

extern "C" void* yt__emutls_get_address(__emutls_control* control) Y_NO_SANITIZE("memory")
{
    auto fn = (void(*)(void*))control;
    void* p;
    fn(&p);
    return p;
}


namespace NYT::NCodegen {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CodegenLogger;

static bool IsIRDumpEnabled()
{
    static bool result = (getenv("DUMP_IR") != nullptr);
    return result;
}

static bool IsPassesTimeDumpEnabled()
{
    static bool result = (getenv("DUMP_PASSES_TIME") != nullptr);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TCGMemoryManager
    : public llvm::SectionMemoryManager
{
public:
    explicit TCGMemoryManager(TRoutineRegistry* routineRegistry)
        : RoutineRegistry_(routineRegistry)
    { }

    uint64_t getSymbolAddress(const std::string& name) override
    {
#if defined(_msan_enabled_)
        if (name == "__emutls_get_address") {
            return (int64_t)yt__emutls_get_address;
        }

#define XX(msan_tls_variable) \
        if (name == "__emutls_v." #msan_tls_variable) { \
            int64_t p = (int64_t) yt_ ## msan_tls_variable; \
            return p; \
        }
MSAN_TLS_STUBS
#undef XX
#endif

        auto address = llvm::SectionMemoryManager::getSymbolAddress(name);
        if (address) {
            return address;
        }

        address = RoutineRegistry_->GetAddress(name.c_str());
        return address;
    }

private:
    // RoutineRegistry is supposed to be a static object.
    TRoutineRegistry* const RoutineRegistry_;

};

class TCGModule::TImpl
{
public:
    TImpl(
        TRoutineRegistry* routineRegistry,
        EExecutionBackend backend,
        const TString& moduleName)
        : RoutineRegistry_(routineRegistry)
        , ExecutionBackend_(backend)
    {
        InitializeCodegen();

#if !LLVM_VERSION_GE(6, 0)
        Context_.setDiagnosticHandler(&TImpl::DiagnosticHandler, nullptr);
#else
        Context_.setDiagnosticHandlerCallBack(&TImpl::DiagnosticHandler, nullptr);
#endif

        llvm::StringRef cpu;
        std::string triple;
        if (ExecutionBackend_ == EExecutionBackend::WebAssembly) {
            cpu = "generic";

            llvm::Triple wasm64Triple;
            wasm64Triple.setArch(llvm::Triple::wasm64);
            triple = wasm64Triple.normalize();
        } else {
            // Infer host parameters.
            cpu = llvm::sys::getHostCPUName();
            triple = llvm::Triple::normalize(
                llvm::sys::getProcessTriple()
#ifdef _win_
                + "-elf"
#endif
            );
#ifdef _darwin_
            // Modules generated with Clang contain macosx10.11.0 OS signature,
            // whereas LLVM modules contains darwin15.0.0.
            // So we rebuild triple to match with Clang object files.
            auto llvmTriple = llvm::Triple(triple);
            llvm::VersionTuple version;
            llvmTriple.getMacOSXVersion(version);
            auto formattedOsName = Format("macosx%v.%v.%v", version.getMajor(), *version.getMinor(), *version.getSubminor());
            auto osNameAsTwine = llvm::Twine(std::string_view(formattedOsName));
            auto fixedTriple = llvm::Triple(llvmTriple.getArchName(), llvmTriple.getVendorName(), osNameAsTwine);
            triple = llvm::Triple::normalize(fixedTriple.getTriple());
#endif
        }

        // Create module.
        auto module = std::make_unique<llvm::Module>(moduleName.c_str(), Context_);
        module->setTargetTriple(triple);
        Module_ = module.get();

        llvm::TargetOptions targetOptions;
        targetOptions.EnableFastISel = true;

        // Create engine.
        std::string what;
        llvm::EngineBuilder builder(std::move(module));
        builder
            .setEngineKind(llvm::EngineKind::JIT)
            .setOptLevel(llvm::CodeGenOpt::Default)
            .setMCJITMemoryManager(std::make_unique<TCGMemoryManager>(RoutineRegistry_))
            .setMCPU(cpu)
            .setErrorStr(&what)
            .setTargetOptions(targetOptions);

        if (ExecutionBackend_ == EExecutionBackend::Native) {
            builder
                .setMCJITMemoryManager(std::make_unique<TCGMemoryManager>(RoutineRegistry_));
        }

        Engine_.reset(builder.create());

        if (!Engine_) {
            THROW_ERROR_EXCEPTION("Could not create llvm::ExecutionEngine")
                << TError(TString(what));
        }

#if !LLVM_VERSION_GE(3, 9)
        Module_->setDataLayout(Engine_->getDataLayout()->getStringRepresentation());
#else
        Module_->setDataLayout(Engine_->getDataLayout().getStringRepresentation());
#endif

    }

    llvm::LLVMContext& GetContext()
    {
        return Context_;
    }

    llvm::Module* GetModule() const
    {
        return Module_;
    }

    llvm::FunctionCallee GetRoutine(const TString& symbol) const
    {
        auto type = RoutineRegistry_->GetTypeBuilder(symbol)(
            const_cast<llvm::LLVMContext&>(Context_));

        return Module_->getOrInsertFunction(symbol.c_str(), type);
    }

    void ExportSymbol(const TString& name)
    {
        YT_VERIFY(ExportedSymbols_.insert(name).second);
    }

    uint64_t GetFunctionAddress(const TString& name)
    {
        if (!Compiled_) {
            Finalize();
        }

        return Engine_->getFunctionAddress(name.c_str());
    }

    void AddObjectFile(
        std::unique_ptr<llvm::object::ObjectFile> sharedObject)
    {
        Engine_->addObjectFile(std::move(sharedObject));
    }

    bool IsSymbolLoaded(const TString& symbol)
    {
        return LoadedSymbols_.count(symbol) != 0;
    }

    void AddLoadedSymbol(const TString& symbol)
    {
        LoadedSymbols_.insert(symbol);
    }

    bool IsFunctionLoaded(const TString& function) const
    {
        return LoadedFunctions_.count(function) != 0;
    }

    void AddLoadedFunction(const TString& function)
    {
        LoadedFunctions_.insert(function);
    }

    bool IsModuleLoaded(TRef data) const
    {
        return LoadedModules_.count(TStringBuf(data.Begin(), data.Size())) != 0;
    }

    void AddLoadedModule(TRef data)
    {
        LoadedModules_.insert(TString(TStringBuf(data.Begin(), data.Size())));
    }

    void BuildWebAssembly()
    {
        YT_VERIFY(ExecutionBackend_ == EExecutionBackend::WebAssembly);

        if (!Compiled_) {
            Finalize();
        }
    }

    TRef GetWebAssemblyBytecode() const
    {
        YT_VERIFY(ExecutionBackend_ == EExecutionBackend::WebAssembly);

        return {WebAssemblyBytecode_.data(), WebAssemblyBytecode_.size()};
    }

    EExecutionBackend GetBackend() const
    {
        return ExecutionBackend_;
    }

private:
    void Finalize()
    {
        YT_VERIFY(!Compiled_);
        Compile();
        Compiled_ = true;
    }

    // See YT-8035 for details why we are clearing COMDAT.
    void ClearComdatSection() const
    {
        for (auto& it : Module_->functions()) {
            it.setComdat(nullptr);
        }

        for (auto& it : Module_->globals()) {
            it.setComdat(nullptr);
        }

        for (auto& it : Module_->ifuncs()) {
            it.setComdat(nullptr);
        }

        Module_->getComdatSymbolTable().clear();
    }

    void RunInternalizePass() const
    {
        auto modulePassManager = llvm::legacy::PassManager();

        modulePassManager.add(llvm::createInternalizePass([&] (const llvm::GlobalValue& gv) -> bool {
            auto name = TString(gv.getName().str());
            return ExportedSymbols_.count(name) > 0;
        }));

        modulePassManager.add(llvm::createGlobalDCEPass());
        modulePassManager.run(*Module_);
    }

    void OptimizeAndAddMSanViaNewPassManager() const
    {
        llvm::LoopAnalysisManager loopAnalysisManager;
        llvm::FunctionAnalysisManager functionalAnalysisManager;
        llvm::CGSCCAnalysisManager cgsccAnalysisManager;
        llvm::ModuleAnalysisManager moduleAnalysisManager;

        llvm::PassBuilder passBuilder;

        passBuilder.registerModuleAnalyses(moduleAnalysisManager);
        passBuilder.registerCGSCCAnalyses(cgsccAnalysisManager);
        passBuilder.registerFunctionAnalyses(functionalAnalysisManager);
        passBuilder.registerLoopAnalyses(loopAnalysisManager);
        passBuilder.crossRegisterProxies(
            loopAnalysisManager,
            functionalAnalysisManager,
            cgsccAnalysisManager,
            moduleAnalysisManager);

        llvm::ModulePassManager modulePassManager = passBuilder.buildO0DefaultPipeline(
            llvm::OptimizationLevel::O0);

        modulePassManager.addPass(
            llvm::MemorySanitizerPass(llvm::MemorySanitizerOptions()));

        modulePassManager.run(*Module_, moduleAnalysisManager);
    }

    void OptimizeViaLegacyPassManager() const
    {
        llvm::PassManagerBuilder passManagerBuilder;
        passManagerBuilder.OptLevel = 0;
        passManagerBuilder.SizeLevel = 0;
        passManagerBuilder.SLPVectorize = true;
        passManagerBuilder.LoopVectorize = true;
        passManagerBuilder.Inliner = llvm::createFunctionInliningPass();

        auto functionPassManager = llvm::legacy::FunctionPassManager(Module_);

        passManagerBuilder.populateFunctionPassManager(functionPassManager);

        functionPassManager.doInitialization();
        for (auto it = Module_->begin(), jt = Module_->end(); it != jt; ++it) {
            if (ExecutionBackend_ == EExecutionBackend::WebAssembly) {
                it->removeFnAttr("target-cpu");
                it->removeFnAttr("target-features");
            }

            if (!it->isDeclaration()) {
                functionPassManager.run(*it);
            }
        }
        functionPassManager.doFinalization();

        auto modulePassManager = llvm::legacy::PassManager();

        passManagerBuilder.populateModulePassManager(modulePassManager);
        modulePassManager.run(*Module_);
    }

    void OptimizeIR() const
    {
        if (NSan::MSanIsOn() &&
            ExecutionBackend_ == EExecutionBackend::Native) // NB(dtorilov): MSan at WebAssembly is not supported.
        {
            OptimizeAndAddMSanViaNewPassManager();
        } else {
            OptimizeViaLegacyPassManager();
        }
    }

    void Compile()
    {
#ifdef PRINT_PASSES_TIME
        if (IsPassesTimeDumpEnabled()) {
            llvm::TimePassesIsEnabled = true;
        }
#endif

        YT_LOG_DEBUG("Started compiling module");

        ClearComdatSection();

        if (IsIRDumpEnabled()) {
            llvm::errs() << "\n******** Before Optimization ***********************************\n";
#if !LLVM_VERSION_GE(5, 0)
            Module_->dump();
#else
            Module_->print(llvm::errs(), nullptr);
#endif
            llvm::errs() << "\n****************************************************************\n";
        }

        YT_LOG_DEBUG("Verifying IR");
        YT_VERIFY(!llvm::verifyModule(*Module_, &llvm::errs()));

        RunInternalizePass();

        // Now, setup optimization pipeline and run actual optimizations.
        YT_LOG_DEBUG("Optimizing IR");

        OptimizeIR();

        if (IsIRDumpEnabled()) {
            llvm::errs() << "\n******** After Optimization ************************************\n";
#if !LLVM_VERSION_GE(5, 0)
            Module_->dump();
#else
            Module_->print(llvm::errs(), nullptr);
#endif
            llvm::errs() << "\n****************************************************************\n";
        }

        YT_LOG_DEBUG("Finalizing module");

        if (ExecutionBackend_ == EExecutionBackend::WebAssembly) {
            BuildWebAssemblyImpl();
        } else {
            Engine_->finalizeObject();
        }

#ifdef PRINT_PASSES_TIME
        if (IsPassesTimeDumpEnabled()) {
            llvm::TimerGroup::printAll(llvm::errs());
            llvm::TimerGroup::clearAll();
        }
#endif

        YT_LOG_DEBUG("Finished compiling module");
        // TODO(sandello): Clean module here.
    }

    void BuildWebAssemblyImpl()
    {
        YT_VERIFY(ExecutionBackend_ == EExecutionBackend::WebAssembly);

        std::lock_guard locked(Engine_->lock);
        llvm::cantFail(Module_->materializeAll());
        {
            llvm::legacy::PassManager passManager;
            llvm::raw_svector_ostream stream(WebAssemblyBytecode_);
            llvm::MCContext* context = nullptr;

            if (Engine_->getTargetMachine()->addPassesToEmitMC(
                passManager,
                context,
                stream,
                !Engine_->getVerifyModules()))
            {
                llvm::report_fatal_error("Target does not support MC emission!");
            }

            passManager.run(*Module_);
        }
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

        YT_LOG_INFO("LLVM has triggered a message: %v/%v: %v",
            DiagnosticSeverityToString(info.getSeverity()),
            DiagnosticKindToString((llvm::DiagnosticKind)info.getKind()),
            os.str().c_str());
    }

    static const char* DiagnosticKindToString(llvm::DiagnosticKind kind)
    {
        switch (kind) {
#if !LLVM_VERSION_GE(3, 9)
            case llvm::DK_Bitcode:
                return "DK_Bitcode";
#endif
            case llvm::DK_InlineAsm:
                return "DK_InlineAsm";
            case llvm::DK_StackSize:
                return "DK_StackSize";
            case llvm::DK_Linker:
                return "DK_Linker";
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
            case llvm::DK_OptimizationFailure:
                return "DK_OptimizationFailure";
            case llvm::DK_MIRParser:
                return "DK_MIRParser";
            case llvm::DK_FirstPluginKind:
                return "DK_FirstPluginKind";
            default:
                return "DK_(?)";
        }
        YT_ABORT();
    }

    static const char* DiagnosticSeverityToString(llvm::DiagnosticSeverity severity)
    {
        switch (severity) {
            case llvm::DS_Error:
                return "DS_Error";
            case llvm::DS_Warning:
                return "DS_Warning";
            case llvm::DS_Remark:
                return "DS_Remark";
            case llvm::DS_Note:
                return "DS_Note";
            default:
                return "DS_(?)";
        }
        YT_ABORT();
    }

private:
    // RoutineRegistry is supposed to be a static object.
    TRoutineRegistry* const RoutineRegistry_;

    const EExecutionBackend ExecutionBackend_;

    llvm::LLVMContext Context_;
    llvm::Module* Module_;

    std::unique_ptr<llvm::ExecutionEngine> Engine_;

    std::set<TString> ExportedSymbols_;

    std::set<TString> LoadedFunctions_;
    std::set<TString> LoadedSymbols_;

    THashSet<TString> LoadedModules_;

    bool Compiled_ = false;

    llvm::SmallVector<char, 0> WebAssemblyBytecode_;
};

////////////////////////////////////////////////////////////////////////////////

TCGModulePtr TCGModule::Create(
    TRoutineRegistry* routineRegistry,
    EExecutionBackend backend,
    const TString& moduleName)
{
    return New<TCGModule>(std::make_unique<TImpl>(routineRegistry, backend, moduleName));
}

TCGModule::TCGModule(std::unique_ptr<TImpl> impl)
    : Impl_(std::move(impl))
{ }

TCGModule::~TCGModule() = default;

llvm::Module* TCGModule::GetModule() const
{
    return Impl_->GetModule();
}

llvm::FunctionCallee TCGModule::GetRoutine(const TString& symbol) const
{
    return Impl_->GetRoutine(symbol);
}

void TCGModule::ExportSymbol(const TString& name)
{
    Impl_->ExportSymbol(name);
}

llvm::LLVMContext& TCGModule::GetContext()
{
    return Impl_->GetContext();
}

uint64_t TCGModule::GetFunctionAddress(const TString& name)
{
    return Impl_->GetFunctionAddress(name);
}

void TCGModule::AddObjectFile(
    std::unique_ptr<llvm::object::ObjectFile> sharedObject)
{
    Impl_->AddObjectFile(std::move(sharedObject));
}

bool TCGModule::IsSymbolLoaded(const TString& symbol) const
{
    return Impl_->IsSymbolLoaded(symbol);
}

void TCGModule::AddLoadedSymbol(const TString& symbol)
{
    Impl_->AddLoadedSymbol(symbol);
}

bool TCGModule::IsFunctionLoaded(const TString& function) const
{
    return Impl_->IsFunctionLoaded(function);
}

void TCGModule::AddLoadedFunction(const TString& function)
{
    Impl_->AddLoadedFunction(function);
}

bool TCGModule::IsModuleLoaded(TRef data) const
{
    return Impl_->IsModuleLoaded(data);
}

void TCGModule::AddLoadedModule(TRef data)
{
    Impl_->AddLoadedModule(data);
}

void TCGModule::BuildWebAssembly()
{
    Impl_->BuildWebAssembly();
}

TRef TCGModule::GetWebAssemblyBytecode() const
{
    return Impl_->GetWebAssemblyBytecode();
}

EExecutionBackend TCGModule::GetBackend() const
{
    return Impl_->GetBackend();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen
