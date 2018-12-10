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

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/MCJIT.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/IR/DiagnosticInfo.h>
#include <llvm/IR/DiagnosticPrinter.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Object/ObjectFile.h>
#include <llvm/Support/Host.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/CodeGen/Passes.h>
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/Transforms/Instrumentation.h>

#define PRINT_PASSES_TIME
#ifdef PRINT_PASSES_TIME
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

#ifdef _linux_

// This code works around NodeJS dlopen call.
// By reopening library once again, we ensure global symbol visibility
// required for the linker.

static int OnHeaderCallback(dl_phdr_info* info, size_t /*size*/, void* /*data*/)
{
    if (strstr(info->dlpi_name, "ytnode.node")) {
        dlopen(info->dlpi_name, RTLD_NOW | RTLD_GLOBAL);
    }

    return 0;
}

static void LoadDynamicLibrarySymbols()
{
    dl_iterate_phdr(OnHeaderCallback, nullptr);
}

#endif

class TCGMemoryManager
    : public llvm::SectionMemoryManager
{
public:
    explicit TCGMemoryManager(TRoutineRegistry* routineRegistry)
        : RoutineRegistry_(routineRegistry)
    {
#ifdef _linux_
        static std::once_flag onceFlag;
        std::call_once(onceFlag, &LoadDynamicLibrarySymbols);
#endif
    }

    virtual uint64_t getSymbolAddress(const std::string& name) override
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
    TImpl(TRoutineRegistry* routineRegistry, const TString& moduleName)
        : RoutineRegistry_(routineRegistry)
    {
        InitializeCodegen();

#if !LLVM_VERSION_GE(6, 0)
        Context_.setDiagnosticHandler(&TImpl::DiagnosticHandler, nullptr);
#else
        Context_.setDiagnosticHandlerCallBack(&TImpl::DiagnosticHandler, nullptr);
#endif

        // Infer host parameters.
        auto hostCpu = llvm::sys::getHostCPUName();
        auto hostTriple = llvm::Triple::normalize(
            llvm::sys::getProcessTriple()
#ifdef _win_
            + "-elf"
#endif
        );
#ifdef _darwin_
        // Modules generated with Clang contain macosx10.11.0 OS signature,
        // whereas LLVM modules contains darwin15.0.0.
        // So we rebuild triple to match with Clang object files.
        auto triple = llvm::Triple(hostTriple);
        unsigned maj, min, rev;
        triple.getMacOSXVersion(maj, min, rev);
        auto osName = llvm::Twine(Format("macosx%v.%v.%v", maj, min, rev));
        auto fixedTriple = llvm::Triple(triple.getArchName(), triple.getVendorName(), osName);
        hostTriple = llvm::Triple::normalize(fixedTriple.getTriple());
#endif

        // Create module.
        auto module = std::make_unique<llvm::Module>(moduleName.c_str(), Context_);
        module->setTargetTriple(hostTriple);
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
            .setMCPU(hostCpu)
            .setErrorStr(&what)
            .setTargetOptions(targetOptions);
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

    llvm::Constant* GetRoutine(const TString& symbol) const
    {
        auto type = RoutineRegistry_->GetTypeBuilder(symbol)(
            const_cast<llvm::LLVMContext&>(Context_));

        return Module_->getOrInsertFunction(symbol.c_str(), type);
    }

    void ExportSymbol(const TString& name)
    {
        YCHECK(ExportedSymbols_.insert(name).second);
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

private:
    void Finalize()
    {
        YCHECK(Compiled_ == false);
        Compile();
        Compiled_ = true;
    }

    void Compile()
    {
        using PassManager = llvm::legacy::PassManager;
        using FunctionPassManager = llvm::legacy::FunctionPassManager;

#ifdef PRINT_PASSES_TIME
        if (IsPassesTimeDumpEnabled()) {
            llvm::TimePassesIsEnabled = true;
        }
#endif

        LOG_DEBUG("Started compiling module");

        // See YT-8035 for details why we are clearing COMDAT.
        for (auto it = Module_->begin(), jt = Module_->end(); it != jt; ++it) {
            it->setComdat(nullptr);
        }

        Module_->getComdatSymbolTable().clear();

        if (IsIRDumpEnabled()) {
            llvm::errs() << "\n******** Before Optimization ***********************************\n";
#if !LLVM_VERSION_GE(5, 0)
            Module_->dump();
#else
            Module_->print(llvm::errs(), nullptr);
#endif
            llvm::errs() << "\n****************************************************************\n";
        }

        LOG_DEBUG("Verifying IR");
        YCHECK(!llvm::verifyModule(*Module_, &llvm::errs()));

        std::unique_ptr<PassManager> modulePassManager;
        std::unique_ptr<FunctionPassManager> functionPassManager;

        // Run DCE pass to strip unused code.
        LOG_DEBUG("Pruning dead code (ExportedSymbols: %v)", ExportedSymbols_);

        modulePassManager = std::make_unique<PassManager>();
#if !LLVM_VERSION_GE(3, 9)
        std::vector<const char*> exportedNames;
        for (const auto& exportedSymbol : ExportedSymbols_) {
            exportedNames.emplace_back(exportedSymbol.c_str());
        }
        modulePassManager->add(llvm::createInternalizePass(exportedNames));
#else
        modulePassManager->add(llvm::createInternalizePass([&] (const llvm::GlobalValue& gv) -> bool {
            auto name = TString(gv.getName().str());
            return ExportedSymbols_.count(name) > 0;
        }));
#endif
        modulePassManager->add(llvm::createGlobalDCEPass());
        modulePassManager->run(*Module_);

        // Now, setup optimization pipeline and run actual optimizations.
        LOG_DEBUG("Optimizing IR");

        llvm::PassManagerBuilder passManagerBuilder;
        passManagerBuilder.OptLevel = 0;
        passManagerBuilder.SizeLevel = 0;
        passManagerBuilder.Inliner = llvm::createFunctionInliningPass();

        functionPassManager = std::make_unique<FunctionPassManager>(Module_);
        if (NSan::MSanIsOn()) {
            functionPassManager->add(llvm::createMemorySanitizerPass());
        }
        passManagerBuilder.populateFunctionPassManager(*functionPassManager);

        functionPassManager->doInitialization();
        for (auto it = Module_->begin(), jt = Module_->end(); it != jt; ++it) {
            if (!it->isDeclaration()) {
                functionPassManager->run(*it);
            }
        }
        functionPassManager->doFinalization();

        modulePassManager = std::make_unique<PassManager>();
        passManagerBuilder.populateModulePassManager(*modulePassManager);

        modulePassManager->run(*Module_);

        if (IsIRDumpEnabled()) {
            llvm::errs() << "\n******** After Optimization ************************************\n";
#if !LLVM_VERSION_GE(5, 0)
            Module_->dump();
#else
            Module_->print(llvm::errs(), nullptr);
#endif
            llvm::errs() << "\n****************************************************************\n";
        }

        LOG_DEBUG("Finalizing module");

        Engine_->finalizeObject();
#ifdef PRINT_PASSES_TIME
        if (IsPassesTimeDumpEnabled()) {
            llvm::TimerGroup::printAll(llvm::errs());
        }
#endif
        LOG_DEBUG("Finished compiling module");
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
        Y_UNREACHABLE();
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
        Y_UNREACHABLE();
    }

private:
    // RoutineRegistry is supposed to be a static object.
    TRoutineRegistry* const RoutineRegistry_;

    llvm::LLVMContext Context_;
    llvm::Module* Module_;

    std::unique_ptr<llvm::ExecutionEngine> Engine_;

    std::set<TString> ExportedSymbols_;

    std::set<TString> LoadedFunctions_;
    std::set<TString> LoadedSymbols_;

    THashSet<TString> LoadedModules_;

    bool Compiled_ = false;
};

////////////////////////////////////////////////////////////////////////////////

TCGModulePtr TCGModule::Create(TRoutineRegistry* routineRegistry, const TString& moduleName)
{
    return New<TCGModule>(std::make_unique<TImpl>(routineRegistry, moduleName));
}

TCGModule::TCGModule(std::unique_ptr<TImpl> impl)
    : Impl_(std::move(impl))
{ }

TCGModule::~TCGModule() = default;

llvm::Module* TCGModule::GetModule() const
{
    return Impl_->GetModule();
}

llvm::Constant* TCGModule::GetRoutine(const TString& symbol) const
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen

