#include "init.h"
#include "private.h"

#include <mutex>

#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/Threading.h>

namespace NYT::NCodegen {

////////////////////////////////////////////////////////////////////////////////

static void InitializeCodegenImpl()
{
    YT_VERIFY(llvm::llvm_is_multithreaded());
#if defined(_darwin_) && defined(__aarch64__)
    // The bundled LLVM macOS configuration still selects the X86 native target.
    LLVMInitializeAArch64TargetInfo();
    LLVMInitializeAArch64Target();
    LLVMInitializeAArch64TargetMC();
    LLVMInitializeAArch64AsmParser();
    LLVMInitializeAArch64AsmPrinter();
#else
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmParser();
    llvm::InitializeNativeTargetAsmPrinter();
#endif

    LLVMInitializeWebAssemblyTarget();
    LLVMInitializeWebAssemblyTargetInfo();
    LLVMInitializeWebAssemblyTargetMC();
    LLVMInitializeWebAssemblyAsmParser();
    LLVMInitializeWebAssemblyAsmPrinter();
}

void InitializeCodegen()
{
    static std::once_flag onceFlag;
    std::call_once(onceFlag, &InitializeCodegenImpl);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen
