#include "init.h"
#include "private.h"

#include <mutex>

#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/Threading.h>

namespace NYT {
namespace NCodegen {

////////////////////////////////////////////////////////////////////////////////

static void InitializeCodegenImpl()
{
    YCHECK(llvm::llvm_is_multithreaded());
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmParser();
    llvm::InitializeNativeTargetAsmPrinter();
}

void InitializeCodegen()
{
    static std::once_flag onceFlag;
    std::call_once(onceFlag, &InitializeCodegenImpl);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCodegen
} // namespace NYT

