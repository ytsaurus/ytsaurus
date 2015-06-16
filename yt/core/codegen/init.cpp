#include "stdafx.h"
#include "private.h"
#include "init.h"

#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/Threading.h>

#include <mutex>

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

