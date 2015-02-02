#include "stdafx.h"
#include "private.h"
#include "init.h"

#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/Threading.h>

#include <mutex>

#if defined(__GNUC__) && __GNUC__ == 4 && __GNUC_MINOR__ < 9
namespace std
{
    void __throw_out_of_range_fmt(char const*, ...)
    { }
}
#endif

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

