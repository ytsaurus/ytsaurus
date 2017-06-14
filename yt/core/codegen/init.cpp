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

// Shims to bridge llvm with Arcadia's zlib.
#ifndef YT_IN_ARCADIA
extern "C" {
int arc_compressBound(char*, unsigned long*, const char*, unsigned long, int);
int compressBound(char* dest, unsigned long* destLen, const char* source, unsigned long sourceLen, int level)
{
    return arc_compressBound(dest, destLen, source, sourceLen, level);
}

int arc_compress2(char*, unsigned long*, const char*, unsigned long, int);
int compress2(char* dest, unsigned long* destLen, const char* source, unsigned long sourceLen, int level)
{
    return arc_compress2(dest, destLen, source, sourceLen, level);
}

int arc_uncompress(char*, unsigned long*, const char*, unsigned long);
int uncompress(char* dest, unsigned long* destLen, const char* source, unsigned long sourceLen)
{
    return arc_uncompress(dest, destLen, source, sourceLen);
}

unsigned long arc_crc32(unsigned long, const unsigned char*, unsigned int);
unsigned long crc32(unsigned long crc, const unsigned char* buf, unsigned int len)
{
    return arc_crc32(crc, buf, len);
}
}
#endif

