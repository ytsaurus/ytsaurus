#include "stack_trace.h"
#include "raw_formatter.h"

#include <yt/build/config.h>

#ifdef HAVE_DLFCN_H
#   include <dlfcn.h>
#endif

#ifdef HAVE_CXXABI_H
#   include <cxxabi.h>
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

int GetSymbolInfo(void* pc, char* buffer, int length)
{
    TBaseFormatter formatter(buffer, length);

#if defined(HAVE_DLFCN_H)
    // See http://www.codesourcery.com/cxx-abi/abi.html#mangling
    // And, yes, dladdr() is not async signal safe. We can substitute it
    // with hand-written symbolization code from google-glog in case of any trouble.
    Dl_info info;
    if (!dladdr(pc, &info)) {
        return 0;
    }

    /*
     * typedef struct {
     *     const char *dli_fname;  // Pathname of shared object that
     *                             // contains address
     *     void       *dli_fbase;  // Address at which shared object
     *                             // is loaded
     *     const char *dli_sname;  // Name of nearest symbol with address
     *                             // lower than addr
     *     void       *dli_saddr;  // Exact address of symbol named
     *                             // in dli_sname
     * } Dl_info;
     *
     * If no symbol matching addr could be found, then dli_sname and dli_saddr are set to NULL.
     */

    if (info.dli_sname && info.dli_saddr) {
        formatter.AppendString("<");
#if defined(HAVE_CXXABI_H)
        int demangleStatus = 0;

        if (info.dli_sname[0] == '_' && info.dli_sname[1] == 'Z') {
            // This is also not async signal safe.
            // But (ta-dah!) we can replace it with symbolization code from google-glob.
            char* demangledName = abi::__cxa_demangle(info.dli_sname, 0, 0, &demangleStatus);
            if (demangleStatus == 0) {
                formatter.AppendString(demangledName);
            } else {
                formatter.AppendString(info.dli_sname);
            }
            free(demangledName);
        } else {
            formatter.AppendString(info.dli_sname);
        }
#else
        formatter.AppendString(info.dli_sname);
#endif
        formatter.AppendString("+");
        formatter.AppendNumber((char*)pc - (char*)info.dli_saddr);
        formatter.AppendString(">");
        formatter.AppendString(" ");
    }

    if (info.dli_fname && info.dli_fbase) {
        formatter.AppendString("(");
        formatter.AppendString(info.dli_fname);
        formatter.AppendString("+");
        formatter.AppendNumber((char*)pc - (char*)info.dli_fbase);
        formatter.AppendString(")");
    }
#else
    formatter.AppendString("0x");
    formatter.AppendNumber((uintptr_t)pc, 16);
#endif
    return formatter.GetBytesWritten();
}

void DumpStackFrameInfo(TBaseFormatter* formatter, void* pc)
{
    formatter->AppendString("@ ");
    const int width = (sizeof(void*) == 8 ? 12 : 8) + 2;
    // +2 for "0x"; 12 for x86_64 because higher bits are always zeroed.
    formatter->AppendNumberAsHexWithPadding(reinterpret_cast<uintptr_t>(pc), width);
    formatter->AppendString(" ");
    // Get the symbol from the previous address of PC,
    // because PC may be in the next function.
    formatter->Advance(GetSymbolInfo(
        reinterpret_cast<char*>(pc) - 1,
        formatter->GetCursor(),
        formatter->GetBytesRemaining()));
    formatter->AppendString("\n");
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
