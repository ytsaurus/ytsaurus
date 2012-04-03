#include "source_location.h"

#include <cstdlib>

#ifdef _win_
// MSDN says to #include <intrin.h>, but that breaks the VS2005 build.
extern "C" {
    void* _ReturnAddress();
}
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TSourceLocation::TSourceLocation()
    : InstructionPointer(NULL)
    , Function("<unknown>")
    , File("<unknown>")
    , Line(-1)
{ }

TSourceLocation::TSourceLocation(
    const void* instructionPointer,
    const char* function,
    const char* file,
    int line)
    : InstructionPointer(instructionPointer)
    , Function(function)
    , File(file)
    , Line(line)
{ }

#ifdef _win_
__declspec(noinline)
#endif
const void* GetInstructionPointer()
{
#ifdef _win_
    return _ReturnAddress();
#else
    return __builtin_extract_return_addr(__builtin_return_address(0));
#endif        
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
