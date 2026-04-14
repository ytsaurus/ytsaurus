#include "bytecode.h"

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

bool TModuleBytecode::operator==(const TModuleBytecode& other) const
{
    return Format == other.Format && Data.ToStringBuf() == other.Data.ToStringBuf();
}

TModuleBytecode::operator size_t() const
{
    size_t result = 0;
    HashCombine(result, Format);
    HashCombine(result, Data.ToStringBuf());
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
