#include "symbolize.h"
#include "util/system/compiler.h"

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

void Symbolize(NProto::Profile* profile, bool filesOnly)
{
    Y_UNUSED(profile, filesOnly);
}

std::pair<void*, void*> GetVdsoRange()
{
    return {nullptr, nullptr};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
