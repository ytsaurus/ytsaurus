#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
#endif
#undef HELPERS_INL_H_

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

template <class TSpec>
TIntrusivePtr<TSpec> ParseOperationSpec(NYTree::IMapNodePtr specNode)
{
    auto spec = New<TSpec>();
    try {
        spec->Load(specNode);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing operation spec") << ex;
    }
    return spec;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
