#include "allocation_tags_hooks.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

Y_WEAK const TAllocationTagsHooks& GetAllocationTagsHooks()
{
    static const TAllocationTagsHooks hooks{
        .CreateAllocationTags = [] () -> void* {
            return nullptr;
        },
        .CopyAllocationTags = [] (void* /*opaque*/) -> void* {
            return nullptr;
        },
        .DestroyAllocationTags = [] (void* /*opaque*/) {
        },
        .ComputeAllocationTagsHash = [] (void* /*opaque*/) -> size_t {
            return 0;
        },
        .ReadAllocationTags = [] (void* /*opaque*/) -> TRange<TAllocationTag> {
            return {};
        },
    };
    return hooks;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
