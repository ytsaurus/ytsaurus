#include "phdr_cache.h"

#include <util/system/sanitizers.h>

#if defined(__linux__) && !defined(_tsan_enabled_)

#include <link.h>
#include <dlfcn.h>
#include <assert.h>
#include <vector>
#include <cstddef>

#include <yt/core/misc/assert.h>

namespace NYT {
namespace NPhdrCache {

////////////////////////////////////////////////////////////////////////////////

// This is adapted from
// https://github.com/scylladb/seastar/blob/master/core/exception_hacks.hh
// https://github.com/scylladb/seastar/blob/master/core/exception_hacks.cc

using TDlIterateFunc = int (*) (int (*callback) (struct dl_phdr_info *info, size_t size, void *data), void *data);

TDlIterateFunc GetOriginalDlIteratePhdr()
{
    static auto result = [] {
        auto func = reinterpret_cast<TDlIterateFunc>(dlsym(RTLD_NEXT, "dl_iterate_phdr"));
        YCHECK(func);
        return func;
    }();
    return result;
}

// Never destroyed to avoid races with static destructors.
std::vector<dl_phdr_info>* PhdrCache;

////////////////////////////////////////////////////////////////////////////////

} // namespace NPhdrCache
} // namespace NYT

extern "C"
#ifndef __clang__
[[gnu::visibility("default")]]
[[gnu::externally_visible]]
#endif
int dl_iterate_phdr(int (*callback) (struct dl_phdr_info* info, size_t size, void* data), void* data)
{
    using namespace NYT::NPhdrCache;
    if (!PhdrCache) {
        // Cache is not yet populated, pass through to the original function.
        return GetOriginalDlIteratePhdr()(callback, data);
    }
    int result = 0;
    for (auto& info : *PhdrCache) {
        result = callback(&info, offsetof(dl_phdr_info, dlpi_adds), data);
        if (result != 0) {
            break;
        }
    }
    return result;
}

#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void EnablePhdrCache()
{
#if defined(__linux__) && !defined(_tsan_enabled_)
    using namespace NPhdrCache;
    // Fill out ELF header cache for access without locking.
    // This assumes no dynamic object loading/unloading after this point
    PhdrCache = new std::vector<dl_phdr_info>();
    NSan::MarkAsIntentionallyLeaked(PhdrCache);
    GetOriginalDlIteratePhdr()([] (struct dl_phdr_info *info, size_t /*size*/, void* /*data*/) {
        PhdrCache->push_back(*info);
        return 0;
    }, nullptr);
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
