
#include "wavm_private_imports.h"

#include <yt/yt/library/web_assembly/api/memory_pool.h>

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

TWebAssemblyMemoryPool::~TWebAssemblyMemoryPool()
{
    try {
        Clear();
    } catch (WAVM::Runtime::Exception* exception) {
        // In the case when free inside the vm results in an error
        // (for example, if the user's code causes the allocator to malfunction),
        // we catch the exception and skip the error, since we are already in the destructor.
        WAVM::Runtime::destroyException(exception);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
