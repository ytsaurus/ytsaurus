
#include "wavm_private_imports.h"

#include <yt/yt/library/web_assembly/api/data_transfer.h>

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

TCopyGuard::~TCopyGuard()
{
    if (Compartment_ != nullptr && CopiedOffset_ != 0) {
        try {
            Compartment_->FreeBytes(CopiedOffset_);
        } catch (WAVM::Runtime::Exception* exception) {
            // In the case when free inside the vm results in an error
            // (for example, if the user's code causes the allocator to malfunction),
            // we catch the exception and skip the error, since we are already in the destructor.
            WAVM::Runtime::destroyException(exception);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
