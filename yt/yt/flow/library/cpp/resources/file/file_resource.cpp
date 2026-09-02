#include "file_resource.h"

#include <yt/yt/flow/library/cpp/common/spec.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TFileResourceValidator::Validate(const TResourceSpec& spec)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        !spec.FileProviders.empty(),
        "File resource must configure at least one file provider");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
