#pragma once

#include <library/cpp/yt/memory/ref_counted.h>
#include <library/cpp/yt/misc/guid.h>

namespace NYT::NQueryClient {

using TRowsetId = TGuid;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IJoinTreeNode);
DECLARE_REFCOUNTED_STRUCT(IJoinTree);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
