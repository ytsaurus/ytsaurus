#pragma once

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESequoiaReign,
    ((InitialReign)                         (1))
    ((TransientCypressProxyRegistration)    (2))
);

static_assert(TEnumTraits<ESequoiaReign>::IsMonotonic, "Sequoia reign enum is not monotonic");

ESequoiaReign GetCurrentSequoiaReign() noexcept;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
