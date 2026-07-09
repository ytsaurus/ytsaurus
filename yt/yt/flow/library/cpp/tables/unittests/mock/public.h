#pragma once

#include <yt/yt/flow/library/cpp/tables/public.h>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TInMemoryInputMessages);
DECLARE_REFCOUNTED_CLASS(TInMemoryCompactPartitionOutputMessages);
DECLARE_REFCOUNTED_CLASS(TInMemoryCompactOutputMessages);
DECLARE_REFCOUNTED_CLASS(TInMemoryTimers);
DECLARE_REFCOUNTED_CLASS(TInMemoryKeyStates);
DECLARE_REFCOUNTED_CLASS(TInMemoryKeyVisitorStates);
DECLARE_REFCOUNTED_CLASS(TInMemoryPartitionStates);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
