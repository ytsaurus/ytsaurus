#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/flow/library/cpp/native_client/public.h>
#include <yt/yt/flow/library/cpp/serializer/public.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/ypath/public.h>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TInternalState);
DECLARE_REFCOUNTED_STRUCT(TContext);

DECLARE_REFCOUNTED_STRUCT(IPartitionStates);
DECLARE_REFCOUNTED_CLASS(TPartitionStates);
DECLARE_REFCOUNTED_STRUCT(IKeyStates);
DECLARE_REFCOUNTED_CLASS(TKeyStates);
DECLARE_REFCOUNTED_STRUCT(IKeyVisitorStates);
DECLARE_REFCOUNTED_CLASS(TKeyVisitorStates);
DECLARE_REFCOUNTED_STRUCT(ITimers);
DECLARE_REFCOUNTED_CLASS(TTimers);
DECLARE_REFCOUNTED_STRUCT(IInputMessages);
DECLARE_REFCOUNTED_CLASS(TInputMessages);
DECLARE_REFCOUNTED_CLASS(TCompactInputMessages);

DECLARE_REFCOUNTED_STRUCT(ICompactPartitionOutputMessages);
DECLARE_REFCOUNTED_STRUCT(ICompactOutputMessages);

DECLARE_REFCOUNTED_STRUCT(TTransactionManagerContext);
DECLARE_REFCOUNTED_CLASS(TTransactionManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
