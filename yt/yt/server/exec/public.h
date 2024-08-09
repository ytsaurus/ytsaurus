#pragma once

#include <library/cpp/yt/memory/intrusive_ptr.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NExec {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IUserJobSynchronizer)
DECLARE_REFCOUNTED_STRUCT(IUserJobSynchronizerClient)

DECLARE_REFCOUNTED_CLASS(TUserJobSynchronizer)

DEFINE_ENUM(EProgramExitCode,
    ((ExecutorStderrOpenError)      (100))
    ((ExecutorStderrDuplicateError) (101))
    ((ExecutorError)                (102))
    ((ExecveError)                  (103))
    ((JobProxyNotificationError)    (104))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExec
