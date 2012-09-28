#pragma once

#include "executor.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TBuildSnapshotExecutor
    : public TExecutor
{
public:
    TBuildSnapshotExecutor();

    virtual Stroka GetCommandName() const;

private:
    TCLAP::SwitchArg SetReadOnlyArg;

    virtual EExitCode DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
