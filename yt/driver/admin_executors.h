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

    virtual EExitCode Execute(const std::vector<std::string>& args);

private:
    TCLAP::SwitchArg SetReadOnlyArg;

    virtual Stroka GetCommandName() const;
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
