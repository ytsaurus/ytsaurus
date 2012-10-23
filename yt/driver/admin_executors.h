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

private:
    TCLAP::SwitchArg SetReadOnlyArg;

    virtual EExitCode DoExecute() override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TGCCollectExector
    : public TExecutor
{
public:
    TGCCollectExector();

private:
    virtual EExitCode DoExecute() override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
