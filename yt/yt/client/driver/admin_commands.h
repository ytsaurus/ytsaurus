#pragma once

#include "command.h"

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TBuildSnapshotCommand
    : public TTypedCommand<NApi::TBuildSnapshotOptions>
{
public:
    TBuildSnapshotCommand();

private:
    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TBuildMasterSnapshotsCommand
    : public TTypedCommand<NApi::TBuildMasterSnapshotsOptions>
{
public:
    TBuildMasterSnapshotsCommand();

private:
    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TSwitchLeaderCommand
    : public TTypedCommand<NApi::TSwitchLeaderOptions>
{
public:
    TSwitchLeaderCommand();

private:
    NHydra::TCellId CellId_;
    TString NewLeaderAddress_;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
