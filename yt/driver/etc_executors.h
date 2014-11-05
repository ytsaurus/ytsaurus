#pragma once

#include "executor.h"

#include <core/ytree/permission.h>

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

    virtual void DoExecute() override;
    virtual Stroka GetCommandName() const override;

};

////////////////////////////////////////////////////////////////////////////////

class TGCCollectExecutor
    : public TExecutor
{
public:
    TGCCollectExecutor();

private:
    virtual void DoExecute() override;
    virtual Stroka GetCommandName() const override;

};

////////////////////////////////////////////////////////////////////////////////

class TUpdateMembershipExecutor
    : public TRequestExecutor
{
public:
    TUpdateMembershipExecutor();

private:
    TUnlabeledStringArg MemberArg;
    TUnlabeledStringArg GroupArg;

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;

};

class TAddMemberExecutor
    : public TUpdateMembershipExecutor
{
private:
    virtual Stroka GetCommandName() const override;

};

class TRemoveMemberExecutor
    : public TUpdateMembershipExecutor
{
private:
    virtual Stroka GetCommandName() const override;

};

////////////////////////////////////////////////////////////////////////////////

class TCheckPermissionExecutor
    : public TTransactedExecutor
{
public:
    TCheckPermissionExecutor();

private:
    TUnlabeledStringArg UserArg;
    TCLAP::UnlabeledValueArg<NYTree::EPermission> PermissionArg;
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
