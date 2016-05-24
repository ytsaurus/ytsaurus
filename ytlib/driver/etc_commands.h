#pragma once

#include "command.h"

#include <yt/ytlib/ypath/rich.h>

#include <yt/core/ytree/permission.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
class TUpdateMembershipCommand
    : public TTypedCommand<TOptions>
{
protected:
    Stroka Group;
    Stroka Member;

    TUpdateMembershipCommand()
    {
        this->RegisterParameter("group", Group);
        this->RegisterParameter("member", Member);
    }

};

class TAddMemberCommand
    : public TUpdateMembershipCommand<NApi::TAddMemberOptions>
{
public:
    void Execute(ICommandContextPtr context);

};

class TRemoveMemberCommand
    : public TUpdateMembershipCommand<NApi::TRemoveMemberOptions>
{
public:
    void Execute(ICommandContextPtr context);

};

class TParseYPathCommand
    : public TCommandBase
{
private:
    Stroka Path;

public:
    TParseYPathCommand()
    {
        RegisterParameter("path", Path);
    }

    void Execute(ICommandContextPtr context);

};

class TGetVersionCommand
    : public TCommandBase
{
public:
    void Execute(ICommandContextPtr context);

};

class TCheckPermissionCommand
    : public TTypedCommand<NApi::TCheckPermissionOptions>
{
private:
    Stroka User;
    NYPath::TRichYPath Path;
    NYTree::EPermission Permission;

public:
    TCheckPermissionCommand()
    {
        RegisterParameter("user", User);
        RegisterParameter("permission", Permission);
        RegisterParameter("path", Path);

    }

    void Execute(ICommandContextPtr context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
