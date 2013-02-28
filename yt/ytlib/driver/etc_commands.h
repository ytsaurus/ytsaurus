#pragma once

#include "command.h"

#include <ytlib/ytree/permission.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TUpdateMembershipRequest
    : public TRequest
{
    Stroka Group;
    Stroka Member;

    TUpdateMembershipRequest()
    {
        Register("group", Group);
        Register("member", Member);
    }
};

class TAddMemberCommand
    : public TTypedCommandBase<TUpdateMembershipRequest>
{
public:
    explicit TAddMemberCommand(ICommandContext* context)
        : TTypedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;
};

class TRemoveMemberCommand
    : public TTypedCommandBase<TUpdateMembershipRequest>
{
public:
    explicit TRemoveMemberCommand(ICommandContext* context)
        : TTypedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;
};

////////////////////////////////////////////////////////////////////////////////

struct TParseYPathRequest
    : public TRequest
{
    Stroka Path;

    TParseYPathRequest()
    {
        Register("path", Path);
    }
};

class TParseYPathCommand
    : public TTypedCommandBase<TParseYPathRequest>
{
public:
    explicit TParseYPathCommand(ICommandContext* context)
        : TTypedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;
};

////////////////////////////////////////////////////////////////////////////////

struct TCheckPermissionRequest
    : public TTransactedRequest
{
    Stroka User;
    NYPath::TRichYPath Path;
    NYTree::EPermission Permission;

    TCheckPermissionRequest()
    {
        Register("user", User);
        Register("permission", Permission);
        Register("path", Path);
    }
};

typedef TIntrusivePtr<TCheckPermissionRequest> TCheckPermissionRequestPtr;

class TCheckPersmissionCommand
    : public TTransactedCommandBase<TCheckPermissionRequest>
{
public:
    explicit TCheckPersmissionCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
