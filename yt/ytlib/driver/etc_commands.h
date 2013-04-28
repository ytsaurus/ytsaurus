#pragma once

#include "command.h"

#include <ytlib/ytree/permission.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TUpdateMembershipRequest
    : public TMutatingRequest
{
    Stroka Group;
    Stroka Member;

    TUpdateMembershipRequest()
    {
        RegisterParameter("group", Group);
        RegisterParameter("member", Member);
    }
};

class TAddMemberCommand
    : public TTypedCommandBase<TUpdateMembershipRequest>
    , public TMutatingCommand
{
private:
    virtual void DoExecute() override;

};

class TRemoveMemberCommand
    : public TTypedCommandBase<TUpdateMembershipRequest>
    , public TMutatingCommand
{
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
        RegisterParameter("path", Path);
    }
};

class TParseYPathCommand
    : public TTypedCommandBase<TParseYPathRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TCheckPermissionRequest
    : public TTransactionalRequest
{
    Stroka User;
    NYPath::TRichYPath Path;
    NYTree::EPermission Permission;

    TCheckPermissionRequest()
    {
        RegisterParameter("user", User);
        RegisterParameter("permission", Permission);
        RegisterParameter("path", Path);
    }
};

typedef TIntrusivePtr<TCheckPermissionRequest> TCheckPermissionRequestPtr;

class TCheckPersmissionCommand
    : public TTypedCommandBase<TCheckPermissionRequest>
    , public TTransactionalCommand
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
