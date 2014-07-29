#pragma once

#include "command.h"

#include <core/ytree/permission.h>

#include <ytlib/ypath/rich.h>

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
    : public TTypedCommand<TUpdateMembershipRequest>
{
private:
    virtual void DoExecute() override;

};

class TRemoveMemberCommand
    : public TTypedCommand<TUpdateMembershipRequest>
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
    : public TTypedCommand<TParseYPathRequest>
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

class TCheckPermissionCommand
    : public TTypedCommand<TCheckPermissionRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
