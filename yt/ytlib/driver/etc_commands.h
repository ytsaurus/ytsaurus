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
    , public TMutatingCommandMixin
{
public:
    explicit TAddMemberCommand(ICommandContext* context)
        : TTypedCommandBase(context)
        , TMutatingCommandMixin(context, Request)
    { }

private:
    virtual void DoExecute() override;
};

class TRemoveMemberCommand
    : public TTypedCommandBase<TUpdateMembershipRequest>
    , public TMutatingCommandMixin
{
public:
    explicit TRemoveMemberCommand(ICommandContext* context)
        : TTypedCommandBase(context)
        , TMutatingCommandMixin(context, Request)
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
        RegisterParameter("path", Path);
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
    , public TTransactionalCommandMixin
{
public:
    explicit TCheckPersmissionCommand(ICommandContext* context)
        : TTypedCommandBase(context)
        , TTransactionalCommandMixin(context, Request)
    { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
