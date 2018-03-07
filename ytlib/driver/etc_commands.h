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
    TString Group;
    TString Member;

    TUpdateMembershipCommand()
    {
        this->RegisterParameter("group", Group);
        this->RegisterParameter("member", Member);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAddMemberCommand
    : public TUpdateMembershipCommand<NApi::TAddMemberOptions>
{
private:
    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TRemoveMemberCommand
    : public TUpdateMembershipCommand<NApi::TRemoveMemberOptions>
{
private:
    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TParseYPathCommand
    : public TCommandBase
{
public:
    TParseYPathCommand();

private:
    TString Path;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetVersionCommand
    : public TCommandBase
{
private:
    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCheckPermissionCommand
    : public TTypedCommand<NApi::TCheckPermissionOptions>
{
public:
    TCheckPermissionCommand();

private:
    TString User;
    NYPath::TRichYPath Path;
    NYTree::EPermission Permission;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

struct TExecuteBatchOptions
    : public NApi::TMutatingOptions
{
    int Concurrency;
};

class TExecuteBatchCommand
    : public TTypedCommand<TExecuteBatchOptions>
{
public:
    TExecuteBatchCommand();

private:
    class TRequest
        : public NYTree::TYsonSerializable
    {
    public:
        TString Command;
        NYTree::IMapNodePtr Parameters;
        NYTree::INodePtr Input;

        TRequest();
    };

    using TRequestPtr = TIntrusivePtr<TRequest>;

    std::vector<TRequestPtr> Requests;

    class TRequestExecutor;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
