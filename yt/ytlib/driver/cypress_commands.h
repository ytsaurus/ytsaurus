#pragma once

#include "command.h"

#include <ytlib/ypath/rich.h>

#include <ytlib/cypress_client/public.h>

namespace NYT {
namespace NDriver {
    
////////////////////////////////////////////////////////////////////////////////

struct TGetRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath Path;
    std::vector<Stroka> Attributes;

    TGetRequest()
    {
        Register("path", Path);
        Register("attributes", Attributes)
            .Default();
    }
};

typedef TIntrusivePtr<TGetRequest> TGetRequestPtr;

class TGetCommand
    : public TTransactedCommandBase<TGetRequest>
{
public:
    explicit TGetCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
        , TUntypedCommandBase(context)
   { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TSetRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath Path;

    TSetRequest()
    {
        Register("path", Path);
    }
};

typedef TIntrusivePtr<TSetRequest> TSetRequestPtr;

class TSetCommand
    : public TTransactedCommandBase<TSetRequest>
{
public:
    explicit TSetCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
        , TUntypedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TRemoveRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath Path;

    TRemoveRequest()
    {
        Register("path", Path);
    }
};

typedef TIntrusivePtr<TRemoveRequest> TRemoveRequestPtr;

class TRemoveCommand
    : public TTransactedCommandBase<TRemoveRequest>
{
public:
    explicit TRemoveCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
        , TUntypedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TListRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath Path;
    std::vector<Stroka> Attributes;

    TListRequest()
    {
        Register("path", Path);
        Register("attributes", Attributes)
            .Default();
    }
};

typedef TIntrusivePtr<TListRequest> TListRequestPtr;

class TListCommand
    : public TTransactedCommandBase<TListRequest>
{
public:
    explicit TListCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
        , TUntypedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TCreateRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath Path;
    NObjectClient::EObjectType Type;

    TCreateRequest()
    {
        Register("path", Path);
        Register("type", Type);
    }
};

typedef TIntrusivePtr<TCreateRequest> TCreateRequestPtr;

class TCreateCommand
    : public TTransactedCommandBase<TCreateRequest>
{
public:
    explicit TCreateCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
        , TUntypedCommandBase(context)
   { }

private:
    virtual void DoExecute() override;
};

////////////////////////////////////////////////////////////////////////////////

struct TLockRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath Path;
    NCypressClient::ELockMode Mode;

    TLockRequest()
    {
        Register("path", Path);
        Register("mode", Mode)
            .Default(NCypressClient::ELockMode::Exclusive);
    }
};

typedef TIntrusivePtr<TLockRequest> TLockRequestPtr;

class TLockCommand
    : public TTransactedCommandBase<TLockRequest>
{
public:
    explicit TLockCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
        , TUntypedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TCopyRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath SourcePath;
    NYPath::TRichYPath DestinationPath;

    TCopyRequest()
    {
        Register("source_path", SourcePath);
        Register("destination_path", DestinationPath);
    }
};

typedef TIntrusivePtr<TCopyRequest> TCopyRequestPtr;

class TCopyCommand
    : public TTransactedCommandBase<TCopyRequest>
{
public:
    explicit TCopyCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
        , TUntypedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TMoveRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath SourcePath;
    NYPath::TRichYPath DestinationPath;

    TMoveRequest()
    {
        Register("source_path", SourcePath);
        Register("destination_path", DestinationPath);
    }
};

typedef TIntrusivePtr<TMoveRequest> TMoveRequestPtr;

class TMoveCommand
    : public TTransactedCommandBase<TMoveRequest>
{
public:
    explicit TMoveCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
        , TUntypedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TExistsRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath Path;

    TExistsRequest()
    {
        Register("path", Path);
    }
};

typedef TIntrusivePtr<TExistsRequest> TExistsRequestPtr;

class TExistsCommand
    : public TTransactedCommandBase<TExistsRequest>
{
public:
    explicit TExistsCommand(ICommandContext* context)
        : TTransactedCommandBase(context)
        , TUntypedCommandBase(context)
   { }

private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

