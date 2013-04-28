#pragma once

#include "command.h"

#include <ytlib/ypath/rich.h>

#include <ytlib/cypress_client/public.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TGetRequest
    : public TTransactionalRequest
{
    NYPath::TRichYPath Path;
    std::vector<Stroka> Attributes;

    TGetRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("attributes", Attributes)
            .Default();
    }
};

typedef TIntrusivePtr<TGetRequest> TGetRequestPtr;

class TGetCommand
    : public TTypedCommandBase<TGetRequest>
    , public TTransactionalCommand
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TSetRequest
    : public TTransactionalRequest
    , public TMutatingRequest
{
    NYPath::TRichYPath Path;

    TSetRequest()
    {
        RegisterParameter("path", Path);
    }
};

typedef TIntrusivePtr<TSetRequest> TSetRequestPtr;

class TSetCommand
    : public TTypedCommandBase<TSetRequest>
    , public TTransactionalCommand
    , public TMutatingCommand
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TRemoveRequest
    : public TTransactionalRequest
    , public TMutatingRequest
{
    NYPath::TRichYPath Path;
    bool Recursive;
    bool Force;

    TRemoveRequest()
    {
        RegisterParameter("path", Path);
        // TODO(ignat): fix all places that use true default value
        // and change default value to false
        RegisterParameter("recursive", Recursive)
            .Default(true);
        RegisterParameter("force", Force)
            .Default(false);
    }
};

typedef TIntrusivePtr<TRemoveRequest> TRemoveRequestPtr;

class TRemoveCommand
    : public TTypedCommandBase<TRemoveRequest>
    , public TTransactionalCommand
    , public TMutatingCommand
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TListRequest
    : public TTransactionalRequest
{
    NYPath::TRichYPath Path;
    std::vector<Stroka> Attributes;

    TListRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("attributes", Attributes)
            .Default();
    }
};

typedef TIntrusivePtr<TListRequest> TListRequestPtr;

class TListCommand
    : public TTypedCommandBase<TListRequest>
    , public TTransactionalCommand
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TCreateRequest
    : public TTransactionalRequest
    , public TMutatingRequest
{
    TNullable<NYPath::TRichYPath> Path;
    NObjectClient::EObjectType Type;
    NYTree::INodePtr Attributes;
    bool Recursive;
    bool IgnoreExisting;

    TCreateRequest()
    {
        RegisterParameter("path", Path)
            .Default(Null);
        RegisterParameter("type", Type);
        RegisterParameter("attributes", Attributes)
            .Default(nullptr);
        RegisterParameter("recursive", Recursive)
            .Default(false);
        RegisterParameter("ignore_existing", IgnoreExisting)
            .Default(false);
    }
};

typedef TIntrusivePtr<TCreateRequest> TCreateRequestPtr;

class TCreateCommand
    : public TTypedCommandBase<TCreateRequest>
    , public TTransactionalCommand
    , public TMutatingCommand
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TLockRequest
    : public TTransactionalRequest
    , public TMutatingRequest
{
    NYPath::TRichYPath Path;
    NCypressClient::ELockMode Mode;

    TLockRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("mode", Mode)
            .Default(NCypressClient::ELockMode::Exclusive);
    }
};

typedef TIntrusivePtr<TLockRequest> TLockRequestPtr;

class TLockCommand
    : public TTypedCommandBase<TLockRequest>
    , public TTransactionalCommand
    , public TMutatingCommand
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TCopyRequest
    : public TTransactionalRequest
    , public TMutatingRequest
{
    NYPath::TRichYPath SourcePath;
    NYPath::TRichYPath DestinationPath;

    TCopyRequest()
    {
        RegisterParameter("source_path", SourcePath);
        RegisterParameter("destination_path", DestinationPath);
    }
};

typedef TIntrusivePtr<TCopyRequest> TCopyRequestPtr;

class TCopyCommand
    : public TTypedCommandBase<TCopyRequest>
    , public TTransactionalCommand
    , public TMutatingCommand
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TMoveRequest
    : public TTransactionalRequest
    , public TMutatingRequest
{
    NYPath::TRichYPath SourcePath;
    NYPath::TRichYPath DestinationPath;

    TMoveRequest()
    {
        RegisterParameter("source_path", SourcePath);
        RegisterParameter("destination_path", DestinationPath);
    }
};

typedef TIntrusivePtr<TMoveRequest> TMoveRequestPtr;

class TMoveCommand
    : public TTypedCommandBase<TMoveRequest>
    , public TTransactionalCommand
    , public TMutatingCommand
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TExistsRequest
    : public TTransactionalRequest
{
    NYPath::TRichYPath Path;

    TExistsRequest()
    {
        RegisterParameter("path", Path);
    }
};

typedef TIntrusivePtr<TExistsRequest> TExistsRequestPtr;

class TExistsCommand
    : public TTypedCommandBase<TExistsRequest>
    , public TTransactionalCommand
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TLinkRequest
    : public TTransactionalRequest
    , public TMutatingRequest
{
    NYPath::TRichYPath LinkPath;
    NYPath::TRichYPath TargetPath;
    NYTree::INodePtr Attributes;
    bool Recursive;
    bool IgnoreExisting;

    TLinkRequest()
    {
        RegisterParameter("link_path", LinkPath);
        RegisterParameter("target_path", TargetPath);
        RegisterParameter("attributes", Attributes)
            .Default(nullptr);
        RegisterParameter("recursive", Recursive)
            .Default(false);
        RegisterParameter("ignore_existing", IgnoreExisting)
            .Default(false);
    }
};

typedef TIntrusivePtr<TLinkRequest> TLinkRequestPtr;

class TLinkCommand
    : public TTypedCommandBase<TLinkRequest>
    , public TTransactionalCommand
    , public TMutatingCommand
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////
} // namespace NDriver
} // namespace NYT

