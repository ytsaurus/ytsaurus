#pragma once

#include "command.h"

#include <ytlib/ypath/rich.h>

#include <ytlib/cypress_client/public.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TGetRequest
    : public TTransactionalRequest
    , public TSuppressableAccessTrackingRequest
{
    NYPath::TRichYPath Path;
    std::vector<Stroka> Attributes;
    TNullable<i64> MaxSize;
    bool IgnoreOpaque;

    TGetRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("attributes", Attributes)
            .Default();
        RegisterParameter("max_size", MaxSize)
            .Default();
        RegisterParameter("ignore_opaque", IgnoreOpaque)
            .Default(false);
    }
};

typedef TIntrusivePtr<TGetRequest> TGetRequestPtr;

class TGetCommand
    : public TTypedCommand<TGetRequest>
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
    : public TTypedCommand<TSetRequest>
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
    : public TTypedCommand<TRemoveRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TListRequest
    : public TTransactionalRequest
    , public TSuppressableAccessTrackingRequest
{
    NYPath::TRichYPath Path;
    std::vector<Stroka> Attributes;
    TNullable<i64> MaxSize;

    TListRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("attributes", Attributes)
            .Default();
        RegisterParameter("max_size", MaxSize)
            .Default();
    }
};

typedef TIntrusivePtr<TListRequest> TListRequestPtr;

class TListCommand
    : public TTypedCommand<TListRequest>
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
    : public TTypedCommand<TCreateRequest>
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
    bool Waitable;

    TLockRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("mode", Mode)
            .Default(NCypressClient::ELockMode::Exclusive);
        RegisterParameter("waitable", Waitable)
            .Default(false);
    }
};

typedef TIntrusivePtr<TLockRequest> TLockRequestPtr;

class TLockCommand
    : public TTypedCommand<TLockRequest>
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
    bool Recursive;
    bool PreserveAccount;

    TCopyRequest()
    {
        RegisterParameter("source_path", SourcePath);
        RegisterParameter("destination_path", DestinationPath);
        RegisterParameter("recursive", Recursive)
            .Default(false);
        RegisterParameter("preserve_account", PreserveAccount)
            .Default(false);
    }
};

typedef TIntrusivePtr<TCopyRequest> TCopyRequestPtr;

class TCopyCommand
    : public TTypedCommand<TCopyRequest>
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
    bool Recursive;
    bool PreserveAccount;

    TMoveRequest()
    {
        RegisterParameter("source_path", SourcePath);
        RegisterParameter("destination_path", DestinationPath);
        RegisterParameter("recursive", Recursive)
            .Default(false);
        RegisterParameter("preserve_account", PreserveAccount)
            .Default(true);
    }
};

typedef TIntrusivePtr<TMoveRequest> TMoveRequestPtr;

class TMoveCommand
    : public TTypedCommand<TMoveRequest>
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
    : public TTypedCommand<TExistsRequest>
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
    : public TTypedCommand<TLinkRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////
} // namespace NDriver
} // namespace NYT

