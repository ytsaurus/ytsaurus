#include "stdafx.h"
#include "yson_file_service.h"
#include "serialize.h"
#include "tree_builder.h"
#include "ephemeral.h"

#include <ytlib/rpc/service.h>

namespace NYT {
namespace NYTree {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

namespace {

// TODO(babenko): consider moving to some general place
class TReplyInterceptorContext
    : public IServiceContext
{
public:
    TReplyInterceptorContext(
        IServiceContext* underlyingContext,
        IAction* onReply)
        : UnderlyingContext(underlyingContext)
        , OnReply(onReply)
    {
        YASSERT(underlyingContext);
        YASSERT(onReply);
    }

    virtual NBus::IMessage::TPtr GetRequestMessage() const
    {
        return UnderlyingContext->GetRequestMessage();
    }

    virtual const NRpc::TRequestId& GetRequestId() const
    {
        return UnderlyingContext->GetRequestId();
    }

    virtual const Stroka& GetPath() const
    {
        return UnderlyingContext->GetPath();
    }

    virtual const Stroka& GetVerb() const
    {
        return UnderlyingContext->GetVerb();
    }

    virtual bool IsOneWay() const
    {
        return UnderlyingContext->IsOneWay();
    }

    virtual bool IsReplied() const
    {
        return UnderlyingContext->IsReplied();
    }

    virtual void Reply(const TError& error)
    {
        UnderlyingContext->Reply(error);
        OnReply->Do();
    }

    virtual TError GetError() const
    {
        return UnderlyingContext->GetError();
    }

    virtual TSharedRef GetRequestBody() const
    {
        return UnderlyingContext->GetRequestBody();
    }

    virtual void SetResponseBody(const TSharedRef& responseBody)
    {
        UnderlyingContext->SetResponseBody(responseBody);
    }

    virtual const yvector<TSharedRef>& RequestAttachments() const
    {
        return UnderlyingContext->RequestAttachments();
    }

    virtual yvector<TSharedRef>& ResponseAttachments()
    {
        return UnderlyingContext->ResponseAttachments();
    }

    virtual const IAttributeDictionary& RequestAttributes() const
    {
        return UnderlyingContext->RequestAttributes();
    }

    virtual IAttributeDictionary& ResponseAttributes()
    {
        return UnderlyingContext->ResponseAttributes();
    }

    virtual void SetRequestInfo(const Stroka& info)
    {
       UnderlyingContext->SetRequestInfo(info);
    }

    virtual Stroka GetRequestInfo() const
    {
        return UnderlyingContext->GetRequestInfo();
    }

    virtual void SetResponseInfo(const Stroka& info)
    {
        UnderlyingContext->SetRequestInfo(info);
    }

    virtual Stroka GetResponseInfo()
    {
        return UnderlyingContext->GetRequestInfo();
    }

    virtual IAction::TPtr Wrap(IAction* action) 
    {
        return UnderlyingContext->Wrap(action);
    }

private:
    IServiceContext::TPtr UnderlyingContext;
    IAction::TPtr OnReply;

};

class TWriteBackService
    : public IYPathService
{
public:
    typedef TIntrusivePtr<TWriteBackService> TPtr;

    TWriteBackService(
        const Stroka& fileName,
        INode* root,
        IYPathService* underlyingService)
        : FileName(fileName)
        , Root(root)
        , UnderlyingService(underlyingService)
    { }

    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb)
    {
        auto result = UnderlyingService->Resolve(path, verb);
        if (result.IsHere()) {
            return TResolveResult::Here(result.GetPath());
        } else {
            return TResolveResult::There(
                ~New<TWriteBackService>(FileName, ~Root, ~result.GetService()),
                result.GetPath());
        }
    }

    virtual void Invoke(IServiceContext* context)
    {
        auto wrappedContext =
            UnderlyingService->IsWriteRequest(context)
            ? New<TReplyInterceptorContext>(
                context,
                ~FromMethod(&TWriteBackService::SaveFile, TPtr(this)))
            : IServiceContext::TPtr(context);
        UnderlyingService->Invoke(~wrappedContext);
    }

    virtual Stroka GetLoggingCategory() const
    {
        return UnderlyingService->GetLoggingCategory();
    }

    virtual bool IsWriteRequest(IServiceContext* context) const
    {
        return UnderlyingService->IsWriteRequest(context);
    }

private:
    Stroka FileName;
    INodePtr Root;
    IYPathServicePtr UnderlyingService;

    void SaveFile()
    {
        try {
            TOFStream stream(FileName);
            // TODO(babenko): make format configurable
            SerializeToYson(~Root, stream, EYsonFormat::Pretty);
        } catch (const std::exception& ex) {
            throw yexception() << Sprintf("Error saving YSON file %s\n%s",
                ~FileName.Quote(),
                ex.what());
        }
    }
};

} // namespace <anonymous>

class TYsonFileService
    : public IYPathService
{
public:
    TYsonFileService(const Stroka& fileName)
        : FileName(fileName)
    { }

    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb)
    {
        UNUSED(verb);

        auto root = LoadFile();
        auto service = New<TWriteBackService>(FileName, ~root, ~root);
        return TResolveResult::There(~service, path);
    }

    virtual void Invoke(NRpc::IServiceContext* context)
    {
        UNUSED(context);
        YUNREACHABLE();
    }

    virtual Stroka GetLoggingCategory() const
    {
        return YTreeLogger.GetCategory();
    }

    virtual bool IsWriteRequest(IServiceContext* context) const
    {
        UNUSED(context);
        YUNREACHABLE();
    }

private:
    Stroka FileName;

    INodePtr LoadFile()
    {
        try {
            TIFStream stream(FileName);
           return DeserializeFromYson(&stream);
        } catch (const std::exception& ex) {
            throw yexception() << Sprintf("Error loading YSON file %s\n%s",
                ~FileName.Quote(),
                ex.what());
        }
    }
};

TYPathServiceProvider CreateYsonFileProvider(const Stroka& fileName)
{
    return FromFunctor([=] () -> IYPathServicePtr
        {
            return New<TYsonFileService>(fileName);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
