#include "stdafx.h"
#include "yson_file_service.h"
#include "tree_builder.h"
#include "ephemeral.h"
#include "virtual.h"
#include "convert.h"

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
        IServiceContextPtr underlyingContext,
        TClosure onReply)
        : UnderlyingContext(underlyingContext)
        , OnReply(onReply)
    {
        YASSERT(underlyingContext);
        YASSERT(!onReply.IsNull());
    }

    virtual NBus::IMessagePtr GetRequestMessage() const
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
        OnReply.Run();
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

    virtual std::vector<TSharedRef>& RequestAttachments()
    {
        return UnderlyingContext->RequestAttachments();
    }

    virtual std::vector<TSharedRef>& ResponseAttachments()
    {
        return UnderlyingContext->ResponseAttachments();
    }

    virtual IAttributeDictionary& RequestAttributes()
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

    virtual TClosure Wrap(TClosure action) 
    {
        return UnderlyingContext->Wrap(action);
    }

private:
    IServiceContextPtr UnderlyingContext;
    TClosure OnReply;

};

class TWriteBackService
    : public IYPathService
{
public:
    typedef TIntrusivePtr<TWriteBackService> TPtr;

    TWriteBackService(
        const Stroka& fileName,
        INodePtr root,
        IYPathServicePtr underlyingService)
        : FileName(fileName)
        , Root(MoveRV(root))
        , UnderlyingService(underlyingService)
    { }

    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb)
    {
        auto result = UnderlyingService->Resolve(path, verb);
        if (result.IsHere()) {
            return TResolveResult::Here(result.GetPath());
        } else {
            return TResolveResult::There(
                New<TWriteBackService>(FileName, Root, result.GetService()),
                result.GetPath());
        }
    }

    virtual void Invoke(IServiceContextPtr context)
    {
        auto wrappedContext =
            UnderlyingService->IsWriteRequest(context)
            ? New<TReplyInterceptorContext>(
                context,
                BIND(&TWriteBackService::SaveFile, MakeStrong(this)))
            : context;
        UnderlyingService->Invoke(wrappedContext);
    }

    virtual Stroka GetLoggingCategory() const
    {
        return UnderlyingService->GetLoggingCategory();
    }

    virtual bool IsWriteRequest(IServiceContextPtr context) const
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
            // TODO(babenko): make format yson serializable
            WriteYson(&stream, ~Root, EYsonFormat::Pretty);
        } catch (const std::exception& ex) {
            throw yexception() << Sprintf("Error saving YSON file %s\n%s",
                ~FileName.Quote(),
                ex.what());
        }
    }
};

} // namespace

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
        return TResolveResult::There(service, path);
    }

    virtual void Invoke(NRpc::IServiceContextPtr context)
    {
        UNUSED(context);
        YUNREACHABLE();
    }

    virtual Stroka GetLoggingCategory() const
    {
        return "YsonFileService";
    }

    virtual bool IsWriteRequest(IServiceContextPtr context) const
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
            return ConvertToNode(&stream);
        } catch (const std::exception& ex) {
            throw yexception() << Sprintf("Error loading YSON file %s\n%s",
                ~FileName.Quote(),
                ex.what());
        }
    }
};

TYPathServiceProducer CreateYsonFileProducer(const Stroka& fileName)
{
    return BIND([=] () -> IYPathServicePtr
        {
            return New<TYsonFileService>(fileName);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
