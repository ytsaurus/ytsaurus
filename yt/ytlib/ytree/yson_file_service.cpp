#include "stdafx.h"
#include "yson_file_service.h"
#include "serialize.h"
#include "tree_builder.h"
#include "ephemeral.h"

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
                ~New<TWriteBackService>(FileName, ~Root, ~UnderlyingService),
                result.GetPath());
        }
    }

    virtual void Invoke(IServiceContext* context)
    {
        auto wrappedContext = New<TReplyInterceptorContext>(
            context,
            ~FromMethod(&TWriteBackService::SaveFile, TPtr(this)));
        UnderlyingService->Invoke(~wrappedContext);
    }

    virtual Stroka GetLoggingCategory() const
    {
        return UnderlyingService->GetLoggingCategory();
    }

private:
    Stroka FileName;
    INode::TPtr Root;
    IYPathService::TPtr UnderlyingService;

    void SaveFile()
    {
        try {
            TOFStream stream(FileName);
            // TODO(babenko): make format configurable
            SerializeToYson(~Root, stream, TYsonWriter::EFormat::Pretty);
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
        
        // TODO(babenko): refactor using IYPathService::IsReadOnly
        auto service =
            IsReadOnly(verb)
            ? IYPathService::TPtr(root)
            : IYPathService::TPtr(New<TWriteBackService>(FileName, ~root, ~root));

        return TResolveResult::There(~service, path);
    }

    virtual void Invoke(NRpc::IServiceContext* context)
    {
        UNUSED(context);
        ythrow yexception() << "Direct invocation is forbidden";
    }

    virtual Stroka GetLoggingCategory() const
    {
        return YTreeLogger.GetCategory();
    }

private:
    Stroka FileName;

    INode::TPtr LoadFile()
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

    static bool IsReadOnly(const Stroka& verb)
    {
        return
            verb != "Set" &&
            verb != "Remove";
    }
};

TYPathServiceProvider::TPtr CreateYsonFileProvider(const Stroka& fileName)
{
    return FromFunctor([=] () -> IYPathService::TPtr
        {
            return New<TYsonFileService>(fileName);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
