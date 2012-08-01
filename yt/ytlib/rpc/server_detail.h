#pragma once

#include "service.h"
#include <ytlib/rpc/rpc.pb.h>

#include <ytlib/bus/message.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TServiceContextBase
    : public IServiceContext
{
public:
    virtual NBus::IMessagePtr GetRequestMessage() const OVERRIDE;

    virtual const TRequestId& GetRequestId() const OVERRIDE;
    virtual const Stroka& GetPath() const OVERRIDE;
    virtual const Stroka& GetVerb() const OVERRIDE;

    virtual bool IsReplied() const OVERRIDE;
    virtual bool IsOneWay() const OVERRIDE;
    
    virtual void Reply(const TError& error) OVERRIDE;
    virtual void Reply(NBus::IMessagePtr responseMessage) OVERRIDE;
    
    virtual const TError& GetError() const OVERRIDE;

    virtual TSharedRef GetRequestBody() const OVERRIDE;
    
    virtual TSharedRef GetResponseBody() OVERRIDE;
    virtual void SetResponseBody(const TSharedRef& responseBody) OVERRIDE;

    virtual std::vector<TSharedRef>& RequestAttachments() OVERRIDE;
    virtual std::vector<TSharedRef>& ResponseAttachments() OVERRIDE;

    virtual NYTree::IAttributeDictionary& RequestAttributes() OVERRIDE;
    virtual NYTree::IAttributeDictionary& ResponseAttributes() OVERRIDE;

    virtual void SetRequestInfo(const Stroka& info) OVERRIDE;
    virtual Stroka GetRequestInfo() const OVERRIDE;

    virtual void SetResponseInfo(const Stroka& info) OVERRIDE;
    virtual Stroka GetResponseInfo() OVERRIDE;

    virtual TClosure Wrap(TClosure action) OVERRIDE;

protected:
    TServiceContextBase(
        const NProto::TRequestHeader& header,
        NBus::IMessagePtr requestMessage);

    TServiceContextBase(NBus::IMessagePtr requestMessage);

    TRequestId RequestId;
    Stroka Path;
    Stroka Verb;
    NBus::IMessagePtr RequestMessage;

    TSharedRef RequestBody;
    std::vector<TSharedRef> RequestAttachments_;
    TAutoPtr<NYTree::IAttributeDictionary> RequestAttributes_;
    bool OneWay;
    bool Replied;
    TError Error;

    TSharedRef ResponseBody;
    std::vector<TSharedRef> ResponseAttachments_;
    TAutoPtr<NYTree::IAttributeDictionary> ResponseAttributes_;

    Stroka RequestInfo;
    Stroka ResponseInfo;

    virtual void DoReply(NBus::IMessagePtr responseMessage) = 0;

    virtual void LogRequest() = 0;
    virtual void LogResponse(const TError& error) = 0;

    static void AppendInfo(Stroka& lhs, const Stroka& rhs);

private:
    void WrapThunk(TClosure action);
    void OnException(const TError& error);
    void CheckRepliable() const;

};

////////////////////////////////////////////////////////////////////////////////

class TReplyInterceptorContext
    : public IServiceContext
{
public:
    TReplyInterceptorContext(
        IServiceContextPtr underlyingContext,
        TClosure onReply);

    virtual NBus::IMessagePtr GetRequestMessage() const OVERRIDE;

    virtual const NRpc::TRequestId& GetRequestId() const OVERRIDE;

    virtual const Stroka& GetPath() const OVERRIDE;
    virtual const Stroka& GetVerb() const OVERRIDE;

    virtual bool IsOneWay() const;

    virtual bool IsReplied() const OVERRIDE;
    virtual void Reply(const TError& error) OVERRIDE;
    virtual void Reply(NBus::IMessagePtr responseMessage) OVERRIDE;

    virtual const TError& GetError() const OVERRIDE;

    virtual TSharedRef GetRequestBody() const OVERRIDE;

    virtual TSharedRef GetResponseBody() OVERRIDE;
    virtual void SetResponseBody(const TSharedRef& responseBody) OVERRIDE;

    virtual std::vector<TSharedRef>& RequestAttachments() OVERRIDE;
    virtual std::vector<TSharedRef>& ResponseAttachments() OVERRIDE;

    virtual NYTree::IAttributeDictionary& RequestAttributes() OVERRIDE;
    virtual NYTree::IAttributeDictionary& ResponseAttributes() OVERRIDE;

    virtual void SetRequestInfo(const Stroka& info) OVERRIDE;
    virtual Stroka GetRequestInfo() const OVERRIDE;

    virtual void SetResponseInfo(const Stroka& info) OVERRIDE;
    virtual Stroka GetResponseInfo() OVERRIDE;

    virtual TClosure Wrap(TClosure action);

private:
    IServiceContextPtr UnderlyingContext;
    TClosure OnReply;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
