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
    virtual NBus::IMessagePtr GetRequestMessage() const override;

    virtual const TRequestId& GetRequestId() const override;
    virtual const Stroka& GetPath() const override;
    virtual const Stroka& GetVerb() const override;

    virtual bool IsReplied() const override;
    virtual bool IsOneWay() const override;
    
    virtual void Reply(const TError& error) override;
    virtual void Reply(NBus::IMessagePtr responseMessage) override;
    
    virtual const TError& GetError() const override;

    virtual TSharedRef GetRequestBody() const override;
    
    virtual TSharedRef GetResponseBody() override;
    virtual void SetResponseBody(const TSharedRef& responseBody) override;

    virtual std::vector<TSharedRef>& RequestAttachments() override;
    virtual std::vector<TSharedRef>& ResponseAttachments() override;

    virtual NYTree::IAttributeDictionary& RequestAttributes() override;
    virtual NYTree::IAttributeDictionary& ResponseAttributes() override;

    virtual void SetRequestInfo(const Stroka& info) override;
    virtual Stroka GetRequestInfo() const override;

    virtual void SetResponseInfo(const Stroka& info) override;
    virtual Stroka GetResponseInfo() override;

    virtual TClosure Wrap(const TClosure& action) override;

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
    void WrapThunk(const TClosure& action);
    void OnException(const TError& error);
    void CheckRepliable() const;

};

////////////////////////////////////////////////////////////////////////////////

class TServiceContextWrapper
    : public IServiceContext
{
public:
    explicit TServiceContextWrapper(IServiceContextPtr underlyingContext);

    virtual NBus::IMessagePtr GetRequestMessage() const override;

    virtual const NRpc::TRequestId& GetRequestId() const override;

    virtual const Stroka& GetPath() const override;
    virtual const Stroka& GetVerb() const override;

    virtual bool IsOneWay() const;

    virtual bool IsReplied() const override;
    virtual void Reply(const TError& error) override;
    virtual void Reply(NBus::IMessagePtr responseMessage) override;

    virtual const TError& GetError() const override;

    virtual TSharedRef GetRequestBody() const override;

    virtual TSharedRef GetResponseBody() override;
    virtual void SetResponseBody(const TSharedRef& responseBody) override;

    virtual std::vector<TSharedRef>& RequestAttachments() override;
    virtual std::vector<TSharedRef>& ResponseAttachments() override;

    virtual NYTree::IAttributeDictionary& RequestAttributes() override;
    virtual NYTree::IAttributeDictionary& ResponseAttributes() override;

    virtual void SetRequestInfo(const Stroka& info) override;
    virtual Stroka GetRequestInfo() const override;

    virtual void SetResponseInfo(const Stroka& info) override;
    virtual Stroka GetResponseInfo() override;

    virtual TClosure Wrap(const TClosure& action) override;

private:
    IServiceContextPtr UnderlyingContext;

};

////////////////////////////////////////////////////////////////////////////////

class TReplyInterceptorContext
    : public TServiceContextWrapper
{
public:
    TReplyInterceptorContext(
        IServiceContextPtr underlyingContext,
        TClosure onReply);

    virtual void Reply(const TError& error) override;
    virtual void Reply(NBus::IMessagePtr responseMessage) override;

private:
    TClosure OnReply;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
