#pragma once

#include "service.h"

#include <core/rpc/rpc.pb.h>

#include <core/bus/message.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TServiceContextBase
    : public IServiceContext
{
public:
    virtual NBus::IMessagePtr GetRequestMessage() const override;

    virtual TRequestId GetRequestId() const override;
    
    virtual TNullable<TInstant> GetRequestStartTime() const override;
    virtual TNullable<TInstant> GetRetryStartTime() const override;
    
    virtual i64 GetPriority() const override;
    
    virtual const Stroka& GetService() const override;
    virtual const Stroka& GetVerb() const override;
    virtual const TRealmId& GetRealmId() const override;

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

    virtual const NProto::TRequestHeader& RequestHeader() const override;
    virtual NProto::TRequestHeader& RequestHeader() override;

    virtual void SetRequestInfo(const Stroka& info) override;
    virtual Stroka GetRequestInfo() const override;

    virtual void SetResponseInfo(const Stroka& info) override;
    virtual Stroka GetResponseInfo() override;

protected:
    TServiceContextBase(
        const NProto::TRequestHeader& header,
        NBus::IMessagePtr requestMessage);

    explicit TServiceContextBase(
        NBus::IMessagePtr requestMessage);

    NProto::TRequestHeader RequestHeader_;
    NBus::IMessagePtr RequestMessage;

    TRequestId RequestId;
    TRealmId RealmId;

    TSharedRef RequestBody;
    std::vector<TSharedRef> RequestAttachments_;
    std::unique_ptr<NYTree::IAttributeDictionary> RequestAttributes_;
    bool Replied;
    TError Error;

    TSharedRef ResponseBody;
    std::vector<TSharedRef> ResponseAttachments_;
    std::unique_ptr<NYTree::IAttributeDictionary> ResponseAttributes_;

    Stroka RequestInfo;
    Stroka ResponseInfo;

    virtual void DoReply(NBus::IMessagePtr responseMessage) = 0;

    virtual void LogRequest() = 0;
    virtual void LogResponse(const TError& error) = 0;

    static void AppendInfo(Stroka& lhs, const Stroka& rhs);

};

////////////////////////////////////////////////////////////////////////////////

class TServiceContextWrapper
    : public IServiceContext
{
public:
    explicit TServiceContextWrapper(IServiceContextPtr underlyingContext);

    virtual NBus::IMessagePtr GetRequestMessage() const override;

    virtual NRpc::TRequestId GetRequestId() const override;
    
    virtual TNullable<TInstant> GetRequestStartTime() const override;
    virtual TNullable<TInstant> GetRetryStartTime() const override;
    
    virtual i64 GetPriority() const override;

    virtual const Stroka& GetService() const override;
    virtual const Stroka& GetVerb() const override;
    virtual const TRealmId& GetRealmId() const override;

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

    virtual const NProto::TRequestHeader& RequestHeader() const override;
    virtual NProto::TRequestHeader& RequestHeader() override;

    using IServiceContext::SetRequestInfo;
    virtual void SetRequestInfo(const Stroka& info) override;

    virtual Stroka GetRequestInfo() const override;

    using IServiceContext::SetResponseInfo;
    virtual void SetResponseInfo(const Stroka& info) override;

    virtual Stroka GetResponseInfo() override;

protected:
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
