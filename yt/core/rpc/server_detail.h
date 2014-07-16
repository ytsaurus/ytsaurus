#pragma once

#include "service.h"
#include "server.h"

#include <core/concurrency/rw_spinlock.h>

#include <core/rpc/rpc.pb.h>

#include <core/logging/log.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TServiceContextBase
    : public IServiceContext
{
public:
    virtual TSharedRefArray GetRequestMessage() const override;

    virtual TRequestId GetRequestId() const override;
    
    virtual TNullable<TInstant> GetRequestStartTime() const override;
    virtual TNullable<TInstant> GetRetryStartTime() const override;
    
    virtual i64 GetPriority() const override;
    
    virtual const Stroka& GetService() const override;
    virtual const Stroka& GetMethod() const override;
    virtual const TRealmId& GetRealmId() const override;

    virtual bool IsReplied() const override;
    virtual bool IsOneWay() const override;

    virtual void Reply(const TError& error) override;
    virtual void Reply(TSharedRefArray responseMessage) override;

    virtual TSharedRefArray GetResponseMessage() const override;

    virtual const TError& GetError() const override;

    virtual TSharedRef GetRequestBody() const override;

    virtual TSharedRef GetResponseBody() override;
    virtual void SetResponseBody(const TSharedRef& responseBody) override;

    virtual std::vector<TSharedRef>& RequestAttachments() override;
    virtual std::vector<TSharedRef>& ResponseAttachments() override;

    virtual const NProto::TRequestHeader& RequestHeader() const override;
    virtual NProto::TRequestHeader& RequestHeader() override;

    virtual void SetRawRequestInfo(const Stroka& info) override;
    virtual void SetRawResponseInfo(const Stroka& info) override;

    virtual NLog::TLogger& GetLogger() override;

protected:
    std::unique_ptr<NProto::TRequestHeader> RequestHeader_;
    TSharedRefArray RequestMessage_;

    TRequestId RequestId_;
    TRealmId RealmId_;

    TSharedRef RequestBody_;
    std::vector<TSharedRef> RequestAttachments_;

    bool Replied_;
    TError Error_;

    TSharedRef ResponseBody_;
    std::vector<TSharedRef> ResponseAttachments_;

    Stroka RequestInfo_;
    Stroka ResponseInfo_;

    NLog::TLogger Logger;


    TServiceContextBase(
        std::unique_ptr<NProto::TRequestHeader> header,
        TSharedRefArray requestMessage,
        NLog::TLogger logger);

    TServiceContextBase(
        TSharedRefArray requestMessage,
        NLog::TLogger logger);

    virtual void DoReply() = 0;

    virtual void LogRequest() = 0;
    virtual void LogResponse(const TError& error) = 0;

    template <class... TArgs>
    static void AppendInfo(TStringBuilder* builder, const char* format, const TArgs&... args)
    {
        if (builder->GetLength() > 0) {
            builder->AppendString(STRINGBUF(", "));
        }
        builder->AppendFormat(format, args...);
    }

private:
    mutable TSharedRefArray ResponseMessage_; // cached


    void Initialize();

};

////////////////////////////////////////////////////////////////////////////////

class TServiceContextWrapper
    : public IServiceContext
{
public:
    explicit TServiceContextWrapper(IServiceContextPtr underlyingContext);

    virtual TSharedRefArray GetRequestMessage() const override;

    virtual NRpc::TRequestId GetRequestId() const override;
    
    virtual TNullable<TInstant> GetRequestStartTime() const override;
    virtual TNullable<TInstant> GetRetryStartTime() const override;
    
    virtual i64 GetPriority() const override;

    virtual const Stroka& GetService() const override;
    virtual const Stroka& GetMethod() const override;
    virtual const TRealmId& GetRealmId() const override;

    virtual bool IsOneWay() const;

    virtual bool IsReplied() const override;
    virtual void Reply(const TError& error) override;
    virtual void Reply(TSharedRefArray responseMessage) override;
    virtual TSharedRefArray GetResponseMessage() const override;

    virtual const TError& GetError() const override;

    virtual TSharedRef GetRequestBody() const override;

    virtual TSharedRef GetResponseBody() override;
    virtual void SetResponseBody(const TSharedRef& responseBody) override;

    virtual std::vector<TSharedRef>& RequestAttachments() override;
    virtual std::vector<TSharedRef>& ResponseAttachments() override;

    virtual const NProto::TRequestHeader& RequestHeader() const override;
    virtual NProto::TRequestHeader& RequestHeader() override;

    virtual void SetRawRequestInfo(const Stroka& info) override;
    virtual void SetRawResponseInfo(const Stroka& info) override;

    virtual NLog::TLogger& GetLogger() override;

protected:
    IServiceContextPtr UnderlyingContext_;

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
    virtual void Reply(TSharedRefArray responseMessage) override;

private:
    TClosure OnReply_;

};

////////////////////////////////////////////////////////////////////////////////

class TServerBase
    : public IServer
{
public:
    TServerBase();

    virtual void RegisterService(IServicePtr service) override;
    virtual void UnregisterService(IServicePtr service) override;
    
    virtual IServicePtr FindService(const TServiceId& serviceId) override;

    virtual void Configure(TServerConfigPtr config) override;

    virtual void Start() override;
    virtual void Stop() override;

protected:
    volatile bool Started_;

    NConcurrency::TReaderWriterSpinLock ServicesLock_;
    yhash_map<TServiceId, IServicePtr> ServiceMap_;

    virtual void DoStart();
    virtual void DoStop();

    std::vector<IServicePtr> FindServices(const Stroka& serviceName);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
