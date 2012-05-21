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
    virtual NBus::IMessage::TPtr GetRequestMessage() const;

    virtual const TRequestId& GetRequestId() const;
    virtual const Stroka& GetPath() const;
    virtual const Stroka& GetVerb() const;

    virtual bool IsReplied() const;
    virtual bool IsOneWay() const;
    virtual void Reply(const TError& error);
    virtual TError GetError() const;

    virtual TSharedRef GetRequestBody() const;
    virtual void SetResponseBody(const TSharedRef& responseBody);

    virtual yvector<TSharedRef>& RequestAttachments();
    virtual yvector<TSharedRef>& ResponseAttachments();

    virtual NYTree::IAttributeDictionary& RequestAttributes();
    virtual NYTree::IAttributeDictionary& ResponseAttributes();

    virtual void SetRequestInfo(const Stroka& info);
    virtual Stroka GetRequestInfo() const;

    virtual void SetResponseInfo(const Stroka& info);
    virtual Stroka GetResponseInfo();

    virtual TClosure Wrap(TClosure action);

protected:
    TServiceContextBase(
        const NProto::TRequestHeader& header,
        NBus::IMessage::TPtr requestMessage);

    TServiceContextBase(NBus::IMessage::TPtr requestMessage);

    TRequestId RequestId;
    Stroka Path;
    Stroka Verb;
    NBus::IMessage::TPtr RequestMessage;

    TSharedRef RequestBody;
    yvector<TSharedRef> RequestAttachments_;
    TAutoPtr<NYTree::IAttributeDictionary> RequestAttributes_;
    bool OneWay;
    bool Replied;
    TError Error;

    TSharedRef ResponseBody;
    yvector<TSharedRef> ResponseAttachments_;
    TAutoPtr<NYTree::IAttributeDictionary> ResponseAttributes_;

    Stroka RequestInfo;
    Stroka ResponseInfo;

    virtual void DoReply(const TError& error, NBus::IMessage::TPtr responseMessage) = 0;

    virtual void LogRequest() = 0;
    virtual void LogResponse(const TError& error) = 0;

    static void AppendInfo(Stroka& lhs, const Stroka& rhs);

private:
    void WrapThunk(TClosure action);
    void OnException(const TError& error);
    void CheckRepliable() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
