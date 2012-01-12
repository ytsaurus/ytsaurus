#pragma once

#include "common.h"
#include "service.h"
#include "rpc.pb.h"

#include <ytlib/bus/message.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TServiceContextBase
    : public IServiceContext
{
public:
    typedef TIntrusivePtr<TServiceContextBase> TPtr;

    virtual NBus::IMessage::TPtr GetRequestMessage() const;

    virtual Stroka GetPath() const;
    virtual Stroka GetVerb() const;

    virtual bool IsReplied() const;
    virtual bool IsOneWay() const;
    virtual void Reply(const TError& error);
    virtual TError GetError() const;

    virtual TSharedRef GetRequestBody() const;
    virtual void SetResponseBody(const TSharedRef& responseBody);

    virtual const yvector<TSharedRef>& RequestAttachments() const;
    virtual yvector<TSharedRef>& ResponseAttachments();

    virtual void SetRequestInfo(const Stroka& info);
    virtual Stroka GetRequestInfo() const;

    virtual void SetResponseInfo(const Stroka& info);
    virtual Stroka GetResponseInfo();

    virtual IAction::TPtr Wrap(IAction* action);

protected:
    TServiceContextBase(
        const NProto::TRequestHeader& header,
        NBus::IMessage* requestMessage);

    TServiceContextBase(NBus::IMessage* requestMessage);

    TRequestId RequestId;
    Stroka Path;
    Stroka Verb;
    NBus::IMessage::TPtr RequestMessage;

    TSharedRef RequestBody;
    yvector<TSharedRef> RequestAttachments_;
    bool OneWay;
    bool Replied;
    TError Error;

    TSharedRef ResponseBody;
    yvector<TSharedRef> ResponseAttachments_;

    Stroka RequestInfo;
    Stroka ResponseInfo;

    virtual void DoReply(const TError& error, NBus::IMessage* responseMessage) = 0;

    virtual void LogRequest() = 0;
    virtual void LogResponse(const TError& error) = 0;
    virtual void LogException(const Stroka& message) = 0;

    static void AppendInfo(Stroka& lhs, const Stroka& rhs);

private:
    void WrapThunk(IAction::TPtr action) throw();
    void CheckRepliable() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
