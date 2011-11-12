#pragma once

#include "common.h"
#include "service.h"

#include "../bus/message.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TServiceContextBase
    : public IServiceContext
{
public:
    typedef TIntrusivePtr<TServiceContextBase> TPtr;

    virtual Stroka GetPath() const;
    virtual Stroka GetVerb() const;

    virtual void Reply(const TError& error);
    virtual bool IsReplied() const;

    virtual TSharedRef GetRequestBody() const;
    virtual void SetResponseBody(TBlob&& responseBody);

    virtual const yvector<TSharedRef>& GetRequestAttachments() const;
    virtual void SetResponseAttachments(const yvector<TSharedRef>& attachments);

    virtual void SetRequestInfo(const Stroka& info);
    virtual Stroka GetRequestInfo() const;

    virtual void SetResponseInfo(const Stroka& info);
    virtual Stroka GetResponseInfo();

    virtual IAction::TPtr Wrap(IAction* action);

protected:
    TServiceContextBase(
        const TRequestId& requestId,
        const Stroka& path,
        const Stroka& verb,
        NBus::IMessage* requestMessage);

    TRequestId RequestId;
    Stroka Path;
    Stroka Verb;
    IParamAction<NBus::IMessage*>::TPtr ResponseHandler;

    TSharedRef RequestBody;
    yvector<TSharedRef> RequestAttachments;
    bool Replied;

    TBlob ResponseBody;
    yvector<TSharedRef> ResponseAttachments;

    Stroka RequestInfo;
    Stroka ResponseInfo;

    static void AppendInfo(Stroka& lhs, const Stroka& rhs);

    virtual void DoReply(const TError& error, NBus::IMessage* responseMessage) = 0;

    virtual void LogRequest() = 0;
    virtual void LogResponse(const TError& error) = 0;
    virtual void LogException(const Stroka& message) = 0;

private:
    void WrapThunk(IAction::TPtr action) throw();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
