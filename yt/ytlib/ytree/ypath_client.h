#pragma once

#include "common.h"
#include "ypath_service.h"

#include "../misc/ref.h"
#include "../misc/property.h"
#include "../bus/message.h"
#include "../rpc/client.h"
#include "../actions/action_util.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYPathRequest;

template <class TRequestMessage, class TResponseMessage>
class TTypedYPathRequest;

class TYPathResponse;

template <class TRequestMessage, class TResponseMessage>
class TTypedYPathResponse;

////////////////////////////////////////////////////////////////////////////////

class TYPathRequest
    : public TRefCountedBase
{
    DECLARE_BYVAL_RO_PROPERTY(Stroka, Verb);
    DECLARE_BYVAL_RW_PROPERTY(TYPath, Path);
    DECLARE_BYREF_RW_PROPERTY(yvector<TSharedRef>, Attachments);

public:
    typedef TIntrusivePtr<TYPathRequest> TPtr;
    
    TYPathRequest(const Stroka& verb, TYPath path);

    NBus::IMessage::TPtr Serialize();

protected:
    virtual bool SerializeBody(TBlob* data) const = 0;

};

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponseMessage>
class TTypedYPathRequest
    : public TYPathRequest
    , public TRequestMessage
{
public:
    typedef TTypedYPathResponse<TRequestMessage, TResponseMessage> TTypedResponse;
    typedef TIntrusivePtr< TTypedYPathRequest<TRequestMessage, TResponseMessage> > TPtr;

    TTypedYPathRequest(const Stroka& verb, TYPath path)
        : TYPathRequest(verb, path)
    { }

protected:
    virtual bool SerializeBody(TBlob* data) const
    {
        return SerializeProtobuf(this, data);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYPathResponse
    : public TRefCountedBase
{
    DECLARE_BYREF_RW_PROPERTY(yvector<TSharedRef>, Attachments);
    DECLARE_BYVAL_RW_PROPERTY(NRpc::TError, Error);

public:
    typedef TIntrusivePtr<TYPathResponse> TPtr;

    void Deserialize(NBus::IMessage* message);

    int GetErrorCode() const;
    bool IsOK() const;

    void ThrowIfError() const;

protected:
    virtual bool DeserializeBody(TRef data) = 0;

};

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponseMessage>
class TTypedYPathResponse
    : public TYPathResponse
    , public TResponseMessage
{
public:
    typedef TIntrusivePtr< TTypedYPathResponse<TRequestMessage, TResponseMessage> > TPtr;

protected:
    virtual bool DeserializeBody(TRef data)
    {
        return DeserializeProtobuf(this, data);
    }

};

////////////////////////////////////////////////////////////////////////////////

#define YPATH_PROXY_METHOD(ns, method) \
    typedef ::NYT::NYTree::TTypedYPathRequest<ns::TReq##method, ns::TRsp##method> TReq##method; \
    typedef ::NYT::NYTree::TTypedYPathResponse<ns::TReq##method, ns::TRsp##method> TRsp##method; \
    \
    static TReq##method::TPtr method(::NYT::NYTree::TYPath path) \
    { \
        return New<TReq##method>(#method, path); \
    }

////////////////////////////////////////////////////////////////////////////////

//! Executes a YPath verb against a local service.
template <class TTypedRequest>
TIntrusivePtr< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >
ExecuteYPath(IYPathService* rootService, TTypedRequest* request);

//! Executes "Get" verb synchronously. Throws if an error has occurred.
TYson SyncExecuteYPathGet(IYPathService* rootService, TYPath path);

//! Executes "Set" verb synchronously. Throws if an error has occurred.
void SyncExecuteYPathSet(IYPathService* rootService, TYPath path, const TYson& value);

//! Executes "Remove" verb synchronously. Throws if an error has occurred.
void SyncExecuteYPathRemove(IYPathService* rootService, TYPath path);

//! Executes "List" verb synchronously. Throws if an error has occurred.
yvector<Stroka> SyncExecuteYPathList(IYPathService* rootService, TYPath path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT


// TODO: move to ypath_client-inl.h

#include "ypath_detail.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class TTypedRequest, class TTypedResponse>
void OnYPathResponse(
    const TYPathResponseHandlerParam& param,
    TIntrusivePtr< TFuture< TIntrusivePtr<TTypedResponse> > > asyncResponse,
    const Stroka& verb,
    TYPath resolvedPath)
{
    auto response = New<TTypedResponse>();
    response->Deserialize(~param.Message);
    if (!response->IsOK()) {
        auto error = response->GetError();
        Stroka message = Sprintf("Error executing YPath operation (Verb: %s, ResolvedPath: %s)\n%s",
            ~verb,
            ~resolvedPath,
            ~error.GetMessage());
        response->SetError(NRpc::TError(error.GetCode(), message));
    }
    asyncResponse->Set(response);
}

template <class TTypedRequest>
TIntrusivePtr< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >
ExecuteYPath(IYPathService* rootService, TTypedRequest* request)
{
    TYPath path = request->GetPath();
    Stroka verb = request->GetVerb();

    IYPathService::TPtr suffixService;
    TYPath suffixPath;
    ResolveYPath(rootService, path, verb, &suffixService, &suffixPath);

    // TODO: can we avoid this?
    request->SetPath(suffixPath);

    auto requestMessage = request->Serialize();
    auto asyncResponse = New< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >();

    auto context = CreateYPathContext(
        ~requestMessage,
        suffixPath,
        verb,
        YTreeLogger.GetCategory(),
        ~FromMethod(
            &OnYPathResponse<TTypedRequest, typename TTypedRequest::TTypedResponse>,
            asyncResponse,
            verb,
            ComputeResolvedYPath(path, suffixPath)));

    try {
        suffixService->Invoke(~context);
    } catch (const NRpc::TServiceException& ex) {
        context->Reply(NRpc::TError(
            EYPathErrorCode(EYPathErrorCode::GenericError),
            ex.what()));
    }

    return asyncResponse;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
