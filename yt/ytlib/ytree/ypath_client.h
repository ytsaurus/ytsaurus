#pragma once

#include "common.h"
#include "ypath_service.h"
#include "ytree.h"

#include <ytlib/misc/ref.h>
#include <ytlib/misc/property.h>
#include <ytlib/bus/message.h>
#include <ytlib/rpc/client.h>
#include <ytlib/actions/action_util.h>

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
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Verb);
    DEFINE_BYVAL_RW_PROPERTY(TYPath, Path);
    DEFINE_BYREF_RW_PROPERTY(yvector<TSharedRef>, Attachments);

public:
    typedef TIntrusivePtr<TYPathRequest> TPtr;
    
    TYPathRequest(const Stroka& verb);

    NBus::IMessage::TPtr Serialize();

protected:
    virtual TBlob SerializeBody() const = 0;

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

    TTypedYPathRequest(const Stroka& verb)
        : TYPathRequest(verb)
    { }

protected:
    virtual TBlob SerializeBody() const
    {
        NLog::TLogger& Logger = YTreeLogger;
        TBlob blob;
        if (!SerializeProtobuf(this, &blob)) {
            LOG_FATAL("Error serializing request body");
        }
        return blob;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYPathResponse
    : public TRefCountedBase
{
    DEFINE_BYREF_RW_PROPERTY(yvector<TSharedRef>, Attachments);
    DEFINE_BYVAL_RW_PROPERTY(TError, Error);

public:
    typedef TIntrusivePtr<TYPathResponse> TPtr;

    void Deserialize(NBus::IMessage* message);

    int GetErrorCode() const;
    bool IsOK() const;

    void ThrowIfError() const;

protected:
    virtual void DeserializeBody(const TRef& data) = 0;

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
    virtual void DeserializeBody(const TRef& data)
    {
        NLog::TLogger& Logger = YTreeLogger;
        if (!DeserializeProtobuf(this, data)) {
            LOG_FATAL("Error deserializing response body");
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_YPATH_PROXY_METHOD(ns, method) \
    typedef ::NYT::NYTree::TTypedYPathRequest<ns::TReq##method, ns::TRsp##method> TReq##method; \
    typedef ::NYT::NYTree::TTypedYPathResponse<ns::TReq##method, ns::TRsp##method> TRsp##method; \
    \
    static TReq##method::TPtr method(const NYT::NYTree::TYPath& path) \
    { \
        auto req = New<TReq##method>(#method); \
        req->SetPath(path); \
        return req; \
    }

////////////////////////////////////////////////////////////////////////////////

//! Asynchronously executes an untyped YPath verb against a given service.
TFuture<NBus::IMessage::TPtr>::TPtr
ExecuteVerb(
    NBus::IMessage* requestMessage,
    IYPathProcessor* processor);

//! Asynchronously executes a typed YPath requested against a given service.
template <class TTypedRequest>
TIntrusivePtr< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >
ExecuteVerb(
    TTypedRequest* request,
    IYPathProcessor* processor);

//! Asynchronously executes "Get" verb. 
TFuture< TValueOrError<TYson> >::TPtr AsyncYPathGet(IYPathService* rootService, const TYPath& path);

//! Synchronously executes "Get" verb. Throws if an error has occurred.
TYson SyncYPathGet(IYPathService* rootService, const TYPath& path);

//! Synchronously executes "GetNode" verb. Throws if an error has occurred.
INode::TPtr SyncYPathGetNode(IYPathService* rootService, const TYPath& path);

//! Synchronously executes "Set" verb. Throws if an error has occurred.
void SyncYPathSet(IYPathService* rootService, const TYPath& path, const TYson& value);

//! Synchronously executes "SetNode" verb. Throws if an error has occurred.
void SyncYPathSetNode(IYPathService* rootService, const TYPath& path, INode* value);

//! Synchronously executes "Remove" verb. Throws if an error has occurred.
void SyncYPathRemove(IYPathService* rootService, const TYPath& path);

//! Synchronously executes "List" verb. Throws if an error has occurred.
yvector<Stroka> SyncYPathList(IYPathService* rootService, const TYPath& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

#define YPATH_CLIENT_INL_H_
#include "ypath_client-inl.h"
#undef YPATH_CLIENT_INL_H_
