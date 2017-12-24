#pragma once

#include "public.h"
#include "ypath_service.h"

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref.h>

#include <yt/core/rpc/client.h>

#include <yt/core/ytree/proto/ypath.pb.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYPathRequest
   : public NRpc::IClientRequest
{
public:
    explicit TYPathRequest(const NRpc::NProto::TRequestHeader& header);

    TYPathRequest(
        const TString& service,
        const TString& method,
        const NYPath::TYPath& path,
        bool mutating);

    virtual NRpc::TRequestId GetRequestId() const override;
    virtual NRpc::TRealmId GetRealmId() const override;
    virtual const TString& GetMethod() const override;
    virtual const TString& GetService() const override;

    virtual const TString& GetUser() const;
    virtual void SetUser(const TString& user);

    virtual bool GetRetry() const override;
    virtual void SetRetry(bool value) override;

    virtual NRpc::TMutationId GetMutationId() const override;
    virtual void SetMutationId(const NRpc::TMutationId& id) override;

    virtual size_t GetHash() const override;

    virtual NRpc::EMultiplexingBand GetMultiplexingBand() const override;
    virtual void SetMultiplexingBand(NRpc::EMultiplexingBand band) override;

    virtual const NRpc::NProto::TRequestHeader& Header() const override;
    virtual NRpc::NProto::TRequestHeader& Header() override;

    virtual TSharedRefArray Serialize() override;

protected:
    NRpc::NProto::TRequestHeader Header_;
    std::vector<TSharedRef> Attachments_;

    virtual bool IsHeavy() const override;

    virtual TSharedRef SerializeBody() const = 0;

};

DEFINE_REFCOUNTED_TYPE(TYPathRequest)

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponseMessage>
class TTypedYPathRequest
    : public TYPathRequest
    , public TRequestMessage
{
public:
    typedef TTypedYPathResponse<TRequestMessage, TResponseMessage> TTypedResponse;

    explicit TTypedYPathRequest(const NRpc::NProto::TRequestHeader& header)
        : TYPathRequest(header)
    { }

    TTypedYPathRequest(
        const TString& service,
        const TString& method,
        const NYPath::TYPath& path,
        bool mutating)
        : TYPathRequest(
            service,
            method,
            path,
            mutating)
    { }

protected:
    virtual TSharedRef SerializeBody() const override
    {
        return SerializeProtoToRefWithEnvelope(*this);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYPathResponse
    : public TRefCounted
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::vector<TSharedRef>, Attachments);

public:
    void Deserialize(TSharedRefArray message);

protected:
    virtual void DeserializeBody(const TRef& data);

};

DEFINE_REFCOUNTED_TYPE(TYPathResponse)

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponseMessage>
class TTypedYPathResponse
    : public TYPathResponse
    , public TResponseMessage
{
protected:
    virtual void DeserializeBody(const TRef& data) override
    {
        DeserializeProtoWithEnvelope(this, data);
    }
};

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_YPATH_PROXY(name) \
    static const ::NYT::NRpc::TServiceDescriptor& GetDescriptor() \
    { \
        static const auto Descriptor = ::NYT::NRpc::TServiceDescriptor(#name); \
        return Descriptor; \
    }

#define DEFINE_YPATH_PROXY_METHOD_IMPL(ns, method, isMutating) \
    typedef ::NYT::NYTree::TTypedYPathRequest<ns::TReq##method, ns::TRsp##method> TReq##method; \
    typedef ::NYT::NYTree::TTypedYPathResponse<ns::TReq##method, ns::TRsp##method> TRsp##method; \
    typedef ::NYT::TIntrusivePtr<TReq##method> TReq##method##Ptr; \
    typedef ::NYT::TIntrusivePtr<TRsp##method> TRsp##method##Ptr; \
    typedef ::NYT::TErrorOr<TRsp##method##Ptr> TErrorOrRsp##method##Ptr; \
    \
    static TReq##method##Ptr method(const NYT::NYPath::TYPath& path = NYT::NYPath::TYPath()) \
    { \
        return New<TReq##method>(GetDescriptor().ServiceName, #method, path, isMutating); \
    }

#define DEFINE_YPATH_PROXY_METHOD(ns, method) \
    DEFINE_YPATH_PROXY_METHOD_IMPL(ns, method, false)

#define DEFINE_MUTATING_YPATH_PROXY_METHOD(ns, method) \
    DEFINE_YPATH_PROXY_METHOD_IMPL(ns, method, true)

////////////////////////////////////////////////////////////////////////////////

const TYPath& GetRequestYPath(const NRpc::NProto::TRequestHeader& header);
void SetRequestYPath(NRpc::NProto::TRequestHeader* header, const TYPath& path);

//! Runs a sequence of IYPathService::Resolve calls aimed to discover the
//! ultimate endpoint responsible for serving a given request.
void ResolveYPath(
    const IYPathServicePtr& rootService,
    const NRpc::IServiceContextPtr& context,
    IYPathServicePtr* suffixService,
    TYPath* suffixPath);

//! Asynchronously executes an untyped request against a given service.
TFuture<TSharedRefArray>
ExecuteVerb(
    const IYPathServicePtr& service,
    const TSharedRefArray& requestMessage);

//! Asynchronously executes a request against a given service.
void ExecuteVerb(
    const IYPathServicePtr& service,
    const NRpc::IServiceContextPtr& context);

//! Asynchronously executes a typed YPath request against a given service.
template <class TTypedRequest>
TFuture<TIntrusivePtr<typename TTypedRequest::TTypedResponse>>
ExecuteVerb(
    const IYPathServicePtr& service,
    const TIntrusivePtr<TTypedRequest>& request);

//! Synchronously executes a typed YPath request against a given service.
//! Throws if an error has occurred.
template <class TTypedRequest>
TIntrusivePtr<typename TTypedRequest::TTypedResponse>
SyncExecuteVerb(
    const IYPathServicePtr& service,
    const TIntrusivePtr<TTypedRequest>& request);

//! Synchronously executes |GetKey| verb. Throws if an error has occurred.
TString SyncYPathGetKey(
    const IYPathServicePtr& service,
    const TYPath& path);

//! Asynchronously executes |Get| verb.
TFuture<NYson::TYsonString> AsyncYPathGet(
    const IYPathServicePtr& service,
    const TYPath& path,
    const TNullable<std::vector<TString>>& attributeKeys = Null);

//! Synchronously executes |Get| verb. Throws if an error has occurred.
NYson::TYsonString SyncYPathGet(
    const IYPathServicePtr& service,
    const TYPath& path,
    const TNullable<std::vector<TString>>& attributeKeys = Null);

//! Asynchronously executes |Exists| verb.
TFuture<bool> AsyncYPathExists(
    const IYPathServicePtr& service,
    const TYPath& path);

//! Synchronously executes |Exists| verb. Throws if an error has occurred.
bool SyncYPathExists(
    const IYPathServicePtr& service,
    const TYPath& path);

//! Synchronously executes |Set| verb. Throws if an error has occurred.
void SyncYPathSet(
    const IYPathServicePtr& service,
    const TYPath& path,
    const NYson::TYsonString& value);

//! Synchronously executes |Remove| verb. Throws if an error has occurred.
void SyncYPathRemove(
    const IYPathServicePtr& service,
    const TYPath& path,
    bool recursive = true,
    bool force = false);

//! Synchronously executes |List| verb. Throws if an error has occurred.
std::vector<TString> SyncYPathList(
    const IYPathServicePtr& service,
    const TYPath& path,
    TNullable<i64> limit = Null);

//! Asynchronously executes |List| verb.
TFuture<std::vector<TString>> AsyncYPathList(
    const IYPathServicePtr& service,
    const TYPath& path,
    TNullable<i64> limit = Null);

/*!
 *  Throws exception if the specified node does not exist.
 */
INodePtr GetNodeByYPath(
    const INodePtr& root,
    const TYPath& path);

INodePtr FindNodeByYPath(
    const INodePtr& root,
    const TYPath& path);

void SetNodeByYPath(
    const INodePtr& root,
    const TYPath& path,
    const INodePtr& value);

//! Creates missing maps along #path.
/*!
 *  E.g. if #root is an empty map and #path is |/a/b/c| then
 *  nested maps |a| and |b| get created. Note that the final key (i.e. |c|)
 *  is not forced (since we have no idea of its type anyway).
 */
void ForceYPath(const INodePtr& root, const TYPath& path);

//! Constructs an ephemeral deep copy of #node.
INodePtr CloneNode(const INodePtr& node);

//! Applies changes given by #patch to #base.
//! Returns the resulting tree.
INodePtr PatchNode(const INodePtr& base, const INodePtr& patch);

//! Checks given nodes for deep equality.
bool AreNodesEqual(const INodePtr& lhs, const INodePtr& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

#define YPATH_CLIENT_INL_H_
#include "ypath_client-inl.h"
#undef YPATH_CLIENT_INL_H_
