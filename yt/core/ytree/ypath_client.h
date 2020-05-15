#pragma once

#include "public.h"
#include "ypath_service.h"

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref.h>

#include <yt/core/rpc/client.h>

#include <yt/core/ytree/proto/ypath.pb.h>

#include <any>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYPathRequest
   : public NRpc::IClientRequest
{
public:
    //! Enables tagging requests with arbitrary payload.
    //! These tags are propagated to the respective responses (if a particular implementation supports this).
    //! This simplifies correlating requests with responses within a batch.
    DEFINE_BYREF_RW_PROPERTY(std::any, Tag);

public:
    explicit TYPathRequest(const NRpc::NProto::TRequestHeader& header);

    TYPathRequest(
        TString service,
        TString method,
        NYPath::TYPath path,
        bool mutating);

    virtual NRpc::TRequestId GetRequestId() const override;
    virtual NRpc::TRealmId GetRealmId() const override;
    virtual const TString& GetMethod() const override;
    virtual const TString& GetService() const override;

    virtual const TString& GetUser() const override;
    virtual void SetUser(const TString& user) override;

    virtual void SetUserAgent(const TString& userAgent) override;

    virtual bool GetRetry() const override;
    virtual void SetRetry(bool value) override;

    virtual NRpc::TMutationId GetMutationId() const override;
    virtual void SetMutationId(NRpc::TMutationId id) override;

    virtual size_t GetHash() const override;

    virtual NRpc::EMultiplexingBand GetMultiplexingBand() const override;
    virtual void SetMultiplexingBand(NRpc::EMultiplexingBand band) override;

    virtual const NRpc::NProto::TRequestHeader& Header() const override;
    virtual NRpc::NProto::TRequestHeader& Header() override;

    virtual bool IsStreamingEnabled() const override;

    virtual const NRpc::TStreamingParameters& ClientAttachmentsStreamingParameters() const override;
    virtual NRpc::TStreamingParameters& ClientAttachmentsStreamingParameters() override;

    virtual const NRpc::TStreamingParameters& ServerAttachmentsStreamingParameters() const override;
    virtual NRpc::TStreamingParameters& ServerAttachmentsStreamingParameters() override;

    virtual NConcurrency::IAsyncZeroCopyOutputStreamPtr GetRequestAttachmentsStream() const override;
    virtual NConcurrency::IAsyncZeroCopyInputStreamPtr GetResponseAttachmentsStream() const override;

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

    //! A copy of the request's tag.
    DEFINE_BYREF_RW_PROPERTY(std::any, Tag);

public:
    void Deserialize(const TSharedRefArray& message);

protected:
    virtual void DeserializeBody(TRef data, std::optional<NCompression::ECodec> codecId = std::nullopt);
};

DEFINE_REFCOUNTED_TYPE(TYPathResponse)

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponseMessage>
class TTypedYPathResponse
    : public TYPathResponse
    , public TResponseMessage
{
protected:
    virtual void DeserializeBody(TRef data, std::optional<NCompression::ECodec> codecId = std::nullopt) override
    {
        if (codecId) {
            DeserializeProtoWithCompression(this, data, *codecId);
        } else {
            DeserializeProtoWithEnvelope(this, data);
        }
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

const TYPath& GetRequestTargetYPath(const NRpc::NProto::TRequestHeader& header);
const TYPath& GetOriginalRequestTargetYPath(const NRpc::NProto::TRequestHeader& header);
void SetRequestTargetYPath(NRpc::NProto::TRequestHeader* header, TYPath path);

bool IsRequestMutating(const NRpc::NProto::TRequestHeader& header);

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
    const std::optional<std::vector<TString>>& attributeKeys = std::nullopt);

//! Synchronously executes |Get| verb. Throws if an error has occurred.
NYson::TYsonString SyncYPathGet(
    const IYPathServicePtr& service,
    const TYPath& path,
    const std::optional<std::vector<TString>>& attributeKeys = std::nullopt);

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
    const NYson::TYsonString& value,
    bool recursive = false);

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
    std::optional<i64> limit = std::nullopt);

//! Asynchronously executes |List| verb.
TFuture<std::vector<TString>> AsyncYPathList(
    const IYPathServicePtr& service,
    const TYPath& path,
    std::optional<i64> limit = std::nullopt);

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

/////////////////////////////////////////////////////////////////////////////

struct TNodeWalkOptions
{
    std::function<INodePtr(const TString&)> MissingAttributeHandler;
    std::function<INodePtr(const IMapNodePtr&, const TString&)> MissingChildKeyHandler;
    std::function<INodePtr(const IListNodePtr&, int)> MissingChildIndexHandler;
    std::function<INodePtr(const INodePtr&)> NodeCannotHaveChildrenHandler;
};

extern TNodeWalkOptions GetNodeByYPathOptions;
extern TNodeWalkOptions FindNodeByYPathOptions;

//! Generic function walking down the node according to given ypath.
INodePtr WalkNodeByYPath(
    const INodePtr& root,
    const TYPath& path,
    const TNodeWalkOptions& options);

/*!
 *  Throws exception if the specified node does not exist.
 */
INodePtr GetNodeByYPath(
    const INodePtr& root,
    const TYPath& path);

/*!
 *  Does not throw exception if the specified node does not exist, but still throws on attempt of
 *  moving to the child of a non-composite node.
 */
INodePtr FindNodeByYPath(
    const INodePtr& root,
    const TYPath& path);

void SetNodeByYPath(
    const INodePtr& root,
    const TYPath& path,
    const INodePtr& value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

#define YPATH_CLIENT_INL_H_
#include "ypath_client-inl.h"
#undef YPATH_CLIENT_INL_H_
