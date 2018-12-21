#include "private.h"

#include <yt/server/object_server/public.h>
#include <yt/server/object_server/object_detail.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/ytlib/object_client/object_ypath.pb.h>

#include <yt/ytlib/chunk_client/chunk_owner_ypath.pb.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/ytree/virtual.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TVirtualStaticTable
    : public NYTree::TSupportsAttributes
    , public NYTree::ISystemAttributeProvider
{
public:
    TVirtualStaticTable(
        const THashSet<NChunkClient::TInputChunkPtr>& chunks,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory);

    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override;

    // TSupportsAttributes overrides
    virtual ISystemAttributeProvider* GetBuiltinAttributeProvider() override;

    // ISystemAttributeProvider overrides
    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    virtual const THashSet<NYTree::TInternedAttributeKey>& GetBuiltinAttributeKeys() override;
    virtual bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    virtual TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(NYTree::TInternedAttributeKey key) override;
    virtual bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value) override;
    virtual bool RemoveBuiltinAttribute(NYTree::TInternedAttributeKey key) override;

    virtual void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override;

    virtual void DoWriteAttributesFragment(
        NYT::NYson::IAsyncYsonConsumer* consumer,
        const std::optional<std::vector<TString>>& attributeKeys,
        bool stable) override;

private:
    NYTree::TBuiltinAttributeKeysCache BuiltinAttributeKeysCache_;

    const THashSet<NChunkClient::TInputChunkPtr>& Chunks_;

    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, GetBasicAttributes);
    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, Fetch);
};

DEFINE_REFCOUNTED_TYPE(TVirtualStaticTable)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
