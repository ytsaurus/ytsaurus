#include "public.h"

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/chunk_server/chunk_owner_node_proxy.h>

#include <yt/yt/server/master/cypress_server/node_proxy_detail.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletOwnerProxyBase
    : public NCypressServer::TCypressNodeProxyBase<NChunkServer::TChunkOwnerNodeProxy, NYTree::IEntityNode, TTabletOwnerBase>
{
public:
    using TCypressNodeProxyBase::TCypressNodeProxyBase;

protected:
    using TBase = TCypressNodeProxyBase<TChunkOwnerNodeProxy, IEntityNode, TTabletOwnerBase>;

    void DoListSystemAttributes(
        std::vector<TAttributeDescriptor>* descriptors,
        bool showTabletAttributes);
    bool DoGetBuiltinAttribute(
        NYTree::TInternedAttributeKey key,
        NYson::IYsonConsumer* consumer,
        bool showTabletAttributes);

    bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value, bool force);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
