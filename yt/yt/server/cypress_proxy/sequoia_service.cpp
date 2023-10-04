#include "sequoia_service.h"

#include "bootstrap.h"
#include "node_proxy.h"
#include "path_resolver.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/sequoia_client/client.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/ypath_detail.h>
#include <yt/yt/core/ytree/ypath_service.h>

#include <yt/yt/core/ypath/tokenizer.h>

namespace NYT::NCypressProxy {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TSequoiaService
    : public TYPathServiceBase
{
public:
    TSequoiaService(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    TResolveResult Resolve(
        const TYPath& path,
        const IYPathServiceContextPtr& context) override
    {
        auto Logger = CypressProxyLogger.WithTag("CypressRequestId: %v", context->GetRequestId());
        auto transaction = WaitFor(Bootstrap_->GetSequoiaClient()->StartTransaction())
            .ValueOrThrow();

        auto resolveResult = ResolvePath(transaction, path);
        if (std::holds_alternative<TCypressResolveResult>(resolveResult)) {
            THROW_ERROR_EXCEPTION(
                NObjectClient::EErrorCode::RequestInvolvesCypress,
                "Cypress request has been passed to Sequoia");
        }

        auto sequoiaResolveResult = GetOrCrash<TSequoiaResolveResult>(resolveResult);
        auto prefixNodeId = sequoiaResolveResult.ResolvedPrefixNodeId;

        return TResolveResultThere{
            CreateNodeProxy(
                Bootstrap_,
                std::move(transaction),
                prefixNodeId,
                std::move(sequoiaResolveResult.ResolvedPrefix)),
            std::move(sequoiaResolveResult.UnresolvedSuffix),
        };
    }

private:
    IBootstrap* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

NYTree::IYPathServicePtr CreateSequoiaService(IBootstrap* bootstrap)
{
    return New<TSequoiaService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
