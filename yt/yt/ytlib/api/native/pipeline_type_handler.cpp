#include "replication_card_type_handler.h"

#include "client_impl.h"
#include "type_handler_detail.h"

#include <yt/yt/flow/lib/native_client/pipeline_init.h>

namespace NYT::NApi::NNative {

using namespace NCypressClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

class TPipelineTypeHandler
    : public TTypeHandlerBase
{
public:
    using TTypeHandlerBase::TTypeHandlerBase;

    virtual std::optional<NCypressClient::TNodeId> CreateNode(
        EObjectType type,
        const TYPath& path,
        const TCreateNodeOptions& options)
    {
        if (type != EObjectType::Pipeline) {
            return {};
        }

        return NFlow::CreatePipelineNode(Client_, path, options);
    }
};

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreatePipelineTypeHandler(TClient* client)
{
    return New<TPipelineTypeHandler>(client);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
