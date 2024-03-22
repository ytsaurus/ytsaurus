#include "client_impl.h"
#include "type_handler_detail.h"

#include <yt/yt/ytlib/queue_client/producer_init.h>

namespace NYT::NApi::NNative {

using namespace NCypressClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

class TProducerTypeHandler
    : public TTypeHandlerBase
{
public:
    using TTypeHandlerBase::TTypeHandlerBase;

    virtual std::optional<NCypressClient::TNodeId> CreateNode(
        EObjectType type,
        const TYPath& path,
        const TCreateNodeOptions& options)
    {
        using namespace NYT::NQueueClient;
        if (type != EObjectType::Producer) {
            return {};
        }

        return CreateProducerNode(Client_, path, options);
    }
};

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreateProducerTypeHandler(TClient* client)
{
    return New<TProducerTypeHandler>(client);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
