#include "queue_producer_type_handler.h"

#include "client_impl.h"
#include "type_handler_detail.h"

#include <yt/yt/ytlib/queue_client/producer_init.h>

namespace NYT::NApi::NNative {

using namespace NCypressClient;
using namespace NQueueClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

class TQueueProducerTypeHandler
    : public TTypeHandlerBase
{
public:
    using TTypeHandlerBase::TTypeHandlerBase;

    virtual std::optional<NCypressClient::TNodeId> CreateNode(
        EObjectType type,
        const TYPath& path,
        const TCreateNodeOptions& options)
    {
        if (type != EObjectType::QueueProducer) {
            return {};
        }

        return CreateProducerNode(Client_, path, options);
    }
};

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreateQueueProducerTypeHandler(TClient* client)
{
    return New<TQueueProducerTypeHandler>(client);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
