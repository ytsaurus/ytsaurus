#include "queue_consumer_type_handler.h"

#include "client_impl.h"
#include "type_handler_detail.h"

#include <yt/yt/ytlib/queue_client/queue_consumer_init.h>

namespace NYT::NApi::NNative {

using namespace NCypressClient;
using namespace NQueueClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

class TQueueConsumerTypeHandler
    : public TTypeHandlerBase
{
public:
    using TTypeHandlerBase::TTypeHandlerBase;

    virtual std::optional<NCypressClient::TNodeId> CreateNode(
        EObjectType type,
        const TYPath& path,
        const TCreateNodeOptions& options)
    {
        if (type != EObjectType::QueueConsumer && type != EObjectType::QueueMultiConsumer) {
            return {};
        }

        return CreateQueueConsumerNode(
            Client_,
            path,
            /*isMultiConsumer*/ type == EObjectType::QueueMultiConsumer,
            options);
    }
};

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreateQueueConsumerTypeHandler(TClient* client)
{
    return New<TQueueConsumerTypeHandler>(client);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
