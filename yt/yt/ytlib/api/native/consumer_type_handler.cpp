#include "client_impl.h"
#include "type_handler_detail.h"

#include <yt/yt/ytlib/queue_client/consumer_init.h>

namespace NYT::NApi::NNative {

using namespace NCypressClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

class TConsumerTypeHandler
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
        if (type != EObjectType::Consumer) {
            return {};
        }

        return CreateConsumerNode(Client_, path, options);
    }
};

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreateConsumerTypeHandler(TClient* client)
{
    return New<TConsumerTypeHandler>(client);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
