#include "default_type_handler.h"

#include "type_handler.h"
#include "client_impl.h"
#include "ypath_helpers.h"

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NApi::NNative {

using namespace NYson;
using namespace NYPath;
using namespace NYTree;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableReplicaTypeHandler
    : public ITypeHandler
{
public:
    explicit TReplicatedTableReplicaTypeHandler(TClient* client)
        : Client_(client)
    { }

    std::optional<TYsonString> GetNode(
        const TYPath& path,
        const TGetNodeOptions& /*options*/) override
    {
        MaybeValidatePermission(path);
        return {};
    }

    std::optional<TYsonString> ListNode(
        const TYPath& path,
        const TListNodeOptions& /*options*/) override
    {
        MaybeValidatePermission(path);
        return {};
    }

private:
    TClient* const Client_;


    void MaybeValidatePermission(const TYPath& path)
    {
        TObjectId objectId;
        if (!TryParseObjectId(path, &objectId) || TypeFromId(objectId) != EObjectType::TableReplica) {
            return;
        }

        Client_->ValidateTableReplicaPermission(objectId, EPermission::Read, /*options*/ {});
    }
};

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreateReplicatedTableReplicaTypeHandler(TClient* client)
{
    return New<TReplicatedTableReplicaTypeHandler>(client);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
