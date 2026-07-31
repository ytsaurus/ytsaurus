#include "access_log.h"
#include "private.h"
#include "public.h"

#include <yt/yt/server/lib/security_server/access_log.h>
#include <yt/yt/server/lib/hydra/mutation_context.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NSecurityServer {

using namespace NObjectClient;
using namespace NRpc;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

namespace {

void TraverseTransactionAncestors(
    const TTransaction* transaction,
    TFluentMap& fluent)
{
    const auto* attributes = transaction->GetAttributes();

    fluent
        .Item("transaction_id").Value(transaction->GetId())
        .DoIf(transaction->GetTitle().has_value(), [&] (auto fluent) {
            fluent
                .Item("transaction_title").Value(transaction->GetTitle());
        })
        .DoIf(attributes, [&] (auto fluent) {
            const auto& attributeMap = attributes->Attributes();
            static const std::vector<std::string> Keys{
                "operation_id",
                "operation_title",
                "operation_type",
            };
            for (const auto& key : Keys) {
                if (auto it = attributeMap.find(key)) {
                    fluent.Item(key).Value(it->second);
                }
            }
        })
        .DoIf(transaction->GetParent(), [&] (auto fluent) {
            fluent
                .Item("parent").BeginMap()
                    .Do([&] (auto fluent) {
                        TraverseTransactionAncestors(transaction->GetParent(), fluent);
                    })
                .EndMap();
        });
}

class TMasterTransactionAccessLogInfo
    : public ITransactionAccessLogInfo
{
public:
    explicit TMasterTransactionAccessLogInfo(const TTransaction* transaction)
        : Transaction_(transaction)
    { }

    void WriteTransactionInfo(NYson::IYsonConsumer* consumer) const override
    {
        if (!Transaction_) {
            return;
        }
        auto fluent = BuildYsonMapFragmentFluently(consumer);
        TraverseTransactionAncestors(Transaction_, fluent);
    }

    TTransactionId GetTransactionId() const override
    {
        return GetObjectId(Transaction_);
    }

private:
    const TTransaction* const Transaction_;
};

NRpc::TMutationId GetCurrentMutationId()
{
    auto* context = NHydra::TryGetCurrentMutationContext();
    return context ? context->Request().MutationId : NullMutationId;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void LogAccess(
    NCellMaster::TBootstrap* bootstrap,
    const NRpc::IServiceContextPtr& context,
    NCypressServer::TNodeId id,
    std::optional<TYPathBuf> path,
    const NTransactionServer::TTransaction* transaction,
    const TAccessLogAttributes& additionalAttributes,
    const std::optional<std::string>& methodOverride)
{
    if (NCypressClient::GetSuppressAccessLogging(context->RequestHeader())) {
        return;
    }

    // Seeing as it has come to actually logging something, surely everything
    // has been actually evaluated by YT_EVALUATE_FOR_ACCESS_LOG, right? (Because
    // it checks for the same conditions YT_LOG_ACCESS does.) Wrong. Setting the
    // "enable_access_log" flag changes those conditions in between. Let's skip
    // logging such a request, it's no great loss.
    if (!path) [[unlikely]] {
        return;
    }

    YT_ASSERT(IsAccessLogEnabled(bootstrap));

    TMasterTransactionAccessLogInfo txInfo(transaction);
    NSecurityServer::LogAccess(context, id, *path, txInfo, GetCurrentMutationId(), additionalAttributes, methodOverride);
}

void LogAccess(
    NCellMaster::TBootstrap* bootstrap,
    NCypressServer::TNodeId id,
    TYPathBuf path,
    const NTransactionServer::TTransaction* transaction,
    const std::string& method,
    const TAccessLogAttributes& additionalAttributes)
{
    YT_ASSERT(IsAccessLogEnabled(bootstrap));

    TMasterTransactionAccessLogInfo txInfo(transaction);
    NSecurityServer::LogAccess(id, path, method, txInfo, GetCurrentMutationId(), additionalAttributes);
}

void LogAccess(
    NCellMaster::TBootstrap* bootstrap,
    const std::string& method,
    const NTransactionServer::TTransaction* transaction)
{
    YT_ASSERT(IsAccessLogEnabled(bootstrap));
    YT_ASSERT(transaction);

    TMasterTransactionAccessLogInfo txInfo(transaction);
    NSecurityServer::LogAccess(method, txInfo);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
