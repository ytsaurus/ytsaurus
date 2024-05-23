#include "path_resolver.h"
#include "helpers.h"

#include <yt/yt/ytlib/sequoia_client/helpers.h>
#include <yt/yt/ytlib/sequoia_client/table_descriptor.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>
#include <yt/yt/ytlib/sequoia_client/ypath_detail.h>

#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_id_to_path.record.h>

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NCypressProxy {

using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NYTree;

using TYPath = NSequoiaClient::TYPath;

////////////////////////////////////////////////////////////////////////////////

static const TString SlashYPath = "/";
static const TString AmpersandYPath = "&";

////////////////////////////////////////////////////////////////////////////////

class TPathResolver
{
public:
    explicit TPathResolver(TSequoiaServiceContext* context)
        : Context_(context)
    { }

    void Resolve()
    {
        auto rawPath = GetRequestTargetYPath(Context_->GetRequestHeader());
        // Paths starting with '&#...' are used for accessing replicated transactions and
        // have to be resolved by master.
        if (rawPath.Empty() || rawPath.StartsWith(AmpersandYPath)) {
            Context_->SetResolveResult(TCypressResolveResult{});
            return;
        }

        TAbsoluteYPathBuf path(rawPath);
        std::optional<TAbsoluteYPath> rewrittenPath;

        auto [rootDesignator, pathSuffix] = path.GetRootDesignator();
        static_assert(std::variant_size<decltype(rootDesignator)>() == 2);

        auto* objectId = std::get_if<TGuid>(&rootDesignator);
        if (objectId) {
            if (!IsSequoiaId(*objectId) || !IsSupportedSequoiaType(TypeFromId(*objectId))) {
                Context_->SetResolveResult(TCypressResolveResult{});
                return;
            }

            if (auto objectPath = FindObjectPath(*objectId)) {
                rewrittenPath = *objectPath + pathSuffix;
                path = *rewrittenPath;
            } else if (Context_->GetMethod() == "Exists") {
                Context_->SetResolveResult(TCypressResolveResult{});
                return;
            } else {
                THROW_ERROR_EXCEPTION("No such object %v", *objectId);
            }

            pathSuffix = path.GetRootDesignator().second;
        }

        struct TResolveAttempt
        {
            TAbsoluteYPath Prefix;
            TString Suffix;
        };
        constexpr int TypicalTokenCount = 16;
        TCompactVector<TResolveAttempt, TypicalTokenCount> resolveAttempts;

        NYPath::TTokenizer tokenizer(pathSuffix.Underlying());
        tokenizer.Advance();

        auto currentPrefix = TAbsoluteYPath(SlashYPath);
        while (tokenizer.Skip(NYPath::ETokenType::Slash)) {
            if (tokenizer.GetType() != NYPath::ETokenType::Literal) {
                break;
            }

            currentPrefix.Append(tokenizer.GetLiteralValue());

            tokenizer.Advance();

            resolveAttempts.push_back(TResolveAttempt{
                .Prefix = currentPrefix,
                .Suffix = TString(tokenizer.GetInput()),
            });

            tokenizer.Skip(NYPath::ETokenType::Ampersand);
        }

        std::vector<NRecords::TPathToNodeIdKey> prefixKeys;
        prefixKeys.reserve(resolveAttempts.size());
        for (const auto& resolveAttempt : resolveAttempts) {
            prefixKeys.push_back(NRecords::TPathToNodeIdKey{
                .Path = resolveAttempt.Prefix.ToMangledSequoiaPath(),
            });
        }

        // TODO(gritukan, babenko): Add column filters to codegen library.
        const auto& schema = ITableDescriptor::Get(ESequoiaTable::PathToNodeId)
            ->GetRecordDescriptor()
            ->GetSchema();
        NTableClient::TColumnFilter columnFilter({
            schema->GetColumnIndex("path"),
            schema->GetColumnIndex("node_id"),
        });
        auto lookupRsps = WaitFor(Context_->GetSequoiaTransaction()->LookupRows(prefixKeys, columnFilter))
            .ValueOrThrow();
        YT_VERIFY(lookupRsps.size() == prefixKeys.size());

        std::optional<TSequoiaResolveResult> result;
        for (int index = 0; index < std::ssize(lookupRsps); ++index) {
            if (const auto& rsp = lookupRsps[index]) {
                auto nodeId = ConvertTo<TNodeId>(rsp->NodeId);
                if (result || TypeFromId(nodeId) == EObjectType::Scion) {
                    const auto& resolveAttempt = resolveAttempts[index];
                    YT_VERIFY(resolveAttempt.Prefix.ToMangledSequoiaPath() == rsp->Key.Path);

                    if (resolveAttempt.Suffix.StartsWith(AmpersandYPath) &&
                        TypeFromId(nodeId) == EObjectType::Scion)
                    {
                        Context_->SetResolveResult(TCypressResolveResult{});
                        return;
                    }

                    result = TSequoiaResolveResult{
                        .ResolvedPrefix = std::move(resolveAttempt.Prefix),
                        .ResolvedPrefixNodeId = nodeId,
                        .UnresolvedSuffix = TYPath(std::move(resolveAttempt.Suffix)),
                    };
                }
            }
        }

        if (result) {
            Context_->SetResolveResult(std::move(*result));
        } else {
            Context_->SetResolveResult(TCypressResolveResult{});
        }
    }

private:
    TSequoiaServiceContext* const Context_;


    std::optional<TAbsoluteYPath> FindObjectPath(TObjectId objectId)
    {
        const auto& schema = ITableDescriptor::Get(ESequoiaTable::PathToNodeId)
            ->GetRecordDescriptor()
            ->GetSchema();
        NTableClient::TColumnFilter columnFilter({
            schema->GetColumnIndex("node_id"),
            schema->GetColumnIndex("path"),
        });

        std::vector<NRecords::TNodeIdToPathKey> key;
        key.push_back(NRecords::TNodeIdToPathKey{.NodeId = objectId});
        auto lookupRsp = std::move(WaitFor(Context_->GetSequoiaTransaction()->LookupRows(key, columnFilter))
            .ValueOrThrow()[0]);

        return lookupRsp
            ? std::make_optional<TAbsoluteYPath>(lookupRsp->Path)
            : std::nullopt;
    }
};

////////////////////////////////////////////////////////////////////////////////

void ResolvePath(TSequoiaServiceContext* context)
{
    TPathResolver resolver(context);
    resolver.Resolve();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
