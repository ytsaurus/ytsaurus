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
    explicit TPathResolver(
        TSequoiaServiceContext* context,
        TString method,
        TRawYPath rawPath)
        : Context_(context)
        , Method_(std::move(method))
        , RawPath_(std::move(rawPath))
    { }

    void Resolve()
    {
        // Paths starting with '&#...' are used for accessing replicated transactions and have to
        // be resolved by master. Empty paths are also special and must be forwarded.
        if (RawPath_.Underlying().Empty() ||
            RawPath_.Underlying().StartsWith(AmpersandYPath))
        {
            Context_->SetResolveResult(TCypressResolveResult{.Path = RawPath_});
            return;
        }

        TAbsoluteYPathBuf path(RawPath_);
        std::optional<TAbsoluteYPath> rewrittenPathHolder;

        static constexpr auto TypicalResolveDepth = 8;

        std::vector<TResolveStep> resolveHistory;
        resolveHistory.reserve(TypicalResolveDepth);

        for (int resolveDepth = 0; ; ++resolveDepth) {
            ValidateYPathResolutionDepth(path.Underlying(), resolveDepth);

            auto [rootDesignator, pathSuffix] = path.GetRootDesignator();
            static_assert(std::variant_size<decltype(rootDesignator)>() == 2);

            auto makeCurrentCypressResolveReuslt = [this, &path] {
                Context_->SetResolveResult(TCypressResolveResult{.Path = TRawYPath(path.ToString())});
            };

            if (auto* objectId = std::get_if<TGuid>(&rootDesignator)) {
                auto objectType = TypeFromId(*objectId);
                if (objectType != EObjectType::Link &&
                    (!IsSequoiaId(*objectId) ||
                    !IsSupportedSequoiaType(objectType)))
                {
                    makeCurrentCypressResolveReuslt();
                    return;
                }

                if (auto objectPath = FindObjectPath(*objectId)) {
                    rewrittenPathHolder = *objectPath + pathSuffix;
                    path = *rewrittenPathHolder;
                } else if (objectType == EObjectType::Link || Method_ == "Exists") {
                    makeCurrentCypressResolveReuslt();
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
            static constexpr auto TypicalTokenCount = 16;
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

            const auto& idMapping = NRecords::TPathToNodeIdDescriptor::Get()->GetIdMapping();
            auto lookupRsps = WaitFor(Context_->GetSequoiaTransaction()->LookupRows(
                prefixKeys,
                {*idMapping.Path, *idMapping.NodeId, *idMapping.RedirectPath}))
                .ValueOrThrow();
            YT_VERIFY(lookupRsps.size() == prefixKeys.size());

            bool needOneMoreResolveIteration = false;
            std::optional<TSequoiaResolveResult> result;
            for (int index = 0; index < std::ssize(lookupRsps); ++index) {
                const auto& rsp = lookupRsps[index];
                if (!rsp) {
                    continue;
                }

                const auto& resolveAttempt = resolveAttempts[index];
                YT_VERIFY(resolveAttempt.Prefix.ToMangledSequoiaPath() == rsp->Key.Path);

                auto tokenizer = NYPath::TTokenizer(resolveAttempt.Suffix);
                tokenizer.Advance();
                auto ampersandSkipped = tokenizer.Skip(NYPath::ETokenType::Ampersand);
                auto slashSkipped = tokenizer.Skip(NYPath::ETokenType::Slash);

                auto nodeType = TypeFromId(rsp->NodeId);
                if (nodeType == EObjectType::Scion && ampersandSkipped) {
                    makeCurrentCypressResolveReuslt();
                    return;
                }

                if (IsLinkType(nodeType) || nodeType == EObjectType::Scion || result) {
                    resolveHistory.push_back(TResolveStep{
                        .ResolvedPrefix = std::move(resolveAttempt.Prefix),
                        .ResolvedPrefixNodeId = rsp->NodeId,
                        .UnresolvedSuffix = TYPath(std::move(resolveAttempt.Suffix)),
                        .Payload = *rsp,
                    });

                    if (nodeType == EObjectType::Scion || result) {
                        result = TSequoiaResolveResult(resolveHistory.back());
                    }
                }

                if (IsLinkType(nodeType)) {
                    if (ampersandSkipped) {
                        break;
                    }

                    if (!slashSkipped &&
                        (tokenizer.GetType() != NYPath::ETokenType::EndOfStream ||
                        Method_ == "Remove" ||
                        Method_ == "Set" ||
                        Method_ == "Create" ||
                        Method_ == "Copy" ||
                        Method_ == "BeginCopy" ||
                        Method_ == "EndCopy"))
                    {
                        break;
                    }

                    rewrittenPathHolder = TAbsoluteYPath(rsp->RedirectPath);
                    rewrittenPathHolder->Join(TYPath(std::move(resolveAttempt.Suffix)));
                    path = *rewrittenPathHolder;

                    result.reset();
                    needOneMoreResolveIteration = true;
                    break;
                }
            }

            if (needOneMoreResolveIteration) {
                continue;
            }

            Context_->SetResolveHistory(std::move(resolveHistory));

            if (result) {
                Context_->SetResolveResult(std::move(*result));
            } else {
                makeCurrentCypressResolveReuslt();
            }
            return;
        }
    }

private:
    TSequoiaServiceContext* const Context_;
    const TString Method_;
    const NSequoiaClient::TRawYPath RawPath_;

    std::optional<TAbsoluteYPath> FindObjectPath(TObjectId objectId)
    {
        const auto& idMapping = NRecords::TNodeIdToPathDescriptor::Get()->GetIdMapping();
        auto lookupRsp = std::move(WaitFor(Context_->GetSequoiaTransaction()->LookupRows<NRecords::TNodeIdToPathKey>(
            {{.NodeId = objectId}},
            {*idMapping.NodeId, *idMapping.Path}))
            .ValueOrThrow()[0]);

        return lookupRsp
            ? std::optional<TAbsoluteYPath>(lookupRsp->Path)
            : std::nullopt;
    }
};

////////////////////////////////////////////////////////////////////////////////

void ResolvePath(
    TSequoiaServiceContext* context,
    TString method,
    NSequoiaClient::TRawYPath rawPath)
{
    auto pathResolver = TPathResolver(context, std::move(method), std::move(rawPath));
    pathResolver.Resolve();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
