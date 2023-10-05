#include "path_resolver.h"

#include "private.h"

#include <yt/yt/ytlib/sequoia_client/helpers.h>
#include <yt/yt/ytlib/sequoia_client/resolve_node.record.h>
#include <yt/yt/ytlib/sequoia_client/reverse_resolve_node.record.h>
#include <yt/yt/ytlib/sequoia_client/table_descriptor.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

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
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

static const auto SlashYPath = TYPath("/");

////////////////////////////////////////////////////////////////////////////////

class TPathResolver
{
public:
    TPathResolver(
        ISequoiaTransactionPtr transaction,
        TYPath path)
        : Transaction_(std::move(transaction))
        , Path_(std::move(path))
    { }

    TResolveResult Resolve()
    {
        Tokenizer_.Reset(Path_);
        TYPath rewrittenPath;

        for (int resolveDepth = 0; ; ++resolveDepth) {
            ValidateYPathResolutionDepth(Path_, resolveDepth);

            if (auto rewrite = MaybeRewriteRoot()) {
                rewrittenPath = std::move(*rewrite);
                Tokenizer_.Reset(rewrittenPath);
                continue;
            }

            struct TResolveAttempt
            {
                TYPath Prefix;
                TYPath Suffix;
            };
            constexpr int TypicalTokenCount = 16;
            TCompactVector<TResolveAttempt, TypicalTokenCount> resolveAttempts;

            TYPath currentPrefix = SlashYPath;
            currentPrefix.reserve(Tokenizer_.GetInput().size());

            while (Tokenizer_.Skip(ETokenType::Slash)) {
                if (Tokenizer_.GetType() != ETokenType::Literal) {
                    break;
                }
                auto literal = Tokenizer_.GetLiteralValue();

                currentPrefix += SlashYPath;
                currentPrefix += std::move(literal);

                Tokenizer_.Advance();

                resolveAttempts.push_back(TResolveAttempt{
                    .Prefix = currentPrefix,
                    .Suffix = TYPath(Tokenizer_.GetInput()),
                });
            }

            std::vector<NRecords::TResolveNodeKey> prefixKeys;
            prefixKeys.reserve(resolveAttempts.size());
            for (const auto& resolveAttempt : resolveAttempts) {
                prefixKeys.push_back(NRecords::TResolveNodeKey{
                    .Path = MangleSequoiaPath(resolveAttempt.Prefix),
                });
            }

            // TODO(gritukan, babenko): Add column filters to codegen library.
            const auto& schema = ITableDescriptor::Get(ESequoiaTable::ResolveNode)
                ->GetRecordDescriptor()
                ->GetSchema();
            NTableClient::TColumnFilter columnFilter({
                schema->GetColumnIndex("path"),
                schema->GetColumnIndex("node_id"),
            });
            auto lookupRsps = WaitFor(Transaction_->LookupRows(prefixKeys, columnFilter))
                .ValueOrThrow();
            YT_VERIFY(lookupRsps.size() == prefixKeys.size());

            bool scionFound = false;
            TSequoiaResolveResult result;
            for (int index = 0; index < std::ssize(lookupRsps); ++index) {
                if (const auto& rsp = lookupRsps[index]) {
                    auto nodeId = ConvertTo<TNodeId>(rsp->NodeId);
                    if (TypeFromId(nodeId) == EObjectType::Scion) {
                        scionFound = true;
                    }

                    if (scionFound) {
                        const auto& resolveAttempt = resolveAttempts[index];
                        YT_VERIFY(MangleSequoiaPath(resolveAttempt.Prefix) == rsp->Key.Path);

                        result = TSequoiaResolveResult{
                            .ResolvedPrefix = resolveAttempt.Prefix,
                            .ResolvedPrefixNodeId = nodeId,
                            .UnresolvedSuffix = resolveAttempt.Suffix,
                        };
                    }
                }
            }

            if (scionFound) {
                return result;
            }

            return TCypressResolveResult{};
        }
    }

private:
    const ISequoiaTransactionPtr Transaction_;

    const TYPath Path_;

    TTokenizer Tokenizer_;

    std::optional<TYPath> MaybeRewriteRoot()
    {
        YT_VERIFY(Tokenizer_.Skip(ETokenType::StartOfStream));
        switch (Tokenizer_.GetType()) {
            case ETokenType::EndOfStream:
                THROW_ERROR_EXCEPTION("YPath cannot be empty");

            case ETokenType::Slash: {
                Tokenizer_.Advance();
                return std::nullopt;
            }

            case ETokenType::Literal: {
                auto token = Tokenizer_.GetToken();
                if (!token.StartsWith(ObjectIdPathPrefix)) {
                    Tokenizer_.ThrowUnexpected();
                }

                auto idWithoutPrefix = token.substr(ObjectIdPathPrefix.size());
                auto objectId = TObjectId::FromString(idWithoutPrefix);

                if (!IsSequoiaId(objectId)) {
                    THROW_ERROR_EXCEPTION("Object id syntax for non-Sequoia objects is not supported yet");
                }

                const auto& schema = ITableDescriptor::Get(ESequoiaTable::ResolveNode)
                    ->GetRecordDescriptor()
                    ->GetSchema();
                NTableClient::TColumnFilter columnFilter({
                    schema->GetColumnIndex("node_id"),
                    schema->GetColumnIndex("path"),
                });

                std::vector<NRecords::TReverseResolveNodeKey> key;
                key.push_back(NRecords::TReverseResolveNodeKey{.NodeId = TNodeId::FromString(idWithoutPrefix)});
                auto lookupRsp = std::move(WaitFor(Transaction_->LookupRows(key, columnFilter))
                    .ValueOrThrow()[0]);

                if (!lookupRsp) {
                    THROW_ERROR_EXCEPTION("No such object")
                        << TErrorAttribute("object_id", objectId);
                }

                Tokenizer_.Advance();

                return lookupRsp->Path + Tokenizer_.GetInput();
            }

            default:
                Tokenizer_.ThrowUnexpected();
        }

        YT_ABORT();
    }
};

////////////////////////////////////////////////////////////////////////////////

TResolveResult ResolvePath(
    ISequoiaTransactionPtr transaction,
    TYPath path)
{
    TPathResolver resolver(
        std::move(transaction),
        std::move(path));
    return resolver.Resolve();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
