#include "path_resolver.h"
#include "helpers.h"
#include "sequoia_session.h"

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
using namespace NYPath;
using namespace NYTree;

using TYPath = NSequoiaClient::TYPath;
using TYPathBuf = NSequoiaClient::TYPathBuf;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

bool ShouldBeResolvedInSequoia(TObjectId id)
{
    auto type = TypeFromId(id);
    // NB: all links are presented in Sequoia tables and have to be resolved in
    // Sequoia.
    return type == EObjectType::Link || (IsSequoiaId(id) && IsSupportedSequoiaType(type));
}

////////////////////////////////////////////////////////////////////////////////

struct TPathPrefix
{
    TAbsoluteYPathBuf Prefix;
    // NB: may contain leading ampersand.
    TYPathBuf Suffix;
};

//! This function is used to obtain path prefixes to fetch them from Sequoia
//! table.
auto CollectPathPrefixes(const TAbsoluteYPath& path)
{
    constexpr auto TypicalTokenCount = 16;
    TCompactVector<TPathPrefix, TypicalTokenCount> resolveAttempts;

    TTokenizer tokenizer(path.Underlying());
    // Skipping root designator.
    tokenizer.Advance();
    YT_VERIFY(tokenizer.Skip(ETokenType::Slash));

    while (tokenizer.Skip(ETokenType::Slash)) {
        if (tokenizer.GetType() != ETokenType::Literal) {
            break;
        }

        tokenizer.Advance();

        // NB: partially tokenized path: parsed_prefix + current_token + suffix
        // tokenizer.GetPrefix(): parsed_prefix
        // tokenizer.GetInput(): current_token + suffix
        resolveAttempts.push_back({
            .Prefix = TAbsoluteYPathBuf(tokenizer.GetPrefix()),
            .Suffix = TYPathBuf(tokenizer.GetInput()),
        });

        // NB: we'll deal with it later. Of course, logically "&" should rather
        // be a part of "resolved prefix" than "unresolved suffix", but here we
        // are collecting path prefixes to resolve them via Sequoia tables.
        tokenizer.Skip(ETokenType::Ampersand);
    }
    return resolveAttempts;
}

//! Takes path prefixes and looks up their node IDs.
auto FetchNodeIds(const TSequoiaSessionPtr& session, TRange<TPathPrefix> prefixes)
{
    std::vector<NRecords::TPathToNodeIdKey> prefixKeys(prefixes.size());
    std::transform(
        prefixes.begin(),
        prefixes.end(),
        prefixKeys.begin(),
        [] (const TPathPrefix& resolveAttempt) -> NRecords::TPathToNodeIdKey {
            return {.Path = resolveAttempt.Prefix.ToMangledSequoiaPath()};
        });
    return session->FindNodesByPath(prefixKeys);
}

bool StartsWithAmpersand(TYPathBuf pathSuffix)
{
    // NB: such places are implemented via |TTokenizer| abstraction but they can
    // be probably implemented more optimally via straightforward checking of
    // first byte. The reason to not do it is unnecessary abstraction layer...
    // TODO(kvk1920): think of it.
    TTokenizer tokenizer(pathSuffix.Underlying());
    tokenizer.Advance();
    return tokenizer.GetType() == ETokenType::Ampersand;
}

bool ShouldFollowLink(TYPathBuf unresolvedSuffix, TStringBuf method)
{
    TTokenizer tokenizer(unresolvedSuffix.Underlying());
    tokenizer.Advance();

    if (tokenizer.GetType() == ETokenType::Ampersand) {
        return false;
    }

    if (tokenizer.GetType() == ETokenType::Slash) {
        return true;
    }

    // NB: when symlink is last component of request's path we try to avoid
    // actions leading to data loss. E.g., it's better to remove symlink instead
    // of table pointed by symlink.

    static const TStringBuf MethodsWithoutRedirection[] = {
        "Remove",
        "Create",
        "Set",
        "Copy",
        "BeginCopy",
        "EndCopy",
    };

    return std::end(MethodsWithoutRedirection) == std::find(
        std::begin(MethodsWithoutRedirection),
        std::end(MethodsWithoutRedirection),
        method);
}

////////////////////////////////////////////////////////////////////////////////

struct TForwardToMaster
{
    TAbsoluteYPath Path;
};

struct TResolveHere
{
    TSequoiaResolveResult Result;
};

struct TResolveThere
{
    TSequoiaResolveIterationResult Result;
    TAbsoluteYPath RewrittenTargetPath;
};

using TResolveIterationResult = std::variant<
    TForwardToMaster,
    TResolveHere,
    TResolveThere>;

TResolveIterationResult ResolveByPath(
    const TSequoiaSessionPtr& session,
    TAbsoluteYPath path,
    TStringBuf method)
{
    auto prefixesToResolve = CollectPathPrefixes(path);
    auto nodes = FetchNodeIds(session, prefixesToResolve);
    YT_VERIFY(prefixesToResolve.size() == nodes.size());

    int index = 0;
    // Skip prefixes before scion.
    while (index < std::ssize(nodes) && !nodes[index]) {
        ++index;
    }

    if (index == std::ssize(nodes)) {
        // We haven't found neither symlink nor scion.
        return TForwardToMaster{std::move(path)};
    }

    TNodeId resolvedId = NullObjectId;
    std::optional<TAbsoluteYPathBuf> resolvedPath;
    TNodeId resolvedParentId = NullObjectId;
    std::optional<TYPathBuf> unresolvedSuffix;

    for (; index < std::ssize(nodes); ++index) {
        const auto& node = nodes[index];
        if (!node) {
            break;
        }

        const auto& prefix = prefixesToResolve[index];
        YT_ASSERT(prefix.Prefix.ToMangledSequoiaPath() == node->Key.Path);

        auto nodeId = node->NodeId;
        auto nodeType = TypeFromId(nodeId);
        if ((nodeType == EObjectType::Scion && StartsWithAmpersand(prefix.Suffix)) ||
            (nodeType == EObjectType::Link && !ShouldFollowLink(prefix.Suffix, method)))
        {
            return TForwardToMaster{std::move(path)};
        }

        resolvedParentId = resolvedId;
        resolvedId = nodeId;
        resolvedPath = prefix.Prefix;
        unresolvedSuffix = prefix.Suffix;

        if (IsLinkType(nodeType) && ShouldFollowLink(prefix.Suffix, method)) {
            auto record = session->FindNodeById(nodeId);
            // Failure here means that Sequoia resolve tables are inconsistent.
            YT_VERIFY(record);
            YT_VERIFY(!record->TargetPath.Empty());

            auto targetPath = TAbsoluteYPath(record->TargetPath);
            targetPath += *unresolvedSuffix;

            return TResolveThere{
                .Result = {
                    .Id = resolvedId,
                    .Path = *resolvedPath,
                },
                .RewrittenTargetPath = std::move(targetPath),
            };
        }
    }

    return TResolveHere{{
        .Id = resolvedId,
        .Path = *resolvedPath,
        .UnresolvedSuffix = *unresolvedSuffix,
        .ParentId = resolvedParentId,
    }};
}

TResolveIterationResult ResolveByObjectId(
    const TSequoiaSessionPtr& session,
    TAbsoluteYPathBuf path,
    TStringBuf method,
    TObjectId rootDesignator,
    TYPathBuf pathSuffix)
{
    if (!ShouldBeResolvedInSequoia(rootDesignator)) {
        return TForwardToMaster{path};
    }

    // If path starts with "#<object-id>" we try to find it in
    // "node_id_to_path" Sequoia table. After that we replace object ID with
    // its path and continue resolving it.
    if (auto record = session->FindNodeById(rootDesignator)) {
        // NB: ampersand should be "resolved" after current step.
        if (IsLinkType(TypeFromId(rootDesignator)) && ShouldFollowLink(pathSuffix, method))
        {
            YT_VERIFY(!record->TargetPath.Empty());

            auto targetPath = TAbsoluteYPath(std::move(record->TargetPath));
            targetPath += pathSuffix;

            return TResolveThere{
                .Result = {
                    .Id = rootDesignator,
                    .Path = TAbsoluteYPath(std::move(record->Path)),
                },
                .RewrittenTargetPath = std::move(targetPath),
            };
        }

        // TODO(kvk1920): handle snapshot branches here since they aren't stored
        // in "path_to_node_id" table.

        auto rewrittenPath = TAbsoluteYPath(std::move(record->Path));
        rewrittenPath += pathSuffix;
        return ResolveByPath(session, std::move(rewrittenPath), method);
    }

    // NB: of course, we could response just after resolve in Sequoia tables.
    // But while we don't have any way to bypass Sequoia resolve we use "exists"
    // verb in tests to check object existence in master.
    // TODO(kvk1920): design some way to bypass Sequoia resolve.
    if (method == "Exists") {
        return TForwardToMaster{std::move(path)};
    }

    // NB: link creation is a bit asynchronous. Link is created at master first
    // and only then is replicated to Sequoia resolve tables. We probably don't
    // want to treat such replication lag as node not existing.
    // TODO(kvk1920): some kind of sync with GUQM.
    if (TypeFromId(rootDesignator) == EObjectType::Link) {
        return TForwardToMaster{std::move(path)};
    }

    THROW_ERROR_EXCEPTION("No such object %v", rootDesignator);
}

//! Returns raw path if it should be resolved by master.
TResolveIterationResult RunResolveIteration(
    const TSequoiaSessionPtr& session,
    TAbsoluteYPath path,
    TStringBuf method)
{
    auto [rootDesignator, pathSuffix] = path.GetRootDesignator();
    return Visit(rootDesignator,
        [&] (TObjectId id) {
            return ResolveByObjectId(session, path, method, id, pathSuffix);
        },
        [&] (TSlashRootDesignatorTag /*tag*/) {
            return ResolveByPath(session, path, method);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TResolveResult ResolvePath(
    const TSequoiaSessionPtr& session,
    TRawYPath rawPath,
    TStringBuf method,
    std::vector<TSequoiaResolveIterationResult>* history)
{
    auto tokenizer = TTokenizer(rawPath.Underlying());
    tokenizer.Advance();
    auto firstTokenType = tokenizer.GetType();

    // Paths starting with '&#...' are used for accessing replicated
    // transactions and have to be resolved by master.
    // Empty paths are also special and must be forwarded.
    if (firstTokenType == ETokenType::Ampersand || firstTokenType == ETokenType::EndOfStream) {
        return TCypressResolveResult{std::move(rawPath)};
    }

    if (history) {
        history->clear();
    }

    auto path = TAbsoluteYPath(std::move(rawPath));
    for (int resolutionDepth = 0; ; ++resolutionDepth) {
        ValidateYPathResolutionDepth(path.Underlying(), resolutionDepth);

        auto iterationResult = RunResolveIteration(session, std::move(path), method);
        static_assert(std::variant_size<decltype(iterationResult)>() == 3);

        if (auto* forwardToMaster = std::get_if<TForwardToMaster>(&iterationResult)) {
            return TCypressResolveResult{
                .Path = std::move(forwardToMaster->Path).ToRawYPath(),
            };
        } else if (auto* resolvedHere = std::get_if<TResolveHere>(&iterationResult)) {
            return std::move(resolvedHere->Result);
        } else {
            auto& resolvedThere = GetOrCrash<TResolveThere>(iterationResult);

            if (history) {
                history->push_back(std::move(resolvedThere.Result));
            }

            path = std::move(resolvedThere.RewrittenTargetPath);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
