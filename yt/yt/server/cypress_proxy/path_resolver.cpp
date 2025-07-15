#include "path_resolver.h"

#include "helpers.h"
#include "private.h"
#include "sequoia_session.h"

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NCypressProxy {

using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = CypressProxyLogger;

////////////////////////////////////////////////////////////////////////////////

bool ShouldBeResolvedInSequoia(TObjectId id)
{
    auto type = TypeFromId(id);
    // NB: All links are presented in Sequoia tables and have to be resolved in
    // Sequoia.
    return type == EObjectType::Link || (IsSequoiaId(id) && IsSupportedSequoiaType(type));
}

////////////////////////////////////////////////////////////////////////////////

constexpr int TypicalTokenCount = 16;

class TPathPrefixes
{
public:
    using TPrefixes = TCompactVector<TAbsolutePathBuf, TypicalTokenCount>;
    DEFINE_BYREF_RO_PROPERTY(TPrefixes, Prefixes);

    // NB: Suffix may contain leading ampersand.
    using TSuffixes = TCompactVector<TYPathBuf, TypicalTokenCount>;
    DEFINE_BYREF_RO_PROPERTY(TSuffixes, Suffixes);

public:
    //! This function is used to obtain path prefixes to fetch them from
    //! the Sequoia table.
    static TPathPrefixes CollectPrefixes(const TYPath& path)
    {
        TPathPrefixes prefixes;

        TCompactVector<size_t, TypicalTokenCount> prefixLengths;

        TStringBuilder builder;
        builder.Reserve(path.size());

        TTokenizer tokenizer(path);
        tokenizer.Advance();

        auto recordPrefixAndSuffix = [&] {
            prefixLengths.push_back(builder.GetLength());
            prefixes.Suffixes_.push_back(tokenizer.GetSuffix());
        };

        tokenizer.Expect(ETokenType::Slash);
        builder.AppendChar(TAbsolutePath::Separator);
        recordPrefixAndSuffix(); // Root designator is a prefix too.
        tokenizer.Advance();

        while (tokenizer.Skip(ETokenType::Slash)) {
            if (tokenizer.GetType() != ETokenType::Literal) {
                break;
            }

            builder.AppendChar(TAbsolutePath::Separator);
            builder.AppendString(tokenizer.GetToken());
            recordPrefixAndSuffix();
            tokenizer.Advance();

            // NB: We'll deal with it later. Of course, logically "&" should rather
            // be a part of "resolved prefix" than "unresolved suffix", but here we
            // are collecting path prefixes to resolve them via Sequoia tables.
            tokenizer.Skip(ETokenType::Ampersand);
        }

        YT_VERIFY(prefixLengths.size() == prefixes.Suffixes_.size());

        prefixes.PrefixHolder_ = TRealPath(builder.Flush());

        for (auto prefixLength : prefixLengths) {
            prefixes.Prefixes_.push_back(
                TAbsolutePathBuf::UnsafeMakeCanonicalPath(
                    TYPathBuf(prefixes.PrefixHolder_.Underlying().data(), prefixLength)
                )
            );
        }

        YT_VERIFY(prefixes.Prefixes_.size() == prefixes.Suffixes_.size());

        return prefixes;
    }

private:
    TRealPath PrefixHolder_;
};

bool StartsWithAmpersand(TYPathBuf pathSuffix)
{
    // NB: Such places are implemented via |TTokenizer| abstraction but they can
    // be probably implemented more optimally via straightforward checking of
    // first byte. The reason to not do it is unnecessary abstraction layer...
    // TODO(kvk1920): think of it.
    TTokenizer tokenizer(pathSuffix);
    tokenizer.Advance();
    return tokenizer.GetType() == ETokenType::Ampersand;
}

bool ShouldFollowLink(TYPathBuf unresolvedSuffix, bool pathIsAdditional, TStringBuf method)
{
    TTokenizer tokenizer(unresolvedSuffix);
    tokenizer.Advance();

    if (tokenizer.GetType() == ETokenType::Ampersand) {
        return false;
    }

    if (tokenizer.GetType() == ETokenType::Slash) {
        return true;
    }

    tokenizer.Expect(NYPath::ETokenType::EndOfStream);

    // NB: When link is last component of request's path we try to avoid
    // actions leading to data loss. E.g., it's better to remove link instead
    // of table pointed by link.

    YT_LOG_ALERT_IF(pathIsAdditional && method != "Copy",
        "Attempting to resolve path as additional for an unexpected method (Method: %v)",
        method);

    if (method == "Copy" && pathIsAdditional) {
        return true;
    }

    static const TStringBuf MethodsWithoutRedirection[] = {
        "Remove",
        "Create",
        "Set",
        "Copy",
        "LockCopyDestination",
        "AssembleTreeCopy",
    };

    return std::end(MethodsWithoutRedirection) == std::find(
        std::begin(MethodsWithoutRedirection),
        std::end(MethodsWithoutRedirection),
        method);
}

////////////////////////////////////////////////////////////////////////////////

struct TForwardToMaster
{
    TYPath Path;
};

struct TResolveHere
{
    TSequoiaResolveResult Result;
};

struct TResolveThere
{
    TSequoiaResolveIterationResult Result;
    TYPath RewrittenTargetPath;
};

using TResolveIterationResult = std::variant<
    TForwardToMaster,
    TResolveHere,
    TResolveThere>;

TResolveIterationResult ResolveByPath(
    const TSequoiaSessionPtr& session,
    TStringBuf method,
    TYPath path,
    bool pathIsAdditional)
{
    auto prefixesToResolve = TPathPrefixes::CollectPrefixes(path);
    auto nodeIds = session->FindNodeIds(prefixesToResolve.Prefixes());
    YT_VERIFY(prefixesToResolve.Prefixes().size() == nodeIds.size());

    int index = 0;
    // Skip prefixes before scion.
    while (index < std::ssize(nodeIds) && !nodeIds[index]) {
        ++index;
    }

    if (index == std::ssize(nodeIds)) {
        // We haven't found neither link nor scion.
        return TForwardToMaster{std::move(path)};
    }

    TNodeId resolvedId = NullObjectId;
    std::optional<TAbsolutePathBuf> resolvedPath;
    TNodeId resolvedParentId = NullObjectId;
    std::optional<TYPathBuf> unresolvedSuffix;

    for (; index < std::ssize(nodeIds); ++index) {
        if (!nodeIds[index]) {
            continue;
        }

        auto nodeId = nodeIds[index];

        auto prefix = prefixesToResolve.Prefixes()[index];
        auto suffix = prefixesToResolve.Suffixes()[index];

        auto nodeType = TypeFromId(nodeId);
        if ((nodeType == EObjectType::Scion && StartsWithAmpersand(suffix)) ||
            (nodeType == EObjectType::Link && !ShouldFollowLink(suffix, pathIsAdditional, method)))
        {
            return TForwardToMaster{std::move(path)};
        }

        resolvedParentId = resolvedId;
        resolvedId = nodeId;
        resolvedPath = prefix;
        unresolvedSuffix = suffix;

        if (IsLinkType(nodeType) && ShouldFollowLink(suffix, pathIsAdditional, method)) {
            // Failure here means that Sequoia resolve tables are inconsistent.
            auto targetPath = session->GetLinkTargetPath(nodeId);
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
        .UnresolvedSuffix = TYPath(*unresolvedSuffix),
        .ParentId = resolvedParentId,
    }};
}

TResolveIterationResult ResolveByObjectId(
    const TSequoiaSessionPtr& session,
    TStringBuf method,
    TYPath path,
    bool pathIsAdditional,
    TObjectId rootDesignator,
    ptrdiff_t suffixOffset)
{
    if (!ShouldBeResolvedInSequoia(rootDesignator)) {
        return TForwardToMaster{std::move(path)};
    }

    auto pathSuffix = TYPathBuf(path, suffixOffset);

    // If path starts with "#<object-id>" we try to find it in
    // "node_id_to_path" Sequoia table. After that we replace object ID with
    // its path and continue resolving it.
    if (auto resolvedNode = session->FindNodePath(rootDesignator)) {
        // NB: Ampersand is resolved (i.e. separated from unresolved
        // |pathSuffix|) in the next step. But in case of ampersand absence we
        // can do path rewriting here. Note that we don't need to distinguish
        // regular and snapshot nodes here.
        if (IsLinkType(TypeFromId(rootDesignator)) && ShouldFollowLink(pathSuffix, pathIsAdditional, method)) {
            auto targetPath = session->GetLinkTargetPath(rootDesignator);
            targetPath += pathSuffix;

            return TResolveThere{
                .Result = {
                    .Id = rootDesignator,
                    .Path = std::move(resolvedNode->Path),
                },
                .RewrittenTargetPath = std::move(targetPath),
            };
        }

        if (resolvedNode->IsSnapshot) {
            return TResolveHere{{
                .Id = rootDesignator,
                .Path = std::move(resolvedNode->Path),
                .UnresolvedSuffix = TYPath(pathSuffix),
                // Snapshot locks of scions are forbidden so to use null parent
                // ID is sufficient to distinguish snapshot from regular node.
                .ParentId = NullObjectId,
            }};
        }

        auto rewrittenPath = std::move(resolvedNode->Path).Underlying();
        rewrittenPath += pathSuffix;
        return ResolveByPath(session, method, std::move(rewrittenPath), pathIsAdditional);
    }

    // NB: of course, we could respond just after resolve in Sequoia tables.
    // But while we don't have any way to bypass Sequoia resolve we use "exists"
    // verb in tests to check object existence in master.
    // TODO(kvk1920): design some way to bypass Sequoia resolve.
    if (method == "Exists" || method == "Get") {
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
    TStringBuf method,
    TYPath path,
    bool pathIsAdditional)
{
    auto [rootDesignator, pathSuffix] = GetRootDesignator(path);
    return Visit(rootDesignator,
        [&] (TObjectId id) {
            auto suffixOffset = pathSuffix.data() - path.data();
            return ResolveByObjectId(session, method, std::move(path), pathIsAdditional, id, suffixOffset);
        },
        [&] (TSlashRootDesignatorTag /*tag*/) {
            return ResolveByPath(session, method, std::move(path), pathIsAdditional);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool TSequoiaResolveResult::IsSnapshot() const noexcept
{
    return TypeFromId(Id) != EObjectType::Scion && !ParentId;
}

TResolveResult ResolvePath(
    const TSequoiaSessionPtr& session,
    TYPath path,
    bool pathIsAdditional,
    TStringBuf service,
    TStringBuf method,
    std::vector<TSequoiaResolveIterationResult>* history)
{
    auto tokenizer = TTokenizer(path);
    tokenizer.Advance();
    auto firstTokenType = tokenizer.GetType();

    if (service == TMasterYPathProxy::GetDescriptor().ServiceName) {
        return TMasterResolveResult{};
    }

    // Paths starting with '&#...' are used for accessing replicated
    // transactions and have to be resolved by master.
    // Empty paths are also special and must be forwarded.
    if (firstTokenType == ETokenType::Ampersand || firstTokenType == ETokenType::EndOfStream) {
        return TCypressResolveResult{std::move(path)};
    }

    if (history) {
        history->clear();
    }

    for (int resolutionDepth = 0; ; ++resolutionDepth) {
        ValidateYPathResolutionDepth(path, resolutionDepth);

        auto iterationResult = RunResolveIteration(session, method, std::move(path), pathIsAdditional);
        static_assert(std::variant_size<decltype(iterationResult)>() == 3);

        if (auto* forwardToMaster = std::get_if<TForwardToMaster>(&iterationResult)) {
            return TCypressResolveResult{
                .Path = std::move(forwardToMaster->Path),
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
