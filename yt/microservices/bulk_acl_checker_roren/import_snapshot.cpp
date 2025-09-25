#include <yt/microservices/bulk_acl_checker_roren/import_snapshot.h>
#include <yt/microservices/bulk_acl_checker_roren/data.pb.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/yt/yt.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>
#include <yt/yt/client/hedging/options.h>
#include <yt/yt/client/hedging/rpc.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/https/client.h>
#include <yt/yt/core/https/config.h>
#include <yt/yt/library/auth/auth.h>
#include <yt/yt/library/named_value/named_value.h>

#include <library/cpp/iterator/zip.h>

#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>
#include <util/system/env.h>

#include <iomanip>
#include <optional>
#include <sstream>

using namespace NYT;
using namespace NRoren;

const THashMap<TString, NSecurityClient::ESecurityAction> STRING_TO_SECURITY_ACTION = {
    {"allow", NSecurityClient::ESecurityAction::Allow},
    {"deny", NSecurityClient::ESecurityAction::Deny},
};

const THashMap<TString, NSecurityClient::EAceInheritanceMode> STRING_TO_INHERITANCEMODE = {
    {"object_only", NSecurityClient::EAceInheritanceMode::ObjectOnly},
    {"object_and_descendants", NSecurityClient::EAceInheritanceMode::ObjectAndDescendants},
    {"descendants_only", NSecurityClient::EAceInheritanceMode::DescendantsOnly},
    {"immediate_descendants_only", NSecurityClient::EAceInheritanceMode::ImmediateDescendantsOnly},
};

const std::vector<std::pair<NSecurityClient::ESecurityAction, NSecurityClient::EAceInheritanceMode>> INDEX_TO_ACL = {
    {NSecurityClient::ESecurityAction::Allow, NSecurityClient::EAceInheritanceMode::ObjectOnly},
    {NSecurityClient::ESecurityAction::Allow, NSecurityClient::EAceInheritanceMode::ObjectAndDescendants},
    {NSecurityClient::ESecurityAction::Allow, NSecurityClient::EAceInheritanceMode::DescendantsOnly},
    {NSecurityClient::ESecurityAction::Allow, NSecurityClient::EAceInheritanceMode::ImmediateDescendantsOnly},
    {NSecurityClient::ESecurityAction::Deny, NSecurityClient::EAceInheritanceMode::ObjectOnly},
    {NSecurityClient::ESecurityAction::Deny, NSecurityClient::EAceInheritanceMode::ObjectAndDescendants},
    {NSecurityClient::ESecurityAction::Deny, NSecurityClient::EAceInheritanceMode::DescendantsOnly},
    {NSecurityClient::ESecurityAction::Deny, NSecurityClient::EAceInheritanceMode::ImmediateDescendantsOnly},
};

const THashMap<std::pair<NSecurityClient::ESecurityAction, NSecurityClient::EAceInheritanceMode>, size_t> ACL_TO_INDEX = {
    {{NSecurityClient::ESecurityAction::Allow, NSecurityClient::EAceInheritanceMode::ObjectOnly}, 0},
    {{NSecurityClient::ESecurityAction::Allow, NSecurityClient::EAceInheritanceMode::ObjectAndDescendants}, 1},
    {{NSecurityClient::ESecurityAction::Allow, NSecurityClient::EAceInheritanceMode::DescendantsOnly}, 2},
    {{NSecurityClient::ESecurityAction::Allow, NSecurityClient::EAceInheritanceMode::ImmediateDescendantsOnly}, 3},
    {{NSecurityClient::ESecurityAction::Deny, NSecurityClient::EAceInheritanceMode::ObjectOnly}, 4},
    {{NSecurityClient::ESecurityAction::Deny, NSecurityClient::EAceInheritanceMode::ObjectAndDescendants}, 5},
    {{NSecurityClient::ESecurityAction::Deny, NSecurityClient::EAceInheritanceMode::DescendantsOnly}, 6},
    {{NSecurityClient::ESecurityAction::Deny, NSecurityClient::EAceInheritanceMode::ImmediateDescendantsOnly}, 7},
};

const THashMap<NSecurityClient::EAceInheritanceMode, std::optional<NSecurityClient::EAceInheritanceMode>> INHERITANCE_MODE_EVOLUTION = {
    {NSecurityClient::EAceInheritanceMode::ObjectOnly, std::nullopt},
    {NSecurityClient::EAceInheritanceMode::ObjectAndDescendants, NSecurityClient::EAceInheritanceMode::ObjectAndDescendants},
    {NSecurityClient::EAceInheritanceMode::DescendantsOnly, NSecurityClient::EAceInheritanceMode::ObjectAndDescendants},
    {NSecurityClient::EAceInheritanceMode::ImmediateDescendantsOnly, NSecurityClient::EAceInheritanceMode::ObjectOnly},
};

using TAcl = THashMap<NSecurityClient::ESecurityAction, THashMap<NSecurityClient::EAceInheritanceMode, THashSet<TString>>>;

// An empty TAcl with keys {ACL actions} X {inheritance modes}.
// See page https://ytsaurus.tech/docs/en/user-guide/storage/access-control.
const TAcl DEFAULT_ACL = {
    {
        NSecurityClient::ESecurityAction::Allow,
        {
            {NSecurityClient::EAceInheritanceMode::ObjectOnly, {}},
            {NSecurityClient::EAceInheritanceMode::ObjectAndDescendants, {}},
            {NSecurityClient::EAceInheritanceMode::DescendantsOnly, {}},
            {NSecurityClient::EAceInheritanceMode::ImmediateDescendantsOnly, {}},
        }
    },
    {
        NSecurityClient::ESecurityAction::Deny,
        {
            {NSecurityClient::EAceInheritanceMode::ObjectOnly, {}},
            {NSecurityClient::EAceInheritanceMode::ObjectAndDescendants, {}},
            {NSecurityClient::EAceInheritanceMode::DescendantsOnly, {}},
            {NSecurityClient::EAceInheritanceMode::ImmediateDescendantsOnly, {}},
        }
    },
};

// Creates a node's child's ACL of by the node's ACL. If a node has an ACE (action, inheritanceMode, subject),
// its child gets an ACE (action, INHERITANCE_MODE_EVOLUTION[inheritanceMode], subject).
std::optional<TAcl> StepAcl(const std::optional<TAcl>& acl)
{
    if (!acl.has_value()) {
        return std::nullopt;
    }
    const auto& aclValue = acl.value();
    auto newReadAcl = DEFAULT_ACL;
    for (const auto& [action, inheritanceModes] : aclValue) {
        for (const auto& [inheritanceMode, subjects] : inheritanceModes) {
            auto it = INHERITANCE_MODE_EVOLUTION.find(inheritanceMode);
            Y_ABORT_IF(it == INHERITANCE_MODE_EVOLUTION.end());
            if (it->second.has_value()) {
                newReadAcl[action][it->second.value()].insert(subjects.begin(), subjects.end());
            }
        }
    }
    return newReadAcl;
}

// Merges ACLs. If A has ACEs (action, inheritanceMode, subjectsA={...}), B has ACEs (action, inheritanceMode, subjectsB={...}),
// than the result will have ACEs (action, inheritanceMode, subjectsA+subjectsB).
TAcl CombineAcl(const std::optional<TAcl>& aclA, const std::optional<TAcl>& aclB)
{
    if (!aclA.has_value()) {
        return CombineAcl(DEFAULT_ACL, aclB);
    }
    if (!aclB.has_value()) {
        return CombineAcl(aclA, DEFAULT_ACL);
    }
    const auto& aclBValue = aclB.value();
    auto newReadAcl = DEFAULT_ACL;
    for (const auto& [action, inheritanceModes] : aclA.value()) {
        for (const auto& [inheritanceMode, subjects] : inheritanceModes) {
            auto& newSubjects = newReadAcl[action][inheritanceMode];
            newSubjects = subjects;
            auto actionIt = aclBValue.find(action);
            if (actionIt != aclBValue.end()) {
                auto inheritanceModeIt = actionIt->second.find(inheritanceMode);
                if (inheritanceModeIt != actionIt->second.end()) {
                    newSubjects.insert(inheritanceModeIt->second.begin(), inheritanceModeIt->second.end());
                }
            }
        }
    }
    return newReadAcl;
}

// Creates TAcl from TNode, taking only read permission.
TAcl MapAcl(const TNode& acl)
{
    auto readAcl = DEFAULT_ACL;
    Y_ABORT_IF(!acl.IsList());
    for (const auto& ace : acl.AsList()) {
        Y_ABORT_IF(!ace.IsMap());
        auto aceAsMap = ace.AsMap();
        auto permissions = aceAsMap.find("permissions");
        Y_ABORT_IF(permissions == aceAsMap.end());
        Y_ABORT_IF(!permissions->second.IsList());
        auto& permissionsList = permissions->second.AsList();
        if (Find(permissionsList.begin(), permissionsList.end(), TNode("read")) == permissionsList.end()) {
            continue;
        }

        auto action = aceAsMap.find("action");
        Y_ABORT_IF(action == aceAsMap.end());
        Y_ABORT_IF(!action->second.IsString());
        auto aclActionIt = STRING_TO_SECURITY_ACTION.find(action->second.AsString());
        Y_ABORT_IF(aclActionIt == STRING_TO_SECURITY_ACTION.end());

        auto inheritanceMode = aceAsMap.find("inheritance_mode");
        Y_ABORT_IF(inheritanceMode == aceAsMap.end());
        Y_ABORT_IF(!inheritanceMode->second.IsString());
        auto inheritanceModeIt = STRING_TO_INHERITANCEMODE.find(inheritanceMode->second.AsString());
        Y_ABORT_IF(inheritanceModeIt == STRING_TO_INHERITANCEMODE.end());

        auto subjects = aceAsMap.find("subjects");
        Y_ABORT_IF(subjects == aceAsMap.end());
        Y_ABORT_IF(!subjects->second.IsList());

        auto& readAclSubjects = readAcl[aclActionIt->second][inheritanceModeIt->second];
        for (const auto& node : subjects->second.AsList()) {
            Y_ABORT_IF(!node.IsString());
            readAclSubjects.insert(node.AsString());
        }
    }
    return readAcl;
}

// Map from index of pair (actions, inheritanceMode) to sorted vector of subjects.
using TAclDump = TMap<TString, std::vector<TString>>;

// Converts TAcl to TAclDump. The inverse function to LoadAclFromDict.
// Actually just merges keys [action][inheritanceMode] into one key [ACL_TO_INDEX[{action, inheritanceMode}]].
TAclDump DumpAclToDict(const std::optional<TAcl>& readAcl)
{
    if (!readAcl.has_value()) {
        return DumpAclToDict(DEFAULT_ACL);
    }
    TAclDump dump;
    for (const auto& [action, inheritanceModes] : readAcl.value()) {
        for (const auto& [inheritanceMode, subjects] : inheritanceModes) {
            if (!subjects.empty()) {
                std::vector<TString> subjectsSorted(subjects.begin(), subjects.end());
                Sort(subjectsSorted.begin(), subjectsSorted.end());
                dump.emplace(ToString(ACL_TO_INDEX.at(std::pair(action, inheritanceMode))), std::move(subjectsSorted));
            }
        }
    }
    return dump;
}

// Converts TAclDump to TAcl. The inverse function to DumpAclToDict.
TAcl LoadAclFromDict(const TAclDump& dump)
{
    TAcl readAcl = DEFAULT_ACL;
    for (const auto& [indexOfAcl, sortedSubjects] : dump) {
        const auto& [action, inheritanceMode] = INDEX_TO_ACL[FromString<size_t>(indexOfAcl)];
        THashSet<TString> subjects(sortedSubjects.begin(), sortedSubjects.end());
        readAcl[action][inheritanceMode] = std::move(subjects);
    }
    return readAcl;
}

TNode NodeFromTAclDump(const TAclDump& dump)
{
    TNode result = TNode::CreateMap();
    for (const auto& [stringIndexOfAcl, sortedSubjects] : dump) {
        auto sortedSubjectsNode = TNode::CreateList();
        auto& sortedSubjectsNodeList = sortedSubjectsNode.AsList();
        sortedSubjectsNodeList.reserve(sortedSubjects.size());
        for (const auto& subject : sortedSubjects) {
            sortedSubjectsNodeList.push_back(TNode{subject});
        }
        result[stringIndexOfAcl] = std::move(sortedSubjectsNode);
    }
    return result;
}

class TACLTrieNode
{
public:
    TACLTrieNode(const TStringBuf path, const std::optional<TAcl>& acl = std::nullopt, TACLTrieNode* nonTrivialPredecessor = nullptr, size_t nonTrivialPredecessorDepth = 0)
        : Path_(path)
        , Acl_(std::move(acl))
        , AclPropagation_({Acl_})
        , NonTrivialPredecessor_(!nonTrivialPredecessor ? this : nonTrivialPredecessor)
        , NonTrivialPredecessorDepth_(!nonTrivialPredecessor ? 0 : nonTrivialPredecessorDepth)
    {
    }

    bool IsTrivial() const
    {
        return NonTrivialPredecessor_ != this;
    }

    std::optional<TAcl> GetAcl(size_t depth = 0)
    {
        if (!IsTrivial()) {
            auto queryDepth = Min(depth, (size_t)2);
            while (AclPropagation_.size() <= queryDepth) {
                AclPropagation_.push_back(StepAcl(AclPropagation_.back()));
            }
            return AclPropagation_[queryDepth];
        }
        return NonTrivialPredecessor_->GetAcl(depth + NonTrivialPredecessorDepth_);
    }

    TString ChildPath(const TStringBuf part)
    {
        return NYT::Format("%v/%v", Path_, part);
    }

    TSimpleSharedPtr<TACLTrieNode> GetChild(const TStringBuf pathPart, bool create = true)
    {
        // Two lookups here because if make one, have to create TACLTrieNode always.
        if (!Children_.contains(pathPart) && create) {
            Children_[pathPart] = MakeSimpleShared<TACLTrieNode>(
                ChildPath(pathPart),
                std::nullopt,
                NonTrivialPredecessor_,
                NonTrivialPredecessorDepth_ + 1);
        }
        return Children_.at(pathPart);
    }

    void SetAcl(bool inheritAcl, const TAcl& acl)
    {
        Acl_ = inheritAcl ? CombineAcl(GetAcl(), acl) : acl;
        AclPropagation_ = {Acl_};
        NonTrivialPredecessor_ = this;
        NonTrivialPredecessorDepth_ = 0;
    }

    TNode Dump()
    {
        TNode result = TNode::CreateList();
        auto& resultList = result.AsList();

        TNode dataC = TNode::CreateMap();
        for (auto& [key, aclTrieNode] : Children_) {
            dataC[key] = aclTrieNode->Dump();
        }
        resultList.push_back(std::move(dataC));

        auto aclDump = NodeFromTAclDump(DumpAclToDict(GetAcl()));
        if (!IsTrivial()) {
            resultList.push_back(std::move(aclDump));
        } else {
            resultList.push_back(TNode::CreateEntity());
        }
        return result;
    }

private:
    TString Path_;
    THashMap<TString, TSimpleSharedPtr<TACLTrieNode>> Children_;
    std::optional<TAcl> Acl_;
    std::vector<std::optional<TAcl>> AclPropagation_;
    TACLTrieNode* NonTrivialPredecessor_;
    size_t NonTrivialPredecessorDepth_;
};

////////////////////////////////////////////////////////////////////////////////

class TACLTrie
{
public:
    explicit TACLTrie(const TStringBuf path = "")
        : Root_(MakeSimpleShared<TACLTrieNode>(path))
    {
    }

    void SetAcl(const TStringBuf path, bool inheritAcl, const TAcl& acl)
    {
        std::vector<TString> pathParts = StringSplitter(path).Split('/');
        TSimpleSharedPtr<TACLTrieNode> node = Root_;
        for (size_t i = 1; i < pathParts.size(); ++i) {
            node = node->GetChild(pathParts[i]);
        }
        node->SetAcl(inheritAcl, acl);
    }

    TNode Dump()
    {
        return Root_->Dump();
    }

private:
    TSimpleSharedPtr<TACLTrieNode> Root_;
};

////////////////////////////////////////////////////////////////////////////////

struct TRowAfterMap
{
    TString Group;
    bool InheritAcl;
    TString Path;
    TAclDump Acl;

    Y_SAVELOAD_DEFINE(
        Group,
        InheritAcl,
        Path,
        Acl);
};

std::vector<TRowAfterMap> GetSortedRows(const TInputPtr<TRowAfterMap>& rowIterator)
{
    std::vector<TRowAfterMap> rows;
    for (const auto& row : rowIterator) {
        rows.push_back(row);
    }
    Sort(rows.begin(), rows.end(), [](const TRowAfterMap& lhs, const TRowAfterMap& rhs) {
        return std::tuple{lhs.Path, lhs.InheritAcl, lhs.Acl} < std::tuple{rhs.Path, rhs.InheritAcl, rhs.Acl};
    });
    return rows;
}

TACLTrie BuildTAclTrie(const TInputPtr<TRowAfterMap>& rowIterator)
{
    TACLTrie trie = TACLTrie();
    std::vector<TRowAfterMap> sortedRows = GetSortedRows(rowIterator);
    for (const auto& entry : sortedRows) {
        trie.SetAcl(entry.Path, entry.InheritAcl, LoadAclFromDict(entry.Acl));
    }
    return trie;
}

void TryRemove(const NYT::IClientPtr ytClient, const std::vector<TString>& paths)
{
    for (const auto& path : paths) {
        if (ytClient->Exists(path)) {
            ytClient->Remove(path);
        }
    }
}

TPipeline CreateEmptyPipeline(const TString& cluster,
    const TString& tmpDir,
    const std::optional<TString>& pool,
    i64 memoryLimit)
{
    TYtPipelineConfig config;
    config.SetCluster(cluster);
    config.SetWorkingDir(tmpDir);

    TNode specPatch;
    if (pool) {
        specPatch["pool"] = pool.value();
    }
    TString userEnv = GetEnv("USER", "root");
    Y_ABORT_IF(userEnv.empty());
    specPatch["mapper"]["environment"]["USER"] = userEnv;
    // TODO(YT-22593): Migrate to std::string.
    specPatch["secure_vault"]["YT_TOKEN"] = TString(NAuth::LoadToken().value_or(""));
    specPatch["reducer"]["memory_limit"] = memoryLimit;
    (*config.MutableOperatinonConfig())["BulkAclCheckerParDo"].SetSpecPatch(NodeToYsonString(specPatch));
    return MakeYtPipeline(config);
}

void MakeAclTreePipeline(
    const TString& input,
    const TString& output,
    const TPipeline& pipeline)
{
    pipeline
    | YtRead<TInputMessage>(input)
    | "BulkAclCheckerParDo" >> ParDo([](const TInputMessage& row, TOutput<TRowAfterMap>& output) {
        TNode aclNode = NodeFromYsonString(row.GetAcl());
        if (!row.GetInheritAcl() || (aclNode.IsList() && !aclNode.AsList().empty())) {
            TRowAfterMap result;
            result.Group = "root";
            result.InheritAcl = row.GetInheritAcl();
            result.Path = row.GetPath();

            result.Acl = DumpAclToDict(MapAcl(aclNode));
            output.Add(result);
        }
    })
    | ParDo([](const TRowAfterMap& row) -> TKV<TString, TRowAfterMap> {
        TKV<TString, TRowAfterMap> kv;
        kv.Key() = row.Group;
        kv.Value() = row;
        return kv;
    })
    | GroupByKey()
    | ParDo([](const TKV<TString, TInputPtr<TRowAfterMap>>& kv, TOutput<TOutputMessage>& output) {
        TACLTrie trie = BuildTAclTrie(kv.Value());
        TString data = NodeToYsonString(trie.Dump());
        size_t partSize = 1024 * 1024;
        for (size_t idx = 0; idx < data.size(); idx += partSize) {
            TOutputMessage result;
            result.SetIdx(idx);
            result.SetPart(TString{data.begin() + idx, data.begin() + (idx + Min(partSize, data.size() - idx))});
            output.Add(result);
        }
    })
    | YtWrite(NYT::TRichYPath{output}.OptimizeFor(EOptimizeForAttr::OF_LOOKUP_ATTR), CreateTableSchema<TOutputMessage>());
}

// This function parses a string time to timestamp IN LOCAL timezone.
// std::get_time does what is needed. I (daterenichev) didn't find its analogue in Arcadia, I also asked in Arcadia Public Chat.
i64 ParseTime(const TStringBuf s)
{
    std::tm timeParsed{};
    std::stringstream ss(s.data());
    ss >> std::get_time(&timeParsed, "%Y-%m-%dT%H:%M:%S");
    Y_ABORT_IF(ss.fail());
    return mktime(&timeParsed);
}

std::optional<TString> ExtractSnapshotId(const TString& objectName)
{
    std::vector<TString> splitByColon = StringSplitter(objectName).Split(':');
    if (splitByColon.size() != 2) {
        return {};
    }
    const auto& snapshotData = splitByColon[1];
    TStringBuf snapshotId;
    TStringBuf numberAfterSnapshotId;
    if (!TStringBuf{snapshotData}.TryRSplit('.', snapshotId, numberAfterSnapshotId)) {
        return {};
    }
    return TString{snapshotId};
}

THashSet<TString> GetSnapshotsToProcess(
    const TString& snapshotId,
    size_t snapshotLimit,
    bool force,
    const NYT::IClientPtr ytClient,
    const TString& destination)
{
    THashSet<TString> snapshots;

    TStringBuf unifiedExportSuffix{"_unified_export"};
    if (snapshotId == "all") {
        for (const auto& exportIdNode : ytClient->List("//sys/admin/snapshots/snapshot_exports")) {
            Y_ABORT_IF(!exportIdNode.IsString());
            TString exportId = exportIdNode.AsString();
            if (exportId.EndsWith(unifiedExportSuffix)) {
                exportId.remove(exportId.size() - unifiedExportSuffix.size());
                snapshots.insert(exportId);
            }
        }
    } else if (snapshotId == "latest") {
        auto exportIdNode = ytClient->Get("//sys/admin/snapshots/snapshot_exports/latest/@key");
        Y_ABORT_IF(!exportIdNode.IsString());
        TString exportId = exportIdNode.AsString();
        if (exportId.EndsWith(unifiedExportSuffix)) {
            exportId.remove(exportId.size() - unifiedExportSuffix.size());
        }
        snapshots.insert(exportId);
    } else {
        snapshots.insert(snapshotId);
    }

    if (snapshotLimit > 0 && snapshots.size() > snapshotLimit) {
        std::vector<TString> snapshotsTmpVec(snapshots.begin(), snapshots.end());
        Sort(snapshotsTmpVec.rbegin(), snapshotsTmpVec.rend());
        snapshots = THashSet<TString>(snapshotsTmpVec.begin(), snapshotsTmpVec.begin() + snapshotLimit);
    }

    if (!force) {
        for (const auto& objectName : ytClient->List(destination)) {
            Y_ABORT_IF(!objectName.IsString());
            auto snapshotToErase = ExtractSnapshotId(objectName.AsString());
            if (snapshotToErase) {
                snapshots.erase(snapshotToErase.value());
            }
        }
    }

    return snapshots;
}

TImportSnapshotsMain::TImportSnapshotsMain(const NLastGetopt::TOpts& opts)
    : Opts_(opts) {}

void TImportSnapshotsMain::ParseArgs(int argc, const char** argv)
{
    NLastGetopt::TOptsParseResult r(&Opts_, argc, argv);

    Y_ABORT_IF(r.GetFreeArgCount() > 1); // Where was a call opts.SetFreeArgsMax(1) before. This Y_ABORT_IF checks that it has actually been executed.
    if (r.GetFreeArgCount() == 1) {
        Options_.Cluster = r.GetFreeArgs()[0];
    } else {
        Options_.Cluster = GetEnv("YT_PROXY", "");
        if (Options_.Cluster.empty()) {
            Opts_.PrintUsage(argv[0], Cerr);
            exit(1);
        }
    }
    Options_.Destination = r.Get("destination");
    Options_.SnapshotId = r.Get("snapshot-id");
    Options_.SnapshotLimit = FromString<size_t>(r.Get("snapshot-limit"));
    Options_.Pool = r.Has("pool") ? r.Get("pool") : std::optional<TString>{};
    Options_.Force = r.Has("force");
    Options_.MemoryLimit = FromString<size_t>(r.Get("memory-limit"));

    if (r.Has("enable-ipv4")) {
        auto resolverConfig = NYT::New<NYT::NNet::TAddressResolverConfig>();
        resolverConfig->EnableIPv4 = true;
        NYT::NNet::TAddressResolver::Get()->Configure(resolverConfig);
    }
}

int TImportSnapshotsMain::ImportSnapshots()
{
    const auto ytClient = NYT::CreateClient(Options_.Cluster);
    ytClient->Create(NYT::Format("%v/tmp", Options_.Destination), NYT::NT_MAP, NYT::TCreateOptions().Recursive(true).IgnoreExisting(true));
    auto basePath = Options_.Destination;

    auto snapshots = GetSnapshotsToProcess(Options_.SnapshotId, Options_.SnapshotLimit, Options_.Force, ytClient, Options_.Destination);
    TString tmpDir = NYT::Format("%v/tmp", basePath);

    auto pipeline = CreateEmptyPipeline(Options_.Cluster, tmpDir, Options_.Pool, Options_.MemoryLimit);

    std::vector<TString> tmpSkeletonPaths;
    std::vector<TString> skeletonPaths;
    tmpSkeletonPaths.reserve(snapshots.size());
    skeletonPaths.reserve(snapshots.size());

    for (const auto& snapshotId : snapshots) {
        TString exportPath = NYT::Format("//sys/admin/snapshots/snapshot_exports/%v_unified_export", snapshotId);

        TNode snapshotCreationZuluTime = ytClient->Get(NYT::Format("%v/@creation_time", exportPath));
        if (!snapshotCreationZuluTime.IsString()) {
            Y_ABORT("Could not get creation_time of [%s]", exportPath.data());
        }
        auto snapshotCreationZuluTimestamp = ParseTime(snapshotCreationZuluTime.AsString());

        TString skeletonPath = NYT::Format("%v/%v:%v.effective_acl_skeleton", basePath, snapshotCreationZuluTimestamp, snapshotId);
        TString tmpSkeletonPath = NYT::Format("%v/tmp/%v:%v.effective_acl_skeleton", basePath, snapshotCreationZuluTimestamp, snapshotId);

        Cerr << NYT::Format("Attempting import of [%v]", snapshotId) << Endl;

        MakeAclTreePipeline(exportPath, tmpSkeletonPath, pipeline);

        tmpSkeletonPaths.push_back(std::move(tmpSkeletonPath));
        skeletonPaths.push_back(std::move(skeletonPath));
    }
    pipeline.Run();

    for (const auto& [snapshotId, tmpSkeletonPath, skeletonPath] : Zip(snapshots, tmpSkeletonPaths, skeletonPaths)) {
        auto tx = ytClient->StartTransaction();
        ytClient->Move(tmpSkeletonPath, skeletonPath, NYT::TMoveOptions().Force(true));
        std::vector<TString> mustBeRemoved{tmpSkeletonPath};
        TryRemove(ytClient, mustBeRemoved);
        tx->Commit();

        Cerr << NYT::Format("Import of [%v] done.", snapshotId) << Endl;
    }

    return 0;
}

int TImportSnapshotsMain::operator()(int argc, const char** argv)
{
    ParseArgs(argc, argv);
    return ImportSnapshots();
}
