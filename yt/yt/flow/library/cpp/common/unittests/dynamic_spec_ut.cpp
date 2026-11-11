#include <yt/yt/flow/library/cpp/common/dynamic_spec.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NFlow {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TCountingVersionProvider
    : public IVersionProvider
{
public:
    TVersion GenerateVersion() override
    {
        return TVersion(++Version_);
    }

private:
    i64 Version_ = 0;
};

class TDynamicSpecPatchTest
    : public ::testing::Test
{
protected:
    IVersionProviderPtr Provider_ = New<TCountingVersionProvider>();
    TVersionedDynamicPipelineSpecPtr State_ = New<TVersionedDynamicPipelineSpec>();

    void SetBase(TStringBuf yson)
    {
        State_ = UpdateDynamicSpecBase(State_, ConvertToNode(TYsonString(yson)), Provider_);
    }

    void Patch(TDynamicSpecPatchRequest patchRequest)
    {
        if (!patchRequest.ExpectedVersion) {
            patchRequest.ExpectedVersion = State_->GetVersion();
        }
        State_ = ApplyDynamicSpecPatch(State_, patchRequest, Provider_);
    }

    void Set(const std::string& path, TStringBuf yson, std::optional<std::string> comment = "Set a test patch")
    {
        Patch({
            .Operation = EDynamicSpecPatchAction::Set,
            .Path = path,
            .Value = ConvertToNode(TYsonString(yson)),
            .Comment = std::move(comment),
        });
    }

    void CancelPatch(TVersion version)
    {
        Patch({.Operation = EDynamicSpecPatchAction::CancelPatch, .PatchVersion = version});
    }

    INodePtr GetNode(const std::string& path)
    {
        return FindNodeByYPath(ConvertToNode(State_->GetValue()), path);
    }

    int GetInt(const std::string& path)
    {
        return ConvertTo<int>(GetNode(path));
    }
};

INodePtr MakeNestedMap(int depth)
{
    auto node = ConvertToNode(1);
    for (int index = 0; index < depth; ++index) {
        auto parent = GetEphemeralNodeFactory()->CreateMap();
        parent->AddChild("x", node);
        node = parent;
    }
    return node;
}

std::string MakeDeepPath(int depth)
{
    std::string result;
    for (int index = 0; index < depth; ++index) {
        result += "/x";
    }
    return result;
}

TDynamicSpecPatchChange MakeChange(std::string path, int value)
{
    TDynamicSpecPatchChange change;
    change.Path = std::move(path);
    change.Value = ConvertToNode(value);
    return change;
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDynamicSpecPatchTest, EqualBaseValueIsPinnedAcrossRelease)
{
    SetBase("{x=1;}");
    auto version = State_->GetVersion();
    Set("/x", "1", "Keep this value");
    EXPECT_GT(State_->GetVersion(), version);
    auto patchVersion = State_->GetVersion();
    ASSERT_EQ(State_->Patches.size(), 1u);
    EXPECT_EQ(State_->Patches[0]->Version, patchVersion);
    EXPECT_EQ(State_->Patches[0]->Comment, "Keep this value");
    EXPECT_EQ(State_->Patches[0]->Timestamp, State_->GetLastUpdate());
    SetBase("{x=2;}");
    EXPECT_GT(State_->GetVersion(), patchVersion);
    EXPECT_EQ(GetInt("/x"), 1);
    EXPECT_EQ(State_->Patches[0]->Version, patchVersion);
    CancelPatch(patchVersion);
    EXPECT_EQ(GetInt("/x"), 2);
    EXPECT_TRUE(State_->Patches.empty());
}

TEST_F(TDynamicSpecPatchTest, PatchesCannotChangeTargetState)
{
    SetBase("{target_state=paused;x=1;}");
    auto version = State_->GetVersion();
    EXPECT_THROW(Set("/target_state", "stopped"), TErrorException);
    EXPECT_THROW(Set("", "{x=2;}"), TErrorException);
    EXPECT_EQ(State_->GetVersion(), version);
    EXPECT_TRUE(State_->Patches.empty());
}

TEST_F(TDynamicSpecPatchTest, ParentReplacementPreservesOlderChildPatches)
{
    SetBase("{x={a=1;b=2;c=3;};}");
    Set("/x/a", "10");
    auto firstChildPatch = State_->GetVersion();
    Set("/x/b", "20");
    auto secondChildPatch = State_->GetVersion();
    Set("/x", "{a=30;}");
    auto parentPatch = State_->GetVersion();
    ASSERT_EQ(State_->Patches.size(), 3u);
    EXPECT_EQ(State_->Patches[0]->Version, firstChildPatch);
    EXPECT_EQ(State_->Patches[1]->Version, secondChildPatch);
    EXPECT_EQ(State_->Patches[2]->Version, parentPatch);
    EXPECT_EQ(BuildDynamicSpecPatchesSnapshot(State_)->AsMap()->GetChildOrThrow("patches")->AsList()->GetChildCount(), 3);
    EXPECT_EQ(GetInt("/x/a"), 30);
    EXPECT_FALSE(GetNode("/x/b"));
    EXPECT_FALSE(GetNode("/x/c"));
    SetBase("{x={a=101;b=102;c=103;new=104;};}");
    EXPECT_FALSE(GetNode("/x/new"));
    CancelPatch(parentPatch);
    EXPECT_EQ(GetInt("/x/a"), 10);
    EXPECT_EQ(GetInt("/x/b"), 20);
    EXPECT_EQ(GetInt("/x/c"), 103);
    EXPECT_EQ(GetInt("/x/new"), 104);
}

TEST_F(TDynamicSpecPatchTest, HiddenChildPatchCanBeCanceledWithoutChangingParent)
{
    SetBase("{x={a=1;};}");
    Set("/x/a", "10");
    auto childPatch = State_->GetVersion();
    Set("/x", "{a=30;}");
    auto parentPatch = State_->GetVersion();
    CancelPatch(childPatch);
    EXPECT_EQ(GetInt("/x/a"), 30);
    ASSERT_EQ(State_->Patches.size(), 1u);
    EXPECT_EQ(State_->Patches[0]->Version, parentPatch);
    CancelPatch(parentPatch);
    EXPECT_EQ(GetInt("/x/a"), 1);
}

TEST_F(TDynamicSpecPatchTest, PatchUpdateCanReplaceItsPaths)
{
    SetBase("{x={a=1;b=2;};}");
    Set("/x/a", "10", "Initial reason");
    auto patchId = State_->GetVersion();
    Patch({
        .Operation = EDynamicSpecPatchAction::Set,
        .Path = "/x/b",
        .Value = ConvertToNode(20),
        .PatchVersion = patchId,
    });
    EXPECT_EQ(GetInt("/x/a"), 1);
    EXPECT_EQ(GetInt("/x/b"), 20);
    ASSERT_EQ(State_->Patches.size(), 1u);
    EXPECT_EQ(State_->Patches[0]->Version, patchId);
    EXPECT_EQ(State_->Patches[0]->Comment, "Initial reason");
    CancelPatch(patchId);
    EXPECT_EQ(GetInt("/x/a"), 1);
    EXPECT_EQ(GetInt("/x/b"), 2);
}

TEST_F(TDynamicSpecPatchTest, ChildOverwriteAndPatchCancellationPreserveNewerChild)
{
    SetBase("{x={a=1;b=2;c=3;};}");
    Set("/x", "{a=10;b=20;}");
    auto mapVersion = State_->GetVersion();
    Set("/x/a", "11");
    auto childVersion = State_->GetVersion();
    EXPECT_EQ(ConvertTo<int>(State_->Patches[0]->Changes[0].Value->AsMap()->GetChildOrThrow("b")), 20);
    CancelPatch(mapVersion);
    EXPECT_EQ(GetInt("/x/a"), 11);
    EXPECT_EQ(GetInt("/x/b"), 2);
    EXPECT_EQ(GetInt("/x/c"), 3);
    ASSERT_EQ(State_->Patches.size(), 1u);
    EXPECT_EQ(State_->Patches[0]->Version, childVersion);
    CancelPatch(childVersion);
    EXPECT_EQ(GetInt("/x/a"), 1);
    EXPECT_TRUE(State_->Patches.empty());
}

TEST_F(TDynamicSpecPatchTest, CancelChildPatchRestoresWholeParentPatch)
{
    SetBase("{computations={noop={output_store={table_request={select_limit_multiplier=5;select_min_limit=10;};};};};}");
    auto parentPath = "/computations/noop/output_store/table_request";
    auto childPath = "/computations/noop/output_store/table_request/select_limit_multiplier";
    Set(parentPath, "{select_limit_multiplier=10;select_min_limit=20;}", "Replace table request");
    auto parentPatch = State_->GetVersion();
    auto parentChanges = GetNodeByYPath(BuildDynamicSpecPatchesSnapshot(State_, parentPatch), "/patches/0/changes/0/value");

    Set(childPath, "45", "Patch limit");
    auto childPatch = State_->GetVersion();
    EXPECT_EQ(GetInt(childPath), 45);
    EXPECT_EQ(ConvertTo<NYTree::TSize>(GetNode(std::string(parentPath) + "/select_min_limit")), NYTree::TSize(20));
    auto changesWithChild = GetNodeByYPath(BuildDynamicSpecPatchesSnapshot(State_, parentPatch), "/patches/0/changes/0/value");
    EXPECT_TRUE(AreNodesEqual(parentChanges, changesWithChild));

    CancelPatch(childPatch);
    EXPECT_EQ(GetInt(childPath), 10);
    EXPECT_EQ(ConvertTo<NYTree::TSize>(GetNode(std::string(parentPath) + "/select_min_limit")), NYTree::TSize(20));
    ASSERT_EQ(State_->Patches.size(), 1u);
    EXPECT_EQ(State_->Patches[0]->Version, parentPatch);
    EXPECT_TRUE(AreNodesEqual(parentChanges,
        GetNodeByYPath(BuildDynamicSpecPatchesSnapshot(State_, parentPatch), "/patches/0/changes/0/value")));
}

TEST_F(TDynamicSpecPatchTest, NestedChildPatchLeavesEmptyParentPatchIntact)
{
    SetBase("{x={a={b=1;c=2;};};}");
    Set("/x", "{}", "Replace map");
    auto parentPatch = State_->GetVersion();
    auto parentChanges = GetNodeByYPath(BuildDynamicSpecPatchesSnapshot(State_, parentPatch), "/patches/0/changes/0/value");

    Set("/x/a/b", "45", "Add nested value");
    auto childPatch = State_->GetVersion();
    EXPECT_EQ(GetInt("/x/a/b"), 45);
    EXPECT_FALSE(GetNode("/x/a/c"));
    EXPECT_TRUE(AreNodesEqual(parentChanges,
        GetNodeByYPath(BuildDynamicSpecPatchesSnapshot(State_, parentPatch), "/patches/0/changes/0/value")));

    CancelPatch(childPatch);
    EXPECT_TRUE(GetNode("/x"));
    EXPECT_FALSE(GetNode("/x/a"));
    ASSERT_EQ(State_->Patches.size(), 1u);
    EXPECT_EQ(State_->Patches[0]->Version, parentPatch);
}

TEST_F(TDynamicSpecPatchTest, PatchListingKeepsRemainingMapReplacement)
{
    SetBase("{x={a=1;b=2;};}");
    Set("/x", "{a=10;b=20;}", "Replace map");
    auto mapPatch = State_->GetVersion();
    Set("/x/a", "11", "Change child");
    auto childPatch = State_->GetVersion();

    auto snapshot = BuildDynamicSpecPatchesSnapshot(State_);
    EXPECT_EQ(snapshot->AsMap()->GetChildValueOrThrow<TVersion>("version"), childPatch);
    auto patches = snapshot->AsMap()->GetChildOrThrow("patches")->AsList();
    ASSERT_EQ(patches->GetChildCount(), 2);
    auto first = patches->GetChildOrThrow(0)->AsMap();
    EXPECT_EQ(first->GetChildValueOrThrow<TVersion>("patch_id"), mapPatch);
    EXPECT_EQ(first->GetChildValueOrThrow<std::string>("comment"), "Replace map");
    EXPECT_EQ(ConvertTo<std::string>(GetNodeByYPath(first, "/changes/0/path")), "/x");
    EXPECT_FALSE(first->FindChild("operation"));
    EXPECT_EQ(ConvertTo<int>(GetNodeByYPath(first, "/changes/0/value/a")), 10);
    EXPECT_EQ(ConvertTo<int>(GetNodeByYPath(first, "/changes/0/value/b")), 20);

    auto selected = BuildDynamicSpecPatchesSnapshot(State_, childPatch);
    EXPECT_EQ(selected->AsMap()->GetChildOrThrow("patches")->AsList()->GetChildCount(), 1);
    EXPECT_THROW(BuildDynamicSpecPatchesSnapshot(State_, TVersion(999)), TErrorException);
}

TEST_F(TDynamicSpecPatchTest, UpdatingCommentKeepsPatchIdAndTimestampAndBumpsStateVersion)
{
    SetBase("{x=1;}");
    Set("/x", "2", "Initial reason");
    auto patchId = State_->GetVersion();
    auto timestamp = State_->Patches[0]->Timestamp;
    Patch({
        .Operation = EDynamicSpecPatchAction::UpdateComment,
        .PatchVersion = patchId,
        .Comment = "Updated reason",
    });
    EXPECT_GT(State_->GetVersion(), patchId);
    EXPECT_EQ(GetInt("/x"), 2);
    ASSERT_EQ(State_->Patches.size(), 1u);
    EXPECT_EQ(State_->Patches[0]->Version, patchId);
    EXPECT_EQ(State_->Patches[0]->Comment, "Updated reason");
    EXPECT_EQ(State_->Patches[0]->Timestamp, timestamp);
}

TEST_F(TDynamicSpecPatchTest, CancelNewerPatchRestoresOlderPatch)
{
    SetBase("{x=1;}");
    Set("/x", "2");
    auto oldVersion = State_->GetVersion();
    Set("/x", "3");
    auto version = State_->GetVersion();
    ASSERT_EQ(State_->Patches.size(), 2u);
    CancelPatch(version);
    EXPECT_EQ(GetInt("/x"), 2);
    CancelPatch(oldVersion);
    EXPECT_EQ(GetInt("/x"), 1);
}

TEST_F(TDynamicSpecPatchTest, OnePatchChangesSeveralPathsAndBlamesOnlyChangedFields)
{
    SetBase("{x={a=1;b=2;c=3;};y=4;}");
    Patch({
        .Operation = EDynamicSpecPatchAction::Set,
        .Changes = {MakeChange("/x/a", 10), MakeChange("/x/c", 30), MakeChange("/y", 40)},
        .Comment = "Change three fields",
    });
    auto patchId = State_->GetVersion();
    ASSERT_EQ(State_->Patches.size(), 1u);
    ASSERT_EQ(State_->Patches[0]->Changes.size(), 3u);
    EXPECT_EQ(GetInt("/x/a"), 10);
    EXPECT_EQ(GetInt("/x/b"), 2);
    EXPECT_EQ(GetInt("/x/c"), 30);
    EXPECT_EQ(GetInt("/y"), 40);

    auto blame = BuildDynamicSpecBlameSnapshot(State_)->AsMap();
    EXPECT_FALSE(blame->FindChild("/x"));
    EXPECT_FALSE(blame->FindChild("/x/b"));
    for (const auto& path : {"/x/a", "/x/c", "/y"}) {
        EXPECT_EQ(blame->GetChildOrThrow(path)->AsMap()->GetChildValueOrThrow<TVersion>("patch_id"), patchId);
    }

    State_ = ConvertTo<TVersionedDynamicPipelineSpecPtr>(ConvertToYsonString(State_));
    EXPECT_EQ(GetInt("/x/a"), 10);
    EXPECT_EQ(BuildDynamicSpecPatchesSnapshot(State_, patchId)->AsMap()->GetChildOrThrow("patches")->AsList()->GetChildOrThrow(0)->AsMap()->GetChildOrThrow("changes")->AsList()->GetChildCount(), 3);
    Set("/x/a", "11");
    CancelPatch(State_->GetVersion());
    EXPECT_EQ(GetInt("/x/a"), 10);
    CancelPatch(patchId);
    EXPECT_EQ(GetInt("/x/a"), 1);
    EXPECT_EQ(GetInt("/x/c"), 3);
    EXPECT_EQ(GetInt("/y"), 4);
}

TEST_F(TDynamicSpecPatchTest, MultiPathPatchRejectsOverlapsAtomically)
{
    SetBase("{x={a=1;b=2;};}");
    auto original = ConvertToNode(State_);
    std::vector<std::vector<TDynamicSpecPatchChange>> conflictingChanges = {
        {MakeChange("/x", 10), MakeChange("/x/a", 20)},
        {MakeChange("/x", 10), MakeChange("/x-foo", 30), MakeChange("/x/a", 20)},
        {MakeChange("/x/a", 20), MakeChange("/x-foo", 30), MakeChange("/x", 10)},
        {MakeChange("/x/a", 10), MakeChange("/x/a", 20)},
    };
    for (const auto& changes : conflictingChanges) {
        EXPECT_THROW(Patch({
                .Operation = EDynamicSpecPatchAction::Set,
                .Changes = changes,
                .Comment = "Conflicting changes",
                     }),
            TErrorException);
        EXPECT_TRUE(AreNodesEqual(original, ConvertToNode(State_)));
    }
    Patch({
        .Operation = EDynamicSpecPatchAction::Set,
        .Changes = {MakeChange("/x-foo", 30), MakeChange("/x/a", 20), MakeChange("/x/b", 40)},
        .Comment = "Independent changes",
    });
    EXPECT_EQ(GetInt("/x-foo"), 30);
    EXPECT_EQ(GetInt("/x/a"), 20);
    EXPECT_EQ(GetInt("/x/b"), 40);
}

TEST_F(TDynamicSpecPatchTest, OnePatchCanContainMoreThanMaximumPatchCountChanges)
{
    SetBase("{}");
    std::vector<TDynamicSpecPatchChange> changes;
    for (int index = 0; index < 101; ++index) {
        changes.push_back(MakeChange("/x" + std::to_string(index), index));
    }
    Patch({
        .Operation = EDynamicSpecPatchAction::Set,
        .Changes = std::move(changes),
        .Comment = "Change many fields",
    });
    ASSERT_EQ(State_->Patches.size(), 1u);
    ASSERT_EQ(State_->Patches[0]->Changes.size(), 101u);
    EXPECT_EQ(GetInt("/x100"), 100);
}

TEST_F(TDynamicSpecPatchTest, EmptyMapsRetainIntentUntilReset)
{
    SetBase("{x={a=1;};other=2;}");
    Set("/x", "{}");
    EXPECT_TRUE(GetNode("/x")->AsMap()->GetChildren().empty());
    SetBase("{x={new=3;};other=4;}");
    EXPECT_FALSE(GetNode("/x/new"));
    Patch({.Operation = EDynamicSpecPatchAction::Reset});
    EXPECT_EQ(GetInt("/x/new"), 3);
    EXPECT_EQ(GetInt("/other"), 4);
}

TEST_F(TDynamicSpecPatchTest, EscapedKeysAndInvalidPaths)
{
    SetBase("{}");
    Set("/x\\/y", "42");
    EXPECT_EQ(GetInt("/x\\/y"), 42);
    auto version = State_->GetVersion();
    for (const auto& path : {"x", "/", "/x//y", "/@x", "/x/*", "/x\\"}) {
        EXPECT_THROW(Set(path, "1"), TErrorException) << path;
        EXPECT_EQ(State_->GetVersion(), version);
    }
}

TEST_F(TDynamicSpecPatchTest, ListElementEditsPreserveListAcrossRelease)
{
    SetBase("{x=[1;2;3;];other=4;}");
    Set("/x/1", "20");
    EXPECT_EQ(ConvertTo<std::vector<int>>(GetNode("/x")), (std::vector<int>{1, 20, 3}));
    auto patchVersion = State_->Patches[0]->Version;
    SetBase("{x=[100;200;300;];other=5;}");
    EXPECT_EQ(ConvertTo<std::vector<int>>(GetNode("/x")), (std::vector<int>{1, 20, 3}));
    EXPECT_EQ(GetInt("/other"), 5);
    CancelPatch(patchVersion);
    EXPECT_EQ(ConvertTo<std::vector<int>>(GetNode("/x")), (std::vector<int>{100, 200, 300}));
}

TEST_F(TDynamicSpecPatchTest, ListInteriorCreatesMapAncestorsAndCancelsAtomically)
{
    SetBase("{x=[{};];}");
    Set("/x/0/a/b", "10");
    EXPECT_EQ(GetInt("/x/0/a/b"), 10);
    auto original = ConvertToNode(State_);
    EXPECT_THROW(Set("/x/2/a", "10"), TErrorException);
    EXPECT_TRUE(AreNodesEqual(original, ConvertToNode(State_)));
    CancelPatch(State_->GetVersion());
    EXPECT_FALSE(GetNode("/x/0/a"));
}

TEST_F(TDynamicSpecPatchTest, NestingLimitsRejectUnsafeMutationsAndRetainRecoverability)
{
    SetBase("{}");
    auto original = ConvertToNode(State_);
    EXPECT_THROW(Set(MakeDeepPath(10000), "1"), TErrorException);
    EXPECT_TRUE(AreNodesEqual(original, ConvertToNode(State_)));
    EXPECT_THROW(Set(MakeDeepPath(DefaultYsonParserNestingLevelLimit), "1"), TErrorException);
    EXPECT_TRUE(AreNodesEqual(original, ConvertToNode(State_)));
    EXPECT_THROW(Patch({
            .Operation = EDynamicSpecPatchAction::Set,
            .Path = MakeDeepPath(DefaultYsonParserNestingLevelLimit / 2),
            .Value = MakeNestedMap(DefaultYsonParserNestingLevelLimit / 2),
            .Comment = "Check persisted patch nesting",
                 }),
        TErrorException);
    EXPECT_TRUE(AreNodesEqual(original, ConvertToNode(State_)));

    auto path = MakeDeepPath(DefaultYsonParserNestingLevelLimit / 2 - 1);
    Set(path, "1");
    auto recovered = NormalizeDynamicSpecState(State_);
    EXPECT_TRUE(AreNodesEqual(ConvertToNode(State_), ConvertToNode(recovered)));
    EXPECT_EQ(GetInt(path), 1);
    Set("/" + std::string(10000, 'x'), "2");
    EXPECT_EQ(GetInt("/" + std::string(10000, 'x')), 2);
}

TEST_F(TDynamicSpecPatchTest, DeepListEditsFailAtomicallyBeforeCloning)
{
    SetBase("{x=[{};];}");
    auto original = ConvertToNode(State_);
    EXPECT_THROW(Patch({
            .Operation = EDynamicSpecPatchAction::Set,
            .Path = "/x/0" + MakeDeepPath(160),
            .Value = MakeNestedMap(100),
            .Comment = "Check nested list patch limits",
                 }),
        TErrorException);
    EXPECT_TRUE(AreNodesEqual(original, ConvertToNode(State_)));
}

TEST_F(TDynamicSpecPatchTest, ReleaseBaseNestingIncludesPersistedStateWrapper)
{
    SetBase("{}");
    auto original = ConvertToNode(State_);
    auto base = MakeNestedMap(DefaultYsonParserNestingLevelLimit - 1);
    EXPECT_NO_THROW(ValidateYson(ConvertToYsonString(base), DefaultYsonParserNestingLevelLimit));
    EXPECT_THROW(UpdateDynamicSpecBase(State_, base, Provider_), TErrorException);
    EXPECT_TRUE(AreNodesEqual(original, ConvertToNode(State_)));
}

TEST_F(TDynamicSpecPatchTest, PatchCountLimitIsCheckedAfterUpdateAndFailsAtomically)
{
    SetBase("{}");
    for (int index = 0; index < 100; ++index) {
        Set("/x" + std::to_string(index), "10");
    }
    auto lastPatch = State_->GetVersion();
    auto firstPatch = State_->Patches[0]->Version;
    Patch({
        .Operation = EDynamicSpecPatchAction::Set,
        .Path = "/x0",
        .Value = ConvertToNode(11),
        .PatchVersion = firstPatch,
    });
    EXPECT_EQ(State_->Patches.size(), 100u);
    auto original = ConvertToNode(State_);
    EXPECT_THROW_WITH_SUBSTRING(Set("/overflow", "20"), "save their changes as the base dynamic spec or cancel unnecessary patches");
    EXPECT_TRUE(AreNodesEqual(original, ConvertToNode(State_)));
    CancelPatch(lastPatch);
    Set("/overflow", "20");
    EXPECT_EQ(State_->Patches.size(), 100u);
}

TEST_F(TDynamicSpecPatchTest, PersistedDynamicSpecValueLimitIsCheckedAtomically)
{
    SetBase("{x=1;}");
    auto original = ConvertToNode(State_);
    EXPECT_THROW(Patch({
            .Operation = EDynamicSpecPatchAction::Set,
            .Path = "/x",
            .Value = ConvertToNode(2),
            .Comment = std::string(NTableClient::MaxAnyValueLength, 'x'),
                 }),
        TErrorException);
    EXPECT_TRUE(AreNodesEqual(original, ConvertToNode(State_)));
}

TEST_F(TDynamicSpecPatchTest, CasIsCheckedForEqualValueAndReset)
{
    SetBase("{x=1;}");
    auto original = ConvertToNode(State_);
    EXPECT_THROW(Patch({
            .Operation = EDynamicSpecPatchAction::Set,
            .Path = "/x",
            .Value = ConvertToNode(1),
            .Comment = "Check stale patch CAS",
            .ExpectedVersion = TVersion(0),
                 }),
        TErrorException);
    EXPECT_THROW(Patch({
            .Operation = EDynamicSpecPatchAction::Reset,
            .ExpectedVersion = TVersion(0),
                 }),
        TErrorException);
    EXPECT_TRUE(AreNodesEqual(original, ConvertToNode(State_)));
}

TEST_F(TDynamicSpecPatchTest, ValueAndPatchesSurviveRecovery)
{
    SetBase("{x=1;}");
    Set("/x", "2", "pin");
    auto patchVersion = State_->GetVersion();
    auto persisted = ConvertToNode(State_)->AsMap();
    EXPECT_FALSE(persisted->FindChild("base_spec"));
    EXPECT_FALSE(persisted->FindChild("runtime_target_state"));
    EXPECT_EQ(ConvertTo<int>(GetNodeByYPath(persisted, "/value/x")), 1);
    ASSERT_EQ(persisted->GetChildOrThrow("patches")->AsList()->GetChildCount(), 1);

    State_ = ConvertTo<TVersionedDynamicPipelineSpecPtr>(ConvertToYsonString(State_));
    EXPECT_EQ(GetInt("/x"), 2);
    SetBase("{x=3;}");
    EXPECT_EQ(GetInt("/x"), 2);
    EXPECT_EQ(State_->Patches[0]->Version, patchVersion);
    CancelPatch(patchVersion);
    EXPECT_EQ(GetInt("/x"), 3);
}

TEST_F(TDynamicSpecPatchTest, SetRequiresNonemptyCommentsAtomically)
{
    SetBase("{x=1;}");
    Set("/x", "2", "Initial patch");
    auto original = ConvertToNode(State_);
    for (const auto& comment : std::vector<std::optional<std::string>>{std::nullopt, ""}) {
        EXPECT_THROW_WITH_SUBSTRING(Patch({
                .Operation = EDynamicSpecPatchAction::Set,
                .Path = "/missing",
                .Value = ConvertToNode(2),
                .Comment = comment,
                .ExpectedVersion = State_->GetVersion(),
                                    }),
            "nonempty comment");
        EXPECT_TRUE(AreNodesEqual(original, ConvertToNode(State_)));
    }
    EXPECT_THROW_WITH_SUBSTRING(Patch({
            .Operation = EDynamicSpecPatchAction::Set,
            .Path = "/x",
            .Value = ConvertToNode(2),
            .Comment = " ",
            .ExpectedVersion = TVersion(0),
                                }),
        "Spec version mismatch");
    EXPECT_TRUE(AreNodesEqual(original, ConvertToNode(State_)));

    auto previousVersion = State_->GetVersion();
    Set("/x", "2", "  Комментарий 😀  ");
    EXPECT_GT(State_->GetVersion(), previousVersion);
    EXPECT_EQ(State_->Patches.back()->Comment, "  Комментарий 😀  ");
    for (const auto& comment : {"\u200b", "\x1c\x1d\x1e\x1f", "\u0085 Причина 😀 \u200b"}) {
        Set("/x", "2", comment);
        EXPECT_EQ(State_->Patches.back()->Comment, comment);
    }
    EXPECT_EQ(State_->Patches.size(), 5u);
}

TEST_F(TDynamicSpecPatchTest, BaseUpdatesAndPatchCancellationDoNotRequireComments)
{
    SetBase("{x=1;}");
    Set("/x", "2");
    auto patchVersion = State_->GetVersion();
    State_->Patches[0]->Comment.reset();
    State_ = NormalizeDynamicSpecState(State_);
    EXPECT_FALSE(State_->Patches[0]->Comment);
    SetBase("{x=3;}");
    EXPECT_FALSE(State_->Patches[0]->Comment);
    EXPECT_EQ(GetInt("/x"), 2);
    CancelPatch(patchVersion);
    EXPECT_EQ(GetInt("/x"), 3);
    Set("/x", "4");
    CancelPatch(State_->GetVersion());
    EXPECT_EQ(GetInt("/x"), 3);
    Set("/x", "5");
    Patch({.Operation = EDynamicSpecPatchAction::Reset});
    EXPECT_TRUE(State_->Patches.empty());
    EXPECT_EQ(GetInt("/x"), 3);
}

TEST_F(TDynamicSpecPatchTest, TargetStateIsStoredInValue)
{
    SetBase("{x=1;target_state=stopped;}");
    Set("/x", "2");
    auto patchVersion = State_->GetVersion();
    EXPECT_EQ(State_->GetValue()->TargetState, EPipelineState::Stopped);
    State_ = UpdateDynamicSpecTargetState(State_, EPipelineState::Completed, Provider_);
    EXPECT_EQ(State_->GetValue()->TargetState, EPipelineState::Completed);
    EXPECT_EQ(State_->GetBaseSpec()->TargetState, EPipelineState::Completed);
    EXPECT_EQ(GetInt("/x"), 2);
    EXPECT_EQ(State_->Patches[0]->Version, patchVersion);
    SetBase("{x=3;target_state=stopped;}");
    EXPECT_EQ(State_->GetValue()->TargetState, EPipelineState::Stopped);
    CancelPatch(patchVersion);
    EXPECT_EQ(GetInt("/x"), 3);
    EXPECT_EQ(State_->GetValue()->TargetState, EPipelineState::Stopped);
    EXPECT_THROW(Set("/target_state", "paused"), TErrorException);
    EXPECT_EQ(State_->GetValue()->TargetState, EPipelineState::Stopped);
}

TEST_F(TDynamicSpecPatchTest, FlatPatchOrderChangesOnlyWithValueUpdates)
{
    SetBase("{x={a=1;b=2;};}");
    Set("/x/a", "10");
    auto childPatch = State_->GetVersion();
    Set("/x", "{a=30;}");
    auto parentPatch = State_->GetVersion();
    auto timestamp = State_->Patches[0]->Timestamp;
    Patch({
        .Operation = EDynamicSpecPatchAction::UpdateComment,
        .PatchVersion = childPatch,
        .Comment = "Still hidden",
    });
    EXPECT_EQ(GetInt("/x/a"), 30);
    EXPECT_EQ(State_->Patches[0]->Version, childPatch);
    EXPECT_EQ(State_->Patches[0]->Timestamp, timestamp);
    auto blame = BuildDynamicSpecBlameSnapshot(State_)->AsMap();
    EXPECT_FALSE(blame->FindChild("/x/a"));
    EXPECT_EQ(blame->GetChildOrThrow("/x")->AsMap()->GetChildValueOrThrow<TVersion>("patch_id"), parentPatch);

    Patch({
        .Operation = EDynamicSpecPatchAction::Set,
        .Path = "/x/a",
        .Value = ConvertToNode(45),
        .PatchVersion = childPatch,
    });
    ASSERT_EQ(State_->Patches.size(), 2u);
    EXPECT_EQ(State_->Patches[0]->Version, parentPatch);
    EXPECT_EQ(State_->Patches[1]->Version, childPatch);
    EXPECT_EQ(GetInt("/x/a"), 45);
    blame = BuildDynamicSpecBlameSnapshot(State_)->AsMap();
    EXPECT_EQ(blame->GetChildOrThrow("/x/a")->AsMap()->GetChildValueOrThrow<TVersion>("patch_id"), childPatch);
    EXPECT_EQ(blame->GetChildOrThrow("/x")->AsMap()->GetChildValueOrThrow<TVersion>("patch_id"), parentPatch);
    CancelPatch(childPatch);
    EXPECT_EQ(GetInt("/x/a"), 30);
    CancelPatch(parentPatch);
    EXPECT_EQ(GetInt("/x/a"), 1);
}

TEST_F(TDynamicSpecPatchTest, FlatChildPatchInheritsNewBaseSiblings)
{
    SetBase("{a={x=1;y=2;};}");
    Set("/a/x", "10");
    auto patchId = State_->GetVersion();
    State_ = NormalizeDynamicSpecState(ConvertTo<TVersionedDynamicPipelineSpecPtr>(ConvertToYsonString(State_)));
    SetBase("{a={x=3;y=20;z=30;};}");
    EXPECT_EQ(GetInt("/a/x"), 10);
    EXPECT_EQ(GetInt("/a/y"), 20);
    EXPECT_EQ(GetInt("/a/z"), 30);
    auto patch = BuildDynamicSpecPatchesSnapshot(State_)->AsMap()->GetChildOrThrow("patches")->AsList()->GetChildOrThrow(0)->AsMap();
    auto changes = patch->GetChildOrThrow("changes")->AsList();
    ASSERT_EQ(changes->GetChildCount(), 1);
    EXPECT_EQ(changes->GetChildOrThrow(0)->AsMap()->GetChildValueOrThrow<std::string>("path"), "/a/x");
    EXPECT_FALSE(patch->FindChild("operation"));
    EXPECT_FALSE(patch->FindChild("children"));
    CancelPatch(patchId);
    EXPECT_EQ(GetInt("/a/x"), 3);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
