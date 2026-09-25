#include <yt/yql/providers/yt/provider/yql_yt_provider_impl.h>
#include <yt/yql/providers/yt/provider/yql_yt_table.h>

#include <yql/essentials/core/yql_expr_type_annotation.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

namespace NYql {
namespace {

void CheckSymlinkIntents(bool create, bool genericWrite, bool readFirst, bool conditional = false, TStringBuf targetHint = {}) {
    const TString testCase = TStringBuilder() << "create=" << create << ", genericWrite=" << genericWrite
        << ", readFirst=" << readFirst << ", conditional=" << conditional << ", targetHint=" << targetHint;
    TExprContext ctx;
    const TPositionHandle pos;
    auto types = MakeIntrusive<TTypeAnnotationContext>();
    auto state = std::make_shared<TYtState>(types.Get());
    auto transformer = CreateYtIntentDeterminationTransformer(state);
    TYtTableInfo linkInfo;
    linkInfo.Name = "Link";
    linkInfo.Cluster = "plato";
    TYtTableInfo targetInfo;
    targetInfo.Name = "Target";
    targetInfo.Cluster = "plato";
    auto link = linkInfo.ToExprNode(ctx, pos).Ptr();
    auto target = targetInfo.ToExprNode(ctx, pos).Ptr();
    auto symlinkTarget = target;
    if (targetHint) {
        TExprNode::TListType hint{ctx.NewAtom(pos, targetHint)};
        if (targetHint == "view") {
            hint.push_back(ctx.NewAtom(pos, "raw"));
        }
        targetInfo.Settings = NNodes::TExprBase(ctx.NewList(pos, {ctx.NewList(pos, std::move(hint))}));
        symlinkTarget = targetInfo.ToExprNode(ctx, pos).Ptr();
    }
    auto world = ctx.NewWorld(pos);
    auto sink = ctx.NewCallable(pos, "DataSink", {ctx.NewAtom(pos, "yt"), ctx.NewAtom(pos, "plato")});
    auto source = ctx.NewCallable(pos, "DataSource", {ctx.NewAtom(pos, "yt"), ctx.NewAtom(pos, "plato")});
    TString mode = create ? "create_symlink" : "drop_symlink";
    if (conditional) {
        mode += create ? "_if_not_exists" : "_if_exists";
    }
    auto settings = ctx.NewList(pos, {ctx.NewList(pos, {
        ctx.NewAtom(pos, "mode"), ctx.NewAtom(pos, mode)})});
    TExprNode::TPtr output;
    const auto read = [&](const TExprNode::TPtr& table) {
        auto node = ctx.NewCallable(pos, "YtReadTableScheme!", {world, source, table, ctx.NewCallable(pos, "Void", {})});
        UNIT_ASSERT_C(transformer->Transform(node, output, ctx) == IGraphTransformer::TStatus::Ok,
            testCase << ": " << ctx.IssueManager.GetIssues().ToString());
    };
    if (readFirst) {
        read(create ? target : link);
    }
    TExprNode::TPtr operation;
    if (genericWrite) {
        operation = ctx.NewCallable(pos, "Write!", {
            world, sink, link, create ? symlinkTarget : ctx.NewCallable(pos, "Void", {}), settings});
    } else if (create) {
        operation = ctx.NewCallable(pos, "YtCreateSymlink!", {world, sink, link, symlinkTarget, settings});
    } else {
        operation = ctx.NewCallable(pos, "YtDropSymlink!", {world, sink, link, settings});
    }
    UNIT_ASSERT_C(transformer->Transform(operation, output, ctx) == IGraphTransformer::TStatus::Ok,
        testCase << ": " << ctx.IssueManager.GetIssues().ToString());
    const auto& linkDesc = state->TablesData->GetTable("plato", "Link", {});
    TYtTableIntents expectedLinkIntents = create ? TYtTableIntent::SymlinkCreate : TYtTableIntent::SymlinkDrop;
    if (!create && readFirst) {
        expectedLinkIntents |= TYtTableIntent::Read;
    }
    UNIT_ASSERT_VALUES_EQUAL_C(linkDesc.Intents, expectedLinkIntents, testCase);
    if (create) {
        const auto& targetDesc = state->TablesData->GetTable("plato", "Target", {});
        UNIT_ASSERT_VALUES_EQUAL_C(targetDesc.Intents, TYtTableIntent::Read | TYtTableIntent::Referenced, testCase);
        UNIT_ASSERT_C(targetDesc.Views.empty(), testCase);
    }
    if (!readFirst) {
        read(create ? target : link);
    }
    const auto& combined = state->TablesData->GetTable("plato", create ? "Target" : "Link", {});
    UNIT_ASSERT_VALUES_EQUAL_C(combined.Intents,
        TYtTableIntent::Read | (create ? TYtTableIntent::Referenced : TYtTableIntent::SymlinkDrop), testCase);
}

} // namespace

Y_UNIT_TEST_SUITE(SymlinkIntents) {
    Y_UNIT_TEST(Create) {
        for (bool genericWrite : {false, true}) {
            for (bool readFirst : {false, true}) {
                CheckSymlinkIntents(/*create=*/true, genericWrite, readFirst);
            }
        }
    }

    Y_UNIT_TEST(Drop) {
        for (bool genericWrite : {false, true}) {
            for (bool readFirst : {false, true}) {
                CheckSymlinkIntents(/*create=*/false, genericWrite, readFirst);
            }
        }
    }

    Y_UNIT_TEST(CreateIfNotExists) {
        for (bool genericWrite : {false, true}) {
            for (bool readFirst : {false, true}) {
                CheckSymlinkIntents(/*create=*/true, genericWrite, readFirst, /*conditional=*/true);
            }
        }
    }

    Y_UNIT_TEST(DropIfExists) {
        for (bool genericWrite : {false, true}) {
            for (bool readFirst : {false, true}) {
                CheckSymlinkIntents(/*create=*/false, genericWrite, readFirst, /*conditional=*/true);
            }
        }
    }

    Y_UNIT_TEST(TargetHintsDoNotChangeReferencedIntents) {
        for (TStringBuf hint : {"view", "xlock"}) {
            for (bool genericWrite : {false, true}) {
                for (bool readFirst : {false, true}) {
                    CheckSymlinkIntents(/*create=*/true, genericWrite, readFirst, /*conditional=*/false, hint);
                }
            }
        }
    }
}

} // namespace NYql
