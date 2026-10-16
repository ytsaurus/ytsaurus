#include <yt/yql/providers/yt/provider/yql_yt_forwarding_gateway.h>
#include <yt/yql/providers/yt/provider/yql_yt_provider_impl.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_services.h>

#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {
namespace {

class TMetadataGateway: public TYtForwardingGatewayBase {
public:
    explicit TMetadataGateway(IYtGateway::TPtr&& slave)
        : TYtForwardingGatewayBase(std::move(slave))
    {
    }

    NThreading::TFuture<TTableInfoResult> GetTableInfo(TGetTableInfoOptions&& options) override {
        UNIT_ASSERT_VALUES_EQUAL(options.Tables().size(), 1);
        Requests.push_back(options.Tables().front());
        TTableInfoResult result;
        result.SetSuccess();
        result.Data.push_back(Response);
        return NThreading::MakeFuture(std::move(result));
    }

    TVector<TTableReq> Requests;
    TTableInfoResult::TTableData Response;
};

struct TMetadataFixture {
    TMetadataFixture() {
        auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        auto services = NFile::TYtFileServices::Make(registry.Get(), {});
        Gateway = MakeIntrusive<TMetadataGateway>(CreateYtFileGateway(services));
        Types->RandomProvider = CreateDeterministicRandomProvider(/*seed=*/1);
        Types->SqlFlags = NSQLTranslation::TSqlFlags{};
        State->Gateway = Gateway;
    }

    TYtTableDescription& Table() {
        return State->TablesData->GetOrAddTable("plato", "Target", /*epoch=*/0);
    }

    void Load() {
        auto input = Ctx.NewWorld(TPositionHandle());
        TExprNode::TPtr output;
        auto status = Transformer->Transform(input, output, Ctx);
        if (status == IGraphTransformer::TStatus::Async) {
            Transformer->GetAsyncFuture(*input).GetValueSync();
            status = Transformer->ApplyAsyncChanges(input, output, Ctx);
        }
        UNIT_ASSERT_C(status == IGraphTransformer::TStatus::Ok, Ctx.IssueManager.GetIssues().ToString());
    }

    void CheckRequest(bool lockOnly, TYtTableIntents intents) {
        UNIT_ASSERT_VALUES_EQUAL(Gateway->Requests.size(), 1);
        const auto& request = Gateway->Requests.front();
        UNIT_ASSERT_VALUES_EQUAL(request.Cluster(), "plato");
        UNIT_ASSERT_VALUES_EQUAL(request.Table(), "Target");
        UNIT_ASSERT_VALUES_EQUAL(request.LockOnly(), lockOnly);
        UNIT_ASSERT_VALUES_EQUAL(request.Intents(), intents);
    }

    TExprContext Ctx;
    TIntrusivePtr<TTypeAnnotationContext> Types = MakeIntrusive<TTypeAnnotationContext>();
    TYtState::TPtr State = std::make_shared<TYtState>(Types.Get());
    THolder<IGraphTransformer> Transformer = CreateYtLoadTableMetadataTransformer(State);
    TIntrusivePtr<TMetadataGateway> Gateway;
};

} // namespace

Y_UNIT_TEST_SUITE(SymlinkMetadata) {
    Y_UNIT_TEST(MissingReferenceTargetIsNotRequestedAgain) {
        TMetadataFixture fixture;
        auto& table = fixture.Table();
        table.Intents = TYtTableIntent::Read | TYtTableIntent::Referenced;
        fixture.Gateway->Response.Meta = MakeIntrusive<TYtTableMetaInfo>();
        fixture.Gateway->Response.Meta->DoesExist = false;
        fixture.Gateway->Response.ReferenceLock = false;

        fixture.Load();
        fixture.CheckRequest(/*lockOnly=*/false, table.Intents);
        UNIT_ASSERT(table.Meta);
        UNIT_ASSERT(!table.Meta->DoesExist);
        UNIT_ASSERT(!table.HasReferenceLock);
        fixture.Load();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Gateway->Requests.size(), 1);
    }

    Y_UNIT_TEST(ReferenceAddedAfterReadRequestsOnlyLock) {
        TMetadataFixture fixture;
        auto& table = fixture.Table();
        table.Intents = TYtTableIntent::Read;
        table.Meta = MakeIntrusive<TYtTableMetaInfo>();
        table.Meta->DoesExist = true;
        const auto meta = table.Meta;
        fixture.Load();
        UNIT_ASSERT(fixture.Gateway->Requests.empty());

        table.Intents |= TYtTableIntent::Referenced;
        fixture.Gateway->Response.ReferenceLock = true;
        fixture.Load();
        fixture.CheckRequest(/*lockOnly=*/true, table.Intents);
        UNIT_ASSERT(table.HasReferenceLock);
        UNIT_ASSERT(table.Meta == meta);
        fixture.Load();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Gateway->Requests.size(), 1);
    }

    Y_UNIT_TEST(MissingNodeStillRequiresSymlinkLock) {
        for (auto intent : {TYtTableIntent::SymlinkCreate, TYtTableIntent::SymlinkDrop}) {
            TMetadataFixture fixture;
            auto& table = fixture.Table();
            table.Intents = intent;
            table.Meta = MakeIntrusive<TYtTableMetaInfo>();
            table.Meta->DoesExist = false;
            const auto meta = table.Meta;
            fixture.Gateway->Response.SymlinkLock = true;

            fixture.Load();
            fixture.CheckRequest(/*lockOnly=*/true, table.Intents);
            UNIT_ASSERT(table.HasSymlinkLock);
            UNIT_ASSERT(table.Meta == meta);
            fixture.Load();
            UNIT_ASSERT_VALUES_EQUAL(fixture.Gateway->Requests.size(), 1);
        }
    }

    Y_UNIT_TEST(SymlinkAndReferenceLocksAccumulateIndependently) {
        for (bool referenceLockedFirst : {false, true}) {
            TMetadataFixture fixture;
            auto& table = fixture.Table();
            table.Intents = TYtTableIntent::Read | TYtTableIntent::Referenced | TYtTableIntent::SymlinkCreate;
            table.Meta = MakeIntrusive<TYtTableMetaInfo>();
            table.Meta->DoesExist = true;
            const auto meta = table.Meta;
            table.HasReferenceLock = referenceLockedFirst;
            table.HasSymlinkLock = !referenceLockedFirst;
            fixture.Gateway->Response.ReferenceLock = !referenceLockedFirst;
            fixture.Gateway->Response.SymlinkLock = referenceLockedFirst;

            fixture.Load();
            fixture.CheckRequest(/*lockOnly=*/true, table.Intents);
            UNIT_ASSERT_C(table.HasReferenceLock, "referenceLockedFirst=" << referenceLockedFirst);
            UNIT_ASSERT_C(table.HasSymlinkLock, "referenceLockedFirst=" << referenceLockedFirst);
            UNIT_ASSERT(table.Meta == meta);
            fixture.Load();
            UNIT_ASSERT_VALUES_EQUAL(fixture.Gateway->Requests.size(), 1);
        }
    }
}

} // namespace NYql
