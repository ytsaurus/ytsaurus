#include "common.h"

#include <yt/yt/orm/server/access_control/access_control_manager.h>
#include <yt/yt/orm/server/access_control/subject_cluster.h>

#include <yt/yt/orm/server/master/yt_connector.h>

#include <yt/yt/orm/server/objects/transaction_manager.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NOrm::NExample::NServer::NTests {
namespace {

using namespace NYT::NYTree;
using namespace NYT::NQueryClient;
using namespace NYT::NOrm::NServer::NAccessControl;

////////////////////////////////////////////////////////////////////////////////

TIntrusivePtr<NOrm::NServer::NObjects::TTransaction> StartTransaction()
{
    return WaitFor(GetBootstrap()->GetTransactionManager()
        ->StartReadWriteTransaction())
        .ValueOrThrow();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TExecutionPoolTagTest, JustWorks)
{
    auto transaction = StartTransaction();

    TString poolUser = "test_pool_user";
    TString simpleUser = "test_user";

    transaction->CreateObjects({
        {
            TObjectTypeValues::User,
            BuildYsonNodeFluently()
                .BeginMap()
                    .Item("meta")
                        .BeginMap()
                            .Item("id").Value(poolUser)
                        .EndMap()
                    .Item("spec")
                        .BeginMap()
                            .Item("execution_pool").Value("test_pool")
                        .EndMap()
                .EndMap()
                ->AsMap()
        },
        {
            TObjectTypeValues::User,
            BuildYsonNodeFluently()
                .BeginMap()
                    .Item("meta")
                        .BeginMap()
                            .Item("id").Value(simpleUser)
                        .EndMap()
                .EndMap()
                ->AsMap()
        }});

    WaitFor(transaction->Commit())
        .ValueOrThrow();

    NRpc::TAuthenticationIdentity userPoolIdentity(poolUser, "test_pool_tag");
    NRpc::TAuthenticationIdentity userSimpleIdentity(simpleUser, "test_tag");

    WaitForPredicate([&] {
        return GetBootstrap()
            ->GetAccessControlManager()
            ->GetClusterSubjectSnapshot()
            ->FindSubject(poolUser) != nullptr;
        });

    TAuthenticatedUserGuard userPoolGuard(GetBootstrap()
        ->GetAccessControlManager(),
        userPoolIdentity);
    transaction = StartTransaction();
    ASSERT_EQ(transaction->GetExecutionPoolTag(), TString("test_pool"));

    TAuthenticatedUserGuard userSimpleGuard(GetBootstrap()
        ->GetAccessControlManager(),
        userSimpleIdentity);
    transaction = StartTransaction();
    ASSERT_EQ(transaction->GetExecutionPoolTag(), std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NClient::NNative::NTests
