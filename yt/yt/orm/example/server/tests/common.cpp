#include "common.h"

#include <yt/yt/orm/example/client/objects/autogen/init.h>

#include <yt/yt/orm/example/server/library/autogen/program.h>

#include <yt/yt/orm/server/objects/object.h>
#include <yt/yt/orm/server/objects/transaction.h>
#include <yt/yt/orm/server/objects/transaction_manager.h>
#include <yt/yt/orm/server/objects/transaction_wrapper.h>
#include <yt/yt/orm/server/objects/watch_query_executor.h>

#include <yt/yt/orm/server/access_control/access_control_manager.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/program/helpers.h>

#include <util/generic/singleton.h>

#include <util/system/env.h>

namespace NYT::NOrm::NExample::NServer::NTests {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TBootstrapHolder
{
public:
    TBootstrapHolder()
        : Bootstrap_(Initialize())
    { }

    IBootstrap* Get() const
    {
        return Bootstrap_;
    }

private:
    IBootstrap* const Bootstrap_;

    IBootstrap* Initialize()
    {
        NClient::NObjects::EnsureGlobalRegistriesAreInitialized();

        auto configPath = TString(GetEnv("EXAMPLE_MASTER_CONFIG_PATH"));
        YT_VERIFY(configPath);
        TIFStream configStream(configPath);
        auto configNode = ConvertToNode(&configStream)->AsMap();
        auto config = New<TMasterConfig>();
        config->Load(configNode);
        ConfigureSingletons(config);

        auto* bootstrap = TMasterProgram::StartBootstrap(config, configNode);

        // Access control state has been loaded => master is connected to YT.
        WaitForPredicate(
            [bootstrap] {
                bootstrap->GetAccessControlManager()->SetAuthenticatedUser(NRpc::TAuthenticationIdentity("root"));
                return true;
            },
            /*options*/ { .IgnoreExceptions = true });

        return bootstrap;
    }
};

////////////////////////////////////////////////////////////////////////////////

IBootstrap* GetBootstrap()
{
    return Singleton<TBootstrapHolder>()->Get();
}

////////////////////////////////////////////////////////////////////////////////

TObjectKey CreateObject(TObjectTypeValue type, NYTree::IMapNodePtr attributes)
{
    auto transaction = WaitFor(GetBootstrap()->GetTransactionManager()
        ->StartReadWriteTransaction())
        .ValueOrThrow();
    auto* object = transaction->CreateObjects({
            {
                .Type = type,
                .Attributes = std::move(attributes)
            }
        },
        /*transactionCallContext*/ {})[0];
    auto objectKey = object->GetKey();
    WaitFor(transaction->Commit())
        .ValueOrThrow();
    EXPECT_TRUE(objectKey);
    return objectKey;
}

TObjectKey CreateUser()
{
    return CreateObject(
        TObjectTypeValues::User,
        GetEphemeralNodeFactory()->CreateMap());
}

TObjectKey CreateEditor()
{
    return CreateObject(
        TObjectTypeValues::Editor,
        GetEphemeralNodeFactory()->CreateMap());
}

TObjectKey CreatePublisher()
{
    return CreateObject(
        TObjectTypeValues::Publisher,
        GetEphemeralNodeFactory()->CreateMap());
}

TObjectKey CreateAuthor()
{
    return CreateObject(
        TObjectTypeValues::Author,
        GetEphemeralNodeFactory()->CreateMap());
}

TObjectKey CreateBook(NYTree::IMapNodePtr spec)
{
    auto publisherKey = CreateObject(
        TObjectTypeValues::Publisher,
        GetEphemeralNodeFactory()->CreateMap());
    return CreateObject(
        TObjectTypeValues::Book,
        BuildYsonNodeFluently()
            .BeginMap()
                .Item("meta")
                    .BeginMap()
                        .Item("isbn").Value("978-1449355739")
                        .Item("parent_key").Value(publisherKey.ToString())
                    .EndMap()
                .Item("spec").Value(spec)
            .EndMap()
            ->AsMap());
}


TObjectKey CreateCat(std::optional<int> friendCatsCount)
{
    return CreateObject(
        TObjectTypeValues::Cat,
        BuildYsonNodeFluently()
            .BeginMap()
                .Item("spec")
                    .BeginMap()
                        .OptionalItem("friend_cats_count", friendCatsCount)
                    .EndMap()
            .EndMap()
            ->AsMap());
}

TTransactionPtr StartTransaction()
{
    return WaitFor(GetBootstrap()->GetTransactionManager()
        ->StartReadWriteTransaction())
        .ValueOrThrow();
}

void CommitTransaction(TTransactionPtr&& transaction)
{
    WaitFor(transaction->Commit())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TWatchQueryResult WatchObject(IBootstrap* bootstrap, TWatchQueryOptions options)
{
    auto transactionManager = bootstrap->GetTransactionManager();
    const auto transaction = TReadOnlyTransactionWrapper(
        transactionManager,
        {NullTimestamp},
        options.TimeLimit,
        true)
        .Unwrap();

    const auto executor = MakeWatchQueryExecutor(transaction);
    return executor->ExecuteWatchQuery(std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NTests
