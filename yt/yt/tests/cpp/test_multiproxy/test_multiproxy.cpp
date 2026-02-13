#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <yt/yt/client/api/distributed_table_client.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/security_client/acl.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/crypto/crypto.h>
#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/client.h>
#include <yt/yt/core/test_framework/framework.h>

#include <util/system/env.h>

////////////////////////////////////////////////////////////////////////////////

using namespace NYT;
using namespace NYT::NApi;
using namespace NYT::NConcurrency;
using namespace NYT::NObjectClient;
using namespace NYT::NSecurityClient;
using namespace NYT::NYson;
using namespace NYT::NYTree;

////////////////////////////////////////////////////////////////////////////////

class TMultiproxyTest
    : public testing::Test
{
protected:
    //
    // We have three clusters:
    //   - RPC proxies of 'read_access' cluster can redirect read requests to 'target' cluster.
    //   - RPC proxies of 'write_access' cluster can redirect write requests to 'target' cluster.
    //   - RPC proxies of 'target' cluster can not redirect any requests.
    static const IClientPtr ReadAccessClient_;
    static const IClientPtr WriteAccessClient_;
    static const IClientPtr TargetClient_;

    static void SetUpTestSuite()
    {
        {
            for (const auto& envKey : {"YT_PROXY_READ_ACCESS", "YT_PROXY_WRITE_ACCESS", "YT_PROXY_TARGET"}) {
                const auto proxyAddress = GetEnv(envKey);
                HttpSetRootToken(proxyAddress, "root_token");
            }
            WaitFor(ReadAccessClient_->SetNode("//sys/rpc_proxies/@config", TYsonString(TStringBuf(R"""(
                {
                    api={
                        multiproxy={
                            presets={
                                default={
                                    enabled_methods=read;
                                };
                            };
                        };
                    };
                    signature_components={
                        generation={
                            cypress_key_writer={};
                            generator={};
                            key_rotator={};
                        };
                        validation={
                            cypress_key_reader={};
                        };
                    };
                }
            )"""))))
                .ThrowOnError();

            WaitFor(WriteAccessClient_->SetNode("//sys/rpc_proxies/@config", TYsonString(TStringBuf(R"""(
                {
                    api={
                        multiproxy={
                            presets={
                                default={
                                    enabled_methods=read_and_write;
                                };
                            };
                        };
                    };
                    signature_components={
                        generation={
                            cypress_key_writer={};
                            generator={};
                            key_rotator={};
                        };
                        validation={
                            cypress_key_reader={};
                        };
                    };
                }
            )"""))))
                .ThrowOnError();

            WaitFor(TargetClient_->SetNode("//sys/rpc_proxies/@config", TYsonString(TStringBuf(R"""(
                {
                    signature_components={
                        validation={
                            cypress_key_reader={};
                        };
                    };
                }
            )"""))))
                .ThrowOnError();

            Sleep(TDuration::Seconds(5));
        }
    }

    TMultiproxyTest()
    { }

    static IClientPtr CreateDirectClient(const std::string& envName, const std::string& token = "root_token")
    {
        TClientOptions options;
        options.Token = token;
        return CreateConnection(envName)->CreateClient(options);
    }

    static IClientPtr CreateRedirectingClient(const std::string& envName, const std::string& targetCluster, const std::string& token = "root_token")
    {
        TClientOptions options;
        options.Token = token;
        options.MultiproxyTargetCluster = targetCluster;
        return CreateConnection(envName)->CreateClient(options);
    }

    static IConnectionPtr CreateConnection(const std::string& envName)
    {
        auto proxy = GetEnv(TString(envName));
        if (proxy.empty()) {
            THROW_ERROR_EXCEPTION(
                "Envrironment %Qv is not specified",
                envName);
        };
        auto connectionConfig = NRpcProxy::TConnectionConfig::CreateFromClusterUrl(proxy);
        return NRpcProxy::CreateConnection(connectionConfig);
    }

    // Return created user token
    static std::string CreateUser(const IClientPtr& client, const std::string& userName)
    {
        auto userToken = std::string(Format("%v", TGuid::Create()));
        TCreateObjectOptions options;
        auto attributes = CreateEphemeralAttributes();
        options.Attributes = BuildAttributeDictionaryFluently()
            .Item("name").Value(userName)
        .EndMap();

        auto targetClusterTokenHash = NCrypto::TSha1Hasher().Append(userToken).GetHexDigestLowerCase();
        WaitFor(client->CreateObject(EObjectType::User, options))
            .ThrowOnError();
        WaitFor(client->SetNode("//sys/tokens/" + targetClusterTokenHash, ConvertToYsonString(userName)))
            .ThrowOnError();
        return userToken;
    }

    static void SetUserBanned(const IClientPtr& client, const std::string& userName, bool banned) {
        const auto path = Format("//sys/users/%v/@banned", userName);
        WaitFor(client->SetNode(path, ConvertToYsonString(banned)))
            .ThrowOnError();
    };

    // Our RPC proxies have authentication enabled. To operate them we want to set root token using HTTP.
    static void HttpSetRootToken(const std::string& proxy, const std::string& rootToken)
    {
        auto poller = CreateThreadPoolPoller(4, "HttpTest");
        auto clientConfig = New<NHttp::TClientConfig>();
        auto httpClient = NHttp::CreateClient(clientConfig, poller);

        auto tokenSha = NCrypto::TSha1Hasher().Append(rootToken).GetHexDigestLowerCase();
        const auto url = Format("http://%v/api/v4/set?path=//sys/tokens/%v", proxy, tokenSha);
        auto response = WaitFor(httpClient->Put(TString(url), TSharedRef::FromString("\"root\"")))
            .ValueOrThrow();
        if (response->GetStatusCode() != NHttp::EStatusCode::OK) {
            auto message = response->ReadAll();
            THROW_ERROR_EXCEPTION("HTTP request failed:\n%v", message);
        }
    }

};

const IClientPtr TMultiproxyTest::ReadAccessClient_ = TMultiproxyTest::CreateDirectClient("YT_PROXY_READ_ACCESS");
const IClientPtr TMultiproxyTest::WriteAccessClient_ = TMultiproxyTest::CreateDirectClient("YT_PROXY_WRITE_ACCESS");
const IClientPtr TMultiproxyTest::TargetClient_ = TMultiproxyTest::CreateDirectClient("YT_PROXY_TARGET");

////////////////////////////////////////////////////////////////////////////////

TEST_F(TMultiproxyTest, TestReadAccess)
{
    TYPath dir = "//home/test-read-access";
    WaitFor(
        TargetClient_->CreateNode(dir + "/table", EObjectType::Table, {.Recursive = true})
    ).ThrowOnError();


    auto readRedirectingClient = CreateRedirectingClient("YT_PROXY_READ_ACCESS", "target");
    EXPECT_EQ(WaitFor(readRedirectingClient->GetClusterName()).ValueOrThrow(), "target");

    auto listResult = WaitFor(readRedirectingClient->ListNode(dir))
        .ValueOrThrow();

    EXPECT_EQ(ConvertTo<std::vector<std::string>>(listResult), std::vector<std::string>{"table"});

    auto expectToFail = [&] {
        WaitFor(
            readRedirectingClient->CreateNode(dir + "/table2", EObjectType::Table, {.Recursive = true})
        ).ThrowOnError();
    };

    EXPECT_THROW_THAT(expectToFail(), testing::HasSubstr("Redirecting \"CreateNode\" request to cluster \"target\" is disabled by configuration"));
}

TEST_F(TMultiproxyTest, TestWriteAccess)
{
    TYPath dir = "//home/test-write-access";
    WaitFor(
        TargetClient_->CreateNode(dir + "/table", EObjectType::Table, {.Recursive = true})
    ).ThrowOnError();

    auto writeRedirectingClient = CreateRedirectingClient("YT_PROXY_WRITE_ACCESS", "target");
    EXPECT_EQ(WaitFor(writeRedirectingClient->GetClusterName()).ValueOrThrow(), "target");

    auto listResult = WaitFor(writeRedirectingClient->ListNode(dir))
        .ValueOrThrow();
    EXPECT_EQ(ConvertTo<std::vector<std::string>>(listResult), std::vector<std::string>{"table"});

    auto table2Id = WaitFor(
        writeRedirectingClient->CreateNode(dir + "/table2", EObjectType::Table, {.Recursive = true})
    ).ValueOrThrow();

    auto createdPath = WaitFor(
        TargetClient_->GetNode(Format("#%v/@path", table2Id))
    ).ValueOrThrow();

    EXPECT_EQ(ConvertTo<std::string>(createdPath), dir + "/table2");
}

TEST_F(TMultiproxyTest, TestNoAccess)
{
    auto noAccessClient = CreateRedirectingClient("YT_PROXY_TARGET", "read_access");

    auto expectToFail = [&] {
        WaitFor(noAccessClient->ListNode("//home"))
            .ThrowOnError();
    };

    EXPECT_THROW_THAT(expectToFail(), testing::HasSubstr("Redirecting \"ListNode\" request to cluster \"read_access\" is disabled by configuration"));
}

TEST_F(TMultiproxyTest, TestSelfAccess)
{
    EXPECT_EQ(WaitFor(TargetClient_->GetClusterName()).ValueOrThrow(), "target");

    auto loopClient = CreateRedirectingClient("YT_PROXY_TARGET", "target");

    auto clusterName = WaitFor(loopClient->GetClusterName())
        .ValueOrThrow();
    EXPECT_EQ(clusterName, "target");
}

TEST_F(TMultiproxyTest, TestTokens)
{
    CreateUser(TargetClient_, "test-tokens-user");
    auto token = CreateUser(ReadAccessClient_, "test-tokens-user");

    {
        // Create `//home/test-tokens that is readable for test-tokens-user
        // and '//home/test-tokens/hidden' that is acl closed for test-tokens-user.
        auto directTargetClient = CreateDirectClient("YT_PROXY_TARGET");
        TCreateNodeOptions createOptions;
        createOptions.Recursive = true;
        createOptions.Attributes = BuildAttributeDictionaryFluently()
            .Item("acl").Value(TSerializableAccessControlList{
                .Entries = {
                    TSerializableAccessControlEntry(NSecurityClient::ESecurityAction::Deny, {"test-tokens-user"}, EPermissionSet::Read)
                },
            })
            .Item("inherit_acl").Value(false)
        .EndMap();

        WaitFor(
            directTargetClient->CreateNode("//home/test-tokens/hidden", EObjectType::MapNode, createOptions)
        ).ThrowOnError();
    }

    {
        auto targetClient = CreateRedirectingClient("YT_PROXY_READ_ACCESS", "target", token);

        auto listResult = ConvertTo<std::vector<std::string>>(
            WaitFor(targetClient->ListNode("//home/test-tokens"))
                .ValueOrThrow());
        EXPECT_EQ(listResult, std::vector<std::string>{"hidden"});

        EXPECT_THROW_THAT(
            WaitFor(targetClient->ListNode("//home/test-tokens/hidden"))
                .ThrowOnError(),
            testing::HasSubstr("Access denied for user \"test-tokens-user\""));
    }

    {
        auto targetClient = CreateRedirectingClient("YT_PROXY_READ_ACCESS", "target", "target-cluster-token");

        EXPECT_THROW_THAT(
            WaitFor(targetClient->ListNode("//home/test-tokens"))
                .ThrowOnError(),
            testing::HasSubstr("Request authentication failed"));
    }
}

TEST_F(TMultiproxyTest, TestRemoteBan)
{
    CreateUser(TargetClient_, "test-remote-ban-user");
    auto token = CreateUser(WriteAccessClient_, "test-remote-ban-user");

    {
        SetUserBanned(TargetClient_, "test-remote-ban-user", true);
        auto targetClient = CreateRedirectingClient("YT_PROXY_WRITE_ACCESS", "target", token);

        auto action = [&] {
            WaitFor(targetClient->ListNode("/"))
                .ValueOrThrow();
        };
        EXPECT_THROW_THAT(
            action(),
            testing::HasSubstr("User \"test-remote-ban-user\" is banned"));
    }
}

TEST_F(TMultiproxyTest, TestLocalBan)
{
    CreateUser(TargetClient_, "test-local-ban-user");
    auto token = CreateUser(WriteAccessClient_, "test-local-ban-user");

    {
        SetUserBanned(WriteAccessClient_, "test-local-ban-user", true);
        auto targetClient = CreateRedirectingClient("YT_PROXY_WRITE_ACCESS", "target", token);

        auto action = [&] {
            WaitFor(targetClient->ListNode("/"))
                .ValueOrThrow();
        };
        EXPECT_THROW_THAT(
            action(),
            testing::HasSubstr("User \"test-local-ban-user\" is banned on cluster \"write_access\""));
    }
}

TEST_F(TMultiproxyTest, TestTypoTargetHostname)
{
    auto targetClient = CreateRedirectingClient("YT_PROXY_WRITE_ACCESS", "target-with-typo");
    auto action = [&] {
        WaitFor(targetClient->ListNode("/"))
            .ValueOrThrow();
    };
    EXPECT_THROW_THAT(
        action(),
        testing::HasSubstr("Cannot find cluster with name \"target-with-typo\""));
}

TEST_F(TMultiproxyTest, TestTypoTargetHostName_YT_26910)
{
    auto targetClient = CreateRedirectingClient("YT_PROXY_WRITE_ACCESS", "target-with-typo");
    auto action = [&] (const IClientPtr& client) {
        WaitFor(client->ListNode("/"))
            .ValueOrThrow();
    };
    EXPECT_THROW_THAT(
        action(targetClient),
        testing::HasSubstr("Cannot find cluster with name \"target-with-typo\""));

    auto token = CreateUser(WriteAccessClient_, "user-yt-26910");

    auto targetClient2 = CreateRedirectingClient("YT_PROXY_WRITE_ACCESS", "target-with-typo", token);
    EXPECT_THROW_THAT(
        action(targetClient2),
        testing::HasSubstr("Cannot find cluster with name \"target-with-typo\""));
}

TEST_F(TMultiproxyTest, TestUserMissingOnTargetCluster)
{
    auto token = CreateUser(WriteAccessClient_, "test-user-missing-on-target-cluster");

    {
        auto targetClient = CreateRedirectingClient("YT_PROXY_WRITE_ACCESS", "target", token);

        auto action = [&] {
            WaitFor(targetClient->ListNode("/"))
                .ValueOrThrow();
        };
        EXPECT_THROW_THAT(
            action(),
            testing::HasSubstr("No such user"));
    }
}

TEST_F(TMultiproxyTest, TestSignaturesThroughMultiproxy)
{
    auto tablePath = "//home/test-signatures-through-multiproxy/table";
    WaitFor(TargetClient_->CreateNode(tablePath, EObjectType::Table, {.Recursive = true}))
        .ThrowOnError();

    auto writeRedirectingClient = CreateRedirectingClient("YT_PROXY_WRITE_ACCESS", "target");

    TDistributedWriteSessionStartOptions options;
    options.CookieCount = 0;
    auto sessionWithCookies = WaitFor(writeRedirectingClient->StartDistributedWriteSession(tablePath, options))
        .ValueOrThrow();

    WaitFor(writeRedirectingClient->FinishDistributedWriteSession(TDistributedWriteSessionWithResults{
        .Session = std::move(sessionWithCookies.Session),
        .Results = {}}))
        .ThrowOnError();
}

TEST_F(TMultiproxyTest, TestSignaturesAreClusterSpecific)
{
    auto tablePath = "//home/test-signatures-are-cluster-specific/table";
    WaitFor(TargetClient_->CreateNode(tablePath, EObjectType::Table, {.Recursive = true}))
        .ThrowOnError();

    auto writeRedirectingClient = CreateRedirectingClient("YT_PROXY_WRITE_ACCESS", "target");

    TDistributedWriteSessionStartOptions options;
    options.CookieCount = 0;
    auto sessionWithCookies = WaitFor(writeRedirectingClient->StartDistributedWriteSession(tablePath, options))
        .ValueOrThrow();

    auto finishResult = WaitFor(TargetClient_->FinishDistributedWriteSession(TDistributedWriteSessionWithResults{
        .Session = std::move(sessionWithCookies.Session),
        .Results = {},
    }));

    EXPECT_THROW_WITH_SUBSTRING(finishResult.ThrowOnError(), "no child with key");
}

////////////////////////////////////////////////////////////////////////////////
