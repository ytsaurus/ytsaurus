//%NUM_MASTERS=1
//%NUM_NODES=3
//%NUM_SCHEDULERS=0
//%DRIVER_BACKENDS=['rpc']
//%ENABLE_RPC_PROXY=True
//%DELTA_MASTER_CONFIG={"object_service":{"timeout_backoff_lead_time":100}}

#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/client/api/rpc_proxy/helpers.h>
#include <yt/yt/client/api/rpc_proxy/public.h>

#include <yt/yt/library/auth/tvm.h>

#include <library/cpp/tvmauth/client/mocked_updater.h>

namespace NYT {
namespace NCppTests {
namespace {

using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto SERVICE_TICKET =
    "3:serv:CBAQ__________9_IgYIlJEGECo:O9-vbod_8czkKrpwJAZCI8UgOIhNr2xKPcS-LWALrVC224jga2nIT6vLiw6q3d6pAT60g9K7NB39LEmh7vMuePtUMjzuZuL-uJg17BsH2iTLCZSxDjWxbU9piA2T6u607jiSyiy-FI74pEPqkz7KKJ28aPsefuC1VUweGkYFzNY";

NAuth::TTvmClientPtr CreateTvmClient() {
    NTvmAuth::TMockedUpdater::TSettings settings;
    settings.SelfTvmId = 100500;
    settings.Backends = {
        {
            /*.Alias_ = */ "my_dest",
            /*.Id_ = */ 2031010,
            /*.Value_ = */ SERVICE_TICKET,
        },
    };

    return std::make_shared<NTvmAuth::TTvmClient>(new NTvmAuth::TMockedUpdater(settings));
}

class TServiceTicketAuthTestWrapper
    : public NAuth::TServiceTicketClientAuth
{
public:
    TServiceTicketAuthTestWrapper(const NAuth::TTvmClientPtr& tvmClient)
        : NAuth::TServiceTicketClientAuth(tvmClient)
    { }

    virtual TString IssueServiceTicket() override
    {
        auto ticket = NAuth::TServiceTicketClientAuth::IssueServiceTicket();
        IssuedServiceTickets_.push_back(ticket);
        return ticket;
    }

    const std::vector<TString>& GetIssuedServiceTickets() const
    {
        return IssuedServiceTickets_;
    }

private:
    std::vector<TString> IssuedServiceTickets_;
};

TEST_F(TApiTestBase, TestTvmServiceTicketAuth)
{
    auto serviceTicketAuth = New<TServiceTicketAuthTestWrapper>(CreateTvmClient());
    auto clientOptions = TClientOptions::FromServiceTicketAuth(serviceTicketAuth);

    auto client = Connection_->CreateClient(clientOptions);

    client->CreateNode("//tmp/test_node", NObjectClient::EObjectType::MapNode).Get();

    auto issuedTickets = serviceTicketAuth->GetIssuedServiceTickets();
    EXPECT_GT(issuedTickets.size(), 0U);
    EXPECT_EQ(issuedTickets.front(), SERVICE_TICKET);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NCppTests
} // namespace NYT
