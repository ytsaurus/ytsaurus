#include <mapreduce/yt/tests/lib/lib.h>

#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/interface/errors.h>
#include <mapreduce/yt/interface/tvm.h>

#include <yt/yt/library/tvm/tvm.h>

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/tvmauth/client/mocked_updater.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>
#include <library/cpp/yt/memory/new.h>

using namespace NYT;
using namespace NYT::NTesting;

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

class TServiceTicketClientAuthTest
    : public NAuth::TServiceTicketClientAuth
{
public:
    TServiceTicketClientAuthTest(const NAuth::TTvmClientPtr& tvmClient)
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

Y_UNIT_TEST_SUITE(TvmAuth)
{
    Y_UNIT_TEST(TestTvmAuth)
    {
        auto serviceTicketAuth = New<TServiceTicketClientAuthTest>(CreateTvmClient());

        TTestFixture fixture(TCreateClientOptions().ServiceTicketAuth(serviceTicketAuth));
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        client->Create(workingDir + "/test_node", ENodeType::NT_MAP);
        auto issuedTickets = serviceTicketAuth->GetIssuedServiceTickets();
        EXPECT_GT(issuedTickets.size(), 0U);
        EXPECT_EQ(issuedTickets.front(), SERVICE_TICKET);
    }

    Y_UNIT_TEST(TestTvmOnly)
    {
        auto serviceTicketAuth = New<TServiceTicketClientAuthTest>(CreateTvmClient());

        UNIT_ASSERT_EXCEPTION_SATISFIES(
            CreateTestClient("somecluster", TCreateClientOptions()
                .ServiceTicketAuth(serviceTicketAuth)
                .TvmOnly(true)),
            std::exception,
            [] (const std::exception& ex) {
                return TString{ex.what()}.Contains("tvm.somecluster.yt.yandex.net:9026");
            }
        );

        UNIT_ASSERT_EXCEPTION_SATISFIES(
            CreateTestClient("somecluster", TCreateClientOptions()
                .ServiceTicketAuth(serviceTicketAuth)),
            std::exception,
            [] (const std::exception& ex) {
                return TString{ex.what()}.Contains("somecluster.yt.yandex.net:80");
            }
        );
    }
}

////////////////////////////////////////////////////////////////////////////////
