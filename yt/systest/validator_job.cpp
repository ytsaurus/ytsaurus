
#include <library/cpp/yt/logging/logger.h>
#include <yt/cpp/mapreduce/interface/logging/logger.h>
#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/systest/validator_job.h>
#include <yt/systest/validator_service.h>

#include <random>

namespace NYT::NTest {

static void AnnounceHostPort(
    IClientPtr client,
    const TString& dir,
    const TString& hostName,
    int port)
{
    client->Create(
        dir + "/" + hostName + ":" + std::to_string(port),
        NT_STRING,
        TCreateOptions().IgnoreExisting(true)
    );
}

REGISTER_VANILLA_JOB(TValidatorJob);

TValidatorJob::TValidatorJob(TString dir)
    : Dir_(dir)
{
}

void TValidatorJob::Do()
{
    NYT::SetLogger(NYT::CreateStdErrLogger(NYT::ILogger::INFO));
    NLogging::TLogger Logger("validator-job");

    YT_VERIFY(!gethostname(Hostname_, sizeof(Hostname_)));

    auto client = NYT::CreateClientFromEnv();

    std::mt19937_64 engine(TInstant::Now().MicroSeconds());
    std::uniform_int_distribution<int> dist(10000, 40000);

    while (true) {
        int port = dist(engine);
        YT_LOG_INFO("Will start validator on port %v", port);
        AnnounceHostPort(client, Dir_, Hostname_, port);
        try {
            RunValidatorService(client, port);
        } catch (const TErrorException& exception) {
            if (exception.Error().GetCode() == NBus::EErrorCode::TransportError) {
                YT_LOG_INFO("Failed to start server, retry with different port (Exception: %v)",
                    exception.what());
                continue;
            }
            std::rethrow_exception(std::current_exception());
        }
    }
}

}  // namespace NYT::NTest
