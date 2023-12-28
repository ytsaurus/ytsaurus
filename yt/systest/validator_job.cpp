
#include <library/cpp/yt/logging/logger.h>
#include <yt/cpp/mapreduce/interface/logging/logger.h>
#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/systest/validator_job.h>
#include <yt/systest/validator_service.h>

namespace NYT::NTest {

REGISTER_VANILLA_JOB(TValidatorJob);

TValidatorJob::TValidatorJob(TString dir)
    : Dir_(dir)
{
}

void TValidatorJob::Do()
{
    NYT::SetLogger(NYT::CreateStdErrLogger(NYT::ILogger::INFO));

    YT_VERIFY(!gethostname(Hostname_, sizeof(Hostname_)));

    const int port = 2827;

    auto client = NYT::CreateClientFromEnv();

    // TODO(YT-20802) implement service announcement and discovery properly.
    client->Create(
        Dir_ + "/" + Hostname_ + ":" + std::to_string(port),
        NT_STRING,
        TCreateOptions().IgnoreExisting(true)
    );

    RunValidatorService(client, port);
}

}  // namespace NYT::NTest
