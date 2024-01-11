#include "client.h"
#include "peer.h"

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/config.h>

#include <yt/yt/server/lib/hydra/config.h>

namespace NYT::NHydraStressTest {

using namespace NConcurrency;
using namespace NRpc;
using namespace NLogging;

//////////////////////////////////////////////////////////////////////////////////

TClient::TClient(
    TConfigPtr config,
    IChannelPtr peerChannel,
    IInvokerPtr invoker,
    TLivenessCheckerPtr livenessChecker,
    int clientId)
    : Config_(config)
    , Invoker_(invoker)
    , LivenessChecker_(livenessChecker)
    , Logger(HydraStressTestLogger.WithTag("ClientId: %v", clientId))
    , ConsistencyChecker_(New<TConsistencyChecker>())
    , Proxy_(peerChannel)
{
    Proxy_.SetDefaultTimeout(Config_->DefaultProxyTimeout);
}

void TClient::Run()
{
    YT_UNUSED_FUTURE(BIND(&TClient::DoRun, MakeStrong(this))
        .AsyncVia(Invoker_)
        .Run());
}

void TClient::RunRead()
{
    YT_LOG_DEBUG("Starting read");

    auto req = Proxy_.Read();

    auto result = WaitFor(req->Invoke());
    auto isOk = result.IsOK();
    LivenessChecker_->Report(isOk);
    if (!isOk) {
        YT_LOG_DEBUG(result, "Read failed");
        return;
    }

    auto value = result.Value()->result();
    YT_LOG_DEBUG("Read succeeded (Value: %v)", value);
    ConsistencyChecker_->Check(value);
}

void TClient::RunCas()
{
    YT_LOG_DEBUG("Starting CAS");

    auto readReq = Proxy_.Read();

    auto readResult = WaitFor(readReq->Invoke());
    auto isOk = readResult.IsOK();
    LivenessChecker_->Report(isOk);
    if (!isOk) {
        YT_LOG_DEBUG(readResult, "CAS read failed");
        return;
    }

    auto expected = readResult.Value()->result();
    YT_LOG_DEBUG("CAS read succeeded (Value: %v)", expected);
    ConsistencyChecker_->Check(expected);

    TDelayedExecutor::WaitForDuration(Config_->ClientWriteCasDelay);

    auto desired = expected + rand() % Config_->ClientIncrement;

    YT_LOG_DEBUG("Starting CAS write (Expected: %v, Desired: %v)",
        expected,
        desired);

    auto writeReq = Proxy_.Cas();
    writeReq->set_expected(expected);
    writeReq->set_desired(desired);
    GenerateMutationId(writeReq);

    auto writeResult = WaitFor(writeReq->Invoke());
    if (!writeResult.IsOK()) {
        YT_LOG_DEBUG(writeResult, "CAS write failed");
        return;
    }

    auto writeResultValue = writeResult.Value();
    if (!writeResultValue->success()) {
        YT_LOG_DEBUG("CAS write failed (Current: %v)", writeResultValue->current());
        return;
    }
    YT_LOG_DEBUG("CAS write succeeded");
}

void TClient::RunSequence()
{
    int count = rand() % 20 + 2;
    int id = rand();
    YT_LOG_DEBUG("Starting sequence (Count: %v, SequenceId: %v)",
        count,
        id);

    auto writeReq = Proxy_.Sequence();
    writeReq->set_count(count);
    writeReq->set_id(id);
    GenerateMutationId(writeReq);

    auto writeResult = WaitFor(writeReq->Invoke());
    if (!writeResult.IsOK()) {
        YT_LOG_DEBUG(writeResult, "Sequence failed (SequenceId: %v)",
            id);
    } else {
        YT_LOG_DEBUG("Sequence succeeded (SequenceId: %v)",
            id);
    }
}

void TClient::DoRun()
{
    while (true) {
        switch (rand() % 3) {
            case 0:
                RunRead();
                break;
            case 1:
                RunCas();
                break;
            case 2:
                RunSequence();
                break;
        }
        TDelayedExecutor::WaitForDuration(Config_->ClientInterval);
    }
}

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
