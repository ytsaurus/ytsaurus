#include "client.h"
#include "peer.h"

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/config.h>

#include <yt/yt/server/lib/hydra/config.h>

namespace NYT::NHydraStressTest {

using namespace NConcurrency;
using namespace NRpc;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TClient::TClient(
    TConfigPtr config,
    IChannelPtr peerChannel,
    IInvokerPtr invoker,
    TLivenessCheckerPtr livenessChecker,
    int clientId)
    : Config_(config)
    , Invoker_(invoker)
    , LivenessChecker_(livenessChecker)
    , Logger(HydraStressTestLogger().WithTag("ClientId", clientId))
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
    YT_TLOG_DEBUG("Starting read");

    auto req = Proxy_.Read();

    auto result = WaitFor(req->Invoke());
    auto isOk = result.IsOK();
    LivenessChecker_->Report(isOk);
    if (!isOk) {
        YT_TLOG_DEBUG("Read failed")
            .With(result);
        return;
    }

    auto value = result.Value()->result();
    YT_TLOG_DEBUG("Read succeeded")
        .With("Value", value);
    ConsistencyChecker_->Check(value);
}

void TClient::RunCas()
{
    YT_TLOG_DEBUG("Starting CAS");

    auto readReq = Proxy_.Read();

    auto readResult = WaitFor(readReq->Invoke());
    auto isOk = readResult.IsOK();
    LivenessChecker_->Report(isOk);
    if (!isOk) {
        YT_TLOG_DEBUG("CAS read failed")
            .With(readResult);
        return;
    }

    auto expected = readResult.Value()->result();
    YT_TLOG_DEBUG("CAS read succeeded")
        .With("Value", expected);
    ConsistencyChecker_->Check(expected);

    TDelayedExecutor::WaitForDuration(Config_->ClientWriteCasDelay);

    auto desired = expected + rand() % Config_->ClientIncrement;

    YT_TLOG_DEBUG("Starting CAS write")
        .With("Expected", expected)
        .With("Desired", desired);

    auto writeReq = Proxy_.Cas();
    writeReq->set_expected(expected);
    writeReq->set_desired(desired);
    GenerateMutationId(writeReq);

    auto writeResult = WaitFor(writeReq->Invoke());
    if (!writeResult.IsOK()) {
        YT_TLOG_DEBUG("CAS write failed")
            .With(writeResult);
        return;
    }

    auto writeResultValue = writeResult.Value();
    if (!writeResultValue->success()) {
        YT_TLOG_DEBUG("CAS write failed")
            .With("Current", writeResultValue->current());
        return;
    }
    YT_TLOG_DEBUG("CAS write succeeded");
}

void TClient::RunSequence()
{
    int count = rand() % 20 + 2;
    int id = rand();
    YT_TLOG_DEBUG("Starting sequence")
        .With("Count", count)
        .With("SequenceId", id);

    auto writeReq = Proxy_.Sequence();
    writeReq->set_count(count);
    writeReq->set_id(id);
    GenerateMutationId(writeReq);

    auto writeResult = WaitFor(writeReq->Invoke());
    if (!writeResult.IsOK()) {
        YT_TLOG_DEBUG("Sequence failed")
            .With("SequenceId", id)
            .With(writeResult);
    } else {
        YT_TLOG_DEBUG("Sequence succeeded")
            .With("SequenceId", id);
    }
}

void TClient::RunThrowException()
{
    bool expected = rand() % 7 != 0;
    YT_TLOG_DEBUG("Starting exception throw")
        .With("Expected", expected);

    auto writeReq = Proxy_.ThrowException();
    writeReq->set_expected(expected);
    GenerateMutationId(writeReq);

    auto writeResult = WaitFor(writeReq->Invoke());
    if (!writeResult.IsOK()) {
        YT_TLOG_DEBUG("ThrowException failed")
            .With(writeResult);
    } else {
        YT_TLOG_DEBUG("ThrowException succeeded");
    }
}

void TClient::DoRun()
{
    while (true) {
        switch (rand() % 4) {
            case 0:
                RunRead();
                break;
            case 1:
                RunCas();
                break;
            case 2:
                RunSequence();
                break;
            case 3:
                RunThrowException();
                break;
        }
        TDelayedExecutor::WaitForDuration(Config_->ClientInterval);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
