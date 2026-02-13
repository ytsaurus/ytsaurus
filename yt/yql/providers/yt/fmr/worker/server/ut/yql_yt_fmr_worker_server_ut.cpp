#include <library/cpp/testing/unittest/tests_data.h>

#include <yt/yql/providers/yt/fmr/coordinator/impl/ut/yql_yt_coordinator_ut.h>
#include <yt/yql/providers/yt/fmr/worker/server/yql_yt_fmr_worker_server.h>

#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/http/misc/httpcodes.h>
#include <util/stream/str.h>


namespace NYql::NFmr {

Y_UNIT_TEST_SUITE(WorkerServerTests) {
    Y_UNIT_TEST(SendPingRequestToWorkerServerWhenRunning) {
        TFmrTestSetup setup;
        auto coordinator = setup.GetFmrCoordinator();
        auto worker = setup.GetFmrWorker(coordinator);

        TPortManager pm;
        const ui16 port = pm.GetPort();
        TFmrWorkerServerSettings workerServerSettings{.Port = port};
        auto workerServer = MakeFmrWorkerServer(workerServerSettings, worker);
        workerServer->Start();

        // Give the server a moment to start
        Sleep(TDuration::MilliSeconds(100));

        TKeepAliveHttpClient httpClient("localhost", port);
        TStringStream outputStream;

        auto httpCode = httpClient.DoGet("/ping", &outputStream);
        UNIT_ASSERT_VALUES_EQUAL(httpCode, HTTP_OK);
        UNIT_ASSERT_VALUES_EQUAL(outputStream.Str(), "Ok");
        outputStream.Clear();
        // Stop the worker and check that the server provide correct status
        worker->Stop();
        Sleep(TDuration::MilliSeconds(100));
        auto httpCodeStopped = httpClient.DoGet("/ping", &outputStream);
        UNIT_ASSERT_VALUES_EQUAL(httpCodeStopped, HTTP_SERVICE_UNAVAILABLE);
        UNIT_ASSERT_VALUES_EQUAL(outputStream.Str(), "Stopped");
        workerServer->Stop();
    }
}

} // namespace NYql::NFmr
