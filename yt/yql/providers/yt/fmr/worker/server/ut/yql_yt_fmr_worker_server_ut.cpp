#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <yt/yql/providers/yt/fmr/worker/server/yql_yt_fmr_worker_server.h>
#include <yt/yql/providers/yt/fmr/worker/impl/yql_yt_worker_impl.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/worker/impl/yql_yt_worker_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file/yql_yt_file_coordinator_service.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>

#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/http/misc/httpcodes.h>
#include <util/stream/str.h>


namespace NYql::NFmr {

Y_UNIT_TEST_SUITE(WorkerServerTests) {
    Y_UNIT_TEST(SendPingRequestToWorkerServerWhenRunning) {
        auto coordinator = MakeFmrCoordinator(TFmrCoordinatorSettings(), MakeFileYtCoordinatorService());
        auto func = [&] (TTask::TPtr /*task*/, std::shared_ptr<std::atomic<bool>> cancelFlag) {
            while (!cancelFlag->load()) {
                return TJobResult{.TaskStatus = ETaskStatus::Completed, .Stats = TStatistics()};
            }
            return TJobResult{.TaskStatus = ETaskStatus::Failed, .Stats = TStatistics()};
        };
        TFmrJobFactorySettings settings{.NumThreads = 1, .Function = func};
        auto factory = MakeFmrJobFactory(settings);

        TFmrWorkerSettings workerSettings{.WorkerId = 0, .RandomProvider = CreateDeterministicRandomProvider(1)};
        auto worker = MakeFmrWorker(coordinator, factory, workerSettings);
        TPortManager pm;
        const ui16 port = pm.GetPort();
        TFmrWorkerServerSettings workerServerSettings{.Port = port};
        auto workerServer = MakeFmrWorkerServer(workerServerSettings, worker);
        workerServer->Start();
        worker->Start();

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
