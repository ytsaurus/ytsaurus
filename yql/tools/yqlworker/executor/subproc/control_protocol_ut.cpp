#include "control_protocol.h"

#include <yql/tools/yqlworker/misc/multiprocessing.h>
#include <yql/essentials/utils/signals/signals.h>
#include <yql/essentials/utils/log/log.h>

#include <util/system/event.h>
#include <util/system/mutex.h>
#include <util/datetime/cputimer.h>

#include <library/cpp/testing/unittest/registar.h>


using namespace NYql;
using namespace NSubprocessTaskExecutor;

struct TStubTerminationHandler: public IWorkerTerminationHandler {
    void OnWorkerTermination(pid_t pid, int status) override {
        Y_UNUSED(pid); Y_UNUSED(status);
        Y_ABORT("must not be called");
    }

    void OnServerTermination() override {
        Y_ABORT("must not be called");
    }

    void OnServerReady(bool inTerminatingState) override {
        Y_UNUSED(inTerminatingState);
        Y_ABORT("must not be called");
    }

    void OnDisconnect() override {
    }
};

struct TStubWorkerControlServerHandler: public IWorkerControlServerHandler {
    void OnSpawnWorkers(ui32 count, int dataServerPort) override {
        Y_UNUSED(count);
        Y_UNUSED(dataServerPort);
        Y_ABORT("must not be called");
    }

    void OnKillWorker(pid_t pid) override {
        Y_UNUSED(pid);
        Y_ABORT("must not be called");
    }

    void OnDisconnect() override {
    }

    void OnRequestReady() override {
        Y_ABORT("must not be called");
    }
};

Y_UNIT_TEST_SUITE(TControlProtocolTest) {

    Y_UNIT_TEST(SpawnWorkers) {
        TMutex mutex;
        auto ch = DuplexIpcChannel();

        struct TTestServerHandler: public TStubWorkerControlServerHandler {
            TMutex& Mutex;
            bool OnSpawnWorkerCalled = false;
            ui32 Count = 0;
            int DataServerPort = 0;

            TTestServerHandler(TMutex& mutex)
                : Mutex(mutex)
            {}

            void OnSpawnWorkers(ui32 count, int dataServerPort) override {
                with_lock(Mutex) {
                    OnSpawnWorkerCalled = true;
                    Count = count;
                    DataServerPort = dataServerPort;
                }
            }
        } serverHandler(mutex);
        TControlServer server(ch.first, INVALID_PIPEHANDLE, &serverHandler);
        server.RunEventLoopInBackground();

        TStubTerminationHandler terminationHandler;
        TControlClient client(ch.second, &terminationHandler);
        client.RunEventLoopInBackground();

        client.SpawnWorkers(3, 12345);
        Sleep(TDuration::MilliSeconds(200));
        with_lock (mutex) {
            UNIT_ASSERT(serverHandler.OnSpawnWorkerCalled);
            UNIT_ASSERT_VALUES_EQUAL(serverHandler.Count, 3);
            UNIT_ASSERT_VALUES_EQUAL(serverHandler.DataServerPort, 12345);
        }
    }

    Y_UNIT_TEST(KillWorker) {
        auto ch = DuplexIpcChannel();
        TMutex mutex;

        struct TTestServerHandler: public TStubWorkerControlServerHandler {
            TMutex& Mutex;
            bool OnKillWorkerCalled = false;
            pid_t Pid = 0;

            TTestServerHandler(TMutex& mutex)
                : Mutex(mutex)
            {}

            void OnKillWorker(pid_t pid) override {
                with_lock(Mutex) {
                    OnKillWorkerCalled = true;
                    Pid = pid;
                }
            }
        } serverHandler(mutex);
        TControlServer server(ch.first, INVALID_PIPEHANDLE, &serverHandler);
        server.RunEventLoopInBackground();

        TStubTerminationHandler terminationHandler;
        TControlClient client(ch.second, &terminationHandler);
        client.RunEventLoopInBackground();

        client.KillWorker(12345);
        Sleep(TDuration::MilliSeconds(200));
        with_lock(mutex) {
            UNIT_ASSERT(serverHandler.OnKillWorkerCalled);
            UNIT_ASSERT_VALUES_EQUAL(serverHandler.Pid, 12345);
        }
    }

    Y_UNIT_TEST(RequestReady) {
        auto ch = DuplexIpcChannel();
        TMutex mutex;

        struct TTestServerHandler: public TStubWorkerControlServerHandler {
            TMutex& Mutex;
            bool OnRequestReadyCalled = false;

            TTestServerHandler(TMutex& mutex)
                    : Mutex(mutex)
            {}

            void OnRequestReady() override {
                with_lock(Mutex) {
                    OnRequestReadyCalled = true;
                }
            }
        } serverHandler(mutex);
        TControlServer server(ch.first, INVALID_PIPEHANDLE, &serverHandler);
        server.RunEventLoopInBackground();

        TStubTerminationHandler terminationHandler;
        TControlClient client(ch.second, &terminationHandler);
        client.RunEventLoopInBackground();

        client.RequestReady();
        Sleep(TDuration::MilliSeconds(200));
        with_lock(mutex) {
            UNIT_ASSERT(serverHandler.OnRequestReadyCalled);
        }
    }

    Y_UNIT_TEST(ResubmitSpawnsAfterServerReady) {
        // Simulates forker restart path: new forker calls NotifyAboutSelfReady()
        // and core re-issues SpawnWorkers via ResubmitPendingSpawns().
        auto ch = DuplexIpcChannel();
        TMutex mutex;

        struct TTestServerHandler: public TStubWorkerControlServerHandler {
            TMutex& Mutex;
            ui32 SpawnCount = 0;
            int DataServerPort = 0;

            TTestServerHandler(TMutex& mutex)
                : Mutex(mutex)
            {}

            void OnSpawnWorkers(ui32 count, int dataServerPort) override {
                with_lock(Mutex) {
                    SpawnCount = count;
                    DataServerPort = dataServerPort;
                }
            }
        } serverHandler(mutex);

        struct TTestTerminationHandler: public TStubTerminationHandler {
            TMutex& Mutex;
            TControlClient* Client = nullptr;
            bool OnServerReadyCalled = false;
            bool InTerminatingState = true;

            TTestTerminationHandler(TMutex& mutex)
                : Mutex(mutex)
            {}

            void OnServerReady(bool inTerminatingState) override {
                with_lock(Mutex) {
                    OnServerReadyCalled = true;
                    InTerminatingState = inTerminatingState;
                }
                Client->SpawnWorkers(3, 6578);
            }
        } terminationHandler(mutex);

        TControlServer server(ch.first, INVALID_PIPEHANDLE, &serverHandler);
        server.RunEventLoopInBackground();

        TControlClient client(ch.second, &terminationHandler);
        terminationHandler.Client = &client;
        client.RunEventLoopInBackground();

        server.NotifyAboutSelfReady();
        Sleep(TDuration::MilliSeconds(200));

        with_lock(mutex) {
            UNIT_ASSERT(terminationHandler.OnServerReadyCalled);
            UNIT_ASSERT(!terminationHandler.InTerminatingState);
            UNIT_ASSERT_VALUES_EQUAL(serverHandler.SpawnCount, 3);
            UNIT_ASSERT_VALUES_EQUAL(serverHandler.DataServerPort, 6578);
        }
    }

    Y_UNIT_TEST(ServerReadyAfterDisconnect) {
        TMutex mutex;

        struct TTestTerminationHandler: public IWorkerTerminationHandler {
            TMutex& Mutex;
            bool OnDisconnectCalled = false;
            ui32 OnServerReadyCount = 0;

            explicit TTestTerminationHandler(TMutex& mutex)
                : Mutex(mutex)
            {}

            void OnWorkerTermination(pid_t, int) override {
                Y_ABORT("must not be called");
            }

            void OnServerTermination() override {
                Y_ABORT("must not be called");
            }

            void OnServerReady(bool) override {
                with_lock(Mutex) {
                    ++OnServerReadyCount;
                }
            }

            void OnDisconnect() override {
                with_lock(Mutex) {
                    OnDisconnectCalled = true;
                }
            }
        };

        auto ch1 = DuplexIpcChannel();
        TStubWorkerControlServerHandler serverHandler1;
        TControlServer server1(ch1.first, INVALID_PIPEHANDLE, &serverHandler1);
        server1.RunEventLoopInBackground();

        TTestTerminationHandler terminationHandler(mutex);
        TControlClient client1(ch1.second, &terminationHandler);
        client1.RunEventLoopInBackground();

        server1.Shutdown();
        Sleep(TDuration::MilliSeconds(100));
        with_lock(mutex) {
            UNIT_ASSERT(terminationHandler.OnDisconnectCalled);
            UNIT_ASSERT_VALUES_EQUAL(terminationHandler.OnServerReadyCount, 0U);
        }
        client1.Shutdown();

        auto ch2 = DuplexIpcChannel();
        TStubWorkerControlServerHandler serverHandler2;
        TControlServer server2(ch2.first, INVALID_PIPEHANDLE, &serverHandler2);
        server2.RunEventLoopInBackground();

        TControlClient client2(ch2.second, &terminationHandler);
        client2.RunEventLoopInBackground();

        server2.NotifyAboutSelfReady();
        Sleep(TDuration::MilliSeconds(100));
        with_lock(mutex) {
            UNIT_ASSERT_VALUES_EQUAL(terminationHandler.OnServerReadyCount, 1U);
        }
    }

    Y_UNIT_TEST(WorkerTermination) {
        InitSignalsWithSelfPipe();
        AllowAnySignals();

        auto ch = DuplexIpcChannel();
        TMutex mutex;
        TStubWorkerControlServerHandler serverHandler;
        TControlServer server(ch.first, SignalPipeR.GetHandle(), &serverHandler);
        server.RunEventLoopInBackground();

        TProcess child([](void*) { return 7; });
        child.Spawn();
        TProcessStatus s = child.Wait();
        server.NotifyAboutTerminatedWorkers({
                { child.GetId(), s.GetRawStatus() }
        });

        struct TTestTerminationHandler: public IWorkerTerminationHandler {
            TMutex& Mutex;
            bool OnWorkerTerminationCalled = false;
            bool OnServerTerminationCalled = false;
            bool OnServerReadyCalled = false;
            bool OnDisconnectCalled = false;
            bool PidOk = false;
            bool IsExited = false;
            int ExitStatus = 0;

            TTestTerminationHandler(TMutex& mutex)
                : Mutex(mutex)
            {}

            void OnWorkerTermination(pid_t pid, int status) override {
                with_lock(Mutex) {
                    OnWorkerTerminationCalled = true;
                    PidOk = pid > 0;
                    TProcessStatus s(status);
                    IsExited = s.IsExited();
                    ExitStatus = s.GetExitStatus();
                }
            }

            void OnServerTermination() override {
                with_lock(Mutex) {
                    OnServerTerminationCalled = true;
                }
            }

            void OnServerReady(bool inTerminatingState) override {
                Y_UNUSED(inTerminatingState);
                with_lock(Mutex) {
                    OnServerReadyCalled = true;
                }
            }

            void OnDisconnect() override {
                with_lock(Mutex) {
                    OnDisconnectCalled = true;
                }
            }
        } terminationHandler(mutex);
        TControlClient client(ch.second, &terminationHandler);
        client.RunEventLoopInBackground();

        Sleep(TDuration::MilliSeconds(100));
        with_lock(mutex) {
            UNIT_ASSERT(!terminationHandler.OnDisconnectCalled);
            UNIT_ASSERT(terminationHandler.OnWorkerTerminationCalled);
            UNIT_ASSERT(terminationHandler.PidOk);
            UNIT_ASSERT(terminationHandler.IsExited);
#ifndef WITH_VALGRIND
            UNIT_ASSERT_VALUES_EQUAL(terminationHandler.ExitStatus, 7);
#endif
        }

        server.NotifyAboutSelfReady();
        Sleep(TDuration::MilliSeconds(100));
        with_lock(mutex) {
            UNIT_ASSERT(terminationHandler.OnServerReadyCalled);
        }

        server.NotifyAboutSelfTermination();
        Sleep(TDuration::MilliSeconds(100));
        with_lock(mutex) {
            UNIT_ASSERT(terminationHandler.OnServerTerminationCalled);
        }
    }

    Y_UNIT_TEST(SocketDisconnect) {
        InitSignalsWithSelfPipe();
        AllowAnySignals();

        auto ch1 = DuplexIpcChannel();
        TMutex mutex;
        struct TTestServerHandler : public TStubWorkerControlServerHandler {
            TMutex& Mutex;
            ui32 OnDisconnectCount = 0;

            TTestServerHandler(TMutex& mutex)
                : Mutex(mutex)
            {}

            void OnDisconnect() override {
                with_lock(Mutex) {
                    ++OnDisconnectCount;
                }
            }
        } serverHandler(mutex);

        struct TTestTerminationHandler : public TStubTerminationHandler {
            TMutex& Mutex;
            ui32 OnDisconnectCount = 0;

            TTestTerminationHandler(TMutex& mutex)
                : Mutex(mutex)
            {}

            void OnDisconnect() override {
                with_lock(Mutex) {
                    ++OnDisconnectCount;
                }
            }
        } terminationHandler(mutex);

        // case 1
        TControlServer server1(ch1.first, SignalPipeR.GetHandle(), &serverHandler);
        server1.RunEventLoopInBackground();

        TControlClient client1(ch1.second, &terminationHandler);
        client1.RunEventLoopInBackground();

        Sleep(TDuration::MilliSeconds(100));
        with_lock(mutex) {
            UNIT_ASSERT_VALUES_EQUAL(serverHandler.OnDisconnectCount, 0U);
            UNIT_ASSERT_VALUES_EQUAL(terminationHandler.OnDisconnectCount, 0U);
        }

        // shutdown client first
        client1.Shutdown();
        Sleep(TDuration::MilliSeconds(100));

        with_lock(mutex) {
            UNIT_ASSERT_VALUES_EQUAL(serverHandler.OnDisconnectCount, 1U);
            UNIT_ASSERT_VALUES_EQUAL(terminationHandler.OnDisconnectCount, 0U);
        }

        server1.Shutdown();

        // case 2
        with_lock(mutex) {
            serverHandler.OnDisconnectCount = 0;
        }
        auto ch2 = DuplexIpcChannel();
        TControlServer server2(ch2.first, SignalPipeR.GetHandle(), &serverHandler);
        server2.RunEventLoopInBackground();

        TControlClient client2(ch2.second, &terminationHandler);
        client2.RunEventLoopInBackground();

        Sleep(TDuration::MilliSeconds(100));
        // shutdown server first
        server2.Shutdown();
        Sleep(TDuration::MilliSeconds(100));

        with_lock(mutex) {
            UNIT_ASSERT_VALUES_EQUAL(serverHandler.OnDisconnectCount, 0U);
            UNIT_ASSERT_VALUES_EQUAL(terminationHandler.OnDisconnectCount, 1U);
        }
    }

    Y_UNIT_TEST(Stress) {
        NLog::YqlLoggerScope logger(&Cerr);

        const static int messagesCount = 10000;
        auto ch = DuplexIpcChannel();

        TStubWorkerControlServerHandler serverHandler;
        TControlServer server(ch.first, INVALID_PIPEHANDLE, &serverHandler);
        server.RunEventLoopInBackground();

        struct TTestTerminationHandler: public TStubTerminationHandler {
            TManualEvent Done;
            int WorkerTerminationCount = 0;

            void OnWorkerTermination(pid_t pid, int /*status*/) override {
                if (++WorkerTerminationCount == messagesCount) {
                    YQL_LOG(INFO)  << "Done.Signal()" << Endl;
                    Done.Signal();
                } else if (WorkerTerminationCount % 1000 == 0) {
                    YQL_LOG(INFO) << "OnWorkerTermination(" << pid << ')';
                }
            }

            void WaitCompletion() {
                Done.WaitI();
            }

        } terminationHandler;
        TControlClient client(ch.second, &terminationHandler);
        client.RunEventLoopInBackground();

        YQL_LOG(INFO) << "--------------[ START SENDING ]----------------";
        try {
            TTimer timer;
            for (int i = 0; i < messagesCount; i += 5) {
                server.NotifyAboutTerminatedWorkers({
                        { i  , 0 },
                        { i+1, 1 },
                        { i+2, 2 },
                        { i+3, 3 },
                        { i+4, 4 },
                });
            }

            YQL_LOG(INFO) << "---------------[ END SENDING ]-----------------";

            terminationHandler.WaitCompletion();
        } catch (...) {
            YQL_LOG(ERROR) << "Exception: " << CurrentExceptionMessage();
        }

        YQL_LOG(INFO) << "-------------------[ DONE ]--------------------";
        server.Shutdown();
        client.Shutdown();
    }
}
