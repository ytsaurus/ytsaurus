#include "multiprocessing.h"

#include <yql/essentials/utils/signals/signals.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/datetime.h>


using namespace NYql;

void SigUsrHandler(int signo)
{
    write(2, "SIGUSR1 arived\n", 15);
    Y_UNUSED(signo);
}

Y_UNIT_TEST_SUITE(TMultiprocessingTest)
{
    Y_UNIT_TEST(Process) {
        TProcessMainFunction mainFunc = [](void*) {
            signal(SIGUSR1, SigUsrHandler);

            sigset_t blockMask;
            SigFillSet(&blockMask);
            SigProcMask(SIG_BLOCK, &blockMask, nullptr);

            // wait for SIGUSR1 or SIGTERM
            SigDelSet(&blockMask, SIGUSR1);
            SigDelSet(&blockMask, SIGTERM);
            NYql::SigSuspend(&blockMask);
            return 42;
        };

        { // normal exit
            TProcess process(mainFunc);
            UNIT_ASSERT(false == process.IsSpawned());
            UNIT_ASSERT_EQUAL(process.GetId(), -1);

            process.Spawn();
            Sleep(TDuration::MilliSeconds(300));

            UNIT_ASSERT(process.IsSpawned());
            UNIT_ASSERT(process.GetId() > 0);
            UNIT_ASSERT(false == process.IsFinished());
            process.SendSignal(SIGUSR1);

            TProcessStatus status = process.Wait();
            UNIT_ASSERT(status.IsExited());
            UNIT_ASSERT(false == status.IsTerminated());
#ifndef WITH_VALGRIND
            UNIT_ASSERT_VALUES_EQUAL(status.GetExitStatus(), 42);
#endif
            UNIT_ASSERT(process.IsFinished());
        }

        { // terminated by a signal
            TProcess process(mainFunc);
            UNIT_ASSERT(false == process.IsSpawned());
            UNIT_ASSERT_EQUAL(process.GetId(), -1);

            process.Spawn();
            Sleep(TDuration::MilliSeconds(300));

            UNIT_ASSERT(process.IsSpawned());
            UNIT_ASSERT(process.GetId() > 0);
            UNIT_ASSERT(false == process.IsFinished());
            process.SendSignal(SIGTERM);

            TProcessStatus status = process.Wait();
            UNIT_ASSERT(false == status.IsExited());
            UNIT_ASSERT(status.IsTerminated());
            UNIT_ASSERT_EQUAL(status.GetTerminationSignal(), SIGTERM);
            UNIT_ASSERT(process.IsFinished());
        }
    }

    Y_UNIT_TEST(ProcessDetach) {
        SOCKET sockets[2];
        SocketPair(sockets);
        TProcess child([&](void*) {
            sleep(2);
            if (write(sockets[0], "done", 4) == -1) {
                Cerr << "cannot write to socket" << Endl;
                return 1;
            }
            return 0;
        });

        child.Spawn();
        UNIT_ASSERT(child.IsSpawned());

        child.Detach();
        UNIT_ASSERT(child.IsDetached());

        // after detaching process these functions will throw yexception
        UNIT_ASSERT_EXCEPTION(child.IsFinished(), yexception);
        UNIT_ASSERT_EXCEPTION(child.Wait(), yexception);
        UNIT_ASSERT_EXCEPTION(child.SendSignal(SIGTERM), yexception);

        // but process have to finish in background
        char buf[0x10] = {0};
        ssize_t nread = read(sockets[1], buf, Y_ARRAY_SIZE(buf));
        UNIT_ASSERT(nread > 0);
        UNIT_ASSERT_STRINGS_EQUAL(buf, "done");
    }
}
