#include "socket_transfer.h"
#include "multiprocessing.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/tempfile.h>
#include <util/folder/dirut.h>

using namespace NYql;

namespace {
    struct TProcessArgs {
        int ProcessId;
        TString SocketPath;
    };

    int ChildProcMain(void* arg) {
        const TProcessArgs* args = static_cast<const TProcessArgs*>(arg);
        Cerr << "Running child: id=" << args->ProcessId << " on socket=" << args->SocketPath << Endl;
        const TString header = "Hello from ";
        const TString sendMsg = TStringBuilder() << header << args->ProcessId;
        auto socketAddr = MakeLocalSocketAddress(args->SocketPath);
        TLocalStreamSocket sock(ReceiveSocket(socketAddr.Get()).Release());

        TStreamSocketOutput out(&sock);
        out.Write(sendMsg.data(), sendMsg.size());

        std::vector<char> buf(header.size() + 1);
        TStreamSocketInput in(&sock);
        in.LoadOrFail(buf.data(), buf.size());

        const TString expected = TStringBuilder() << header << (args->ProcessId ? 0 : 1);
        const TString actual(buf.data(), buf.size());
        if (expected != actual) {
            Cerr << "Unexpected message from peer on process id=" << args->ProcessId << ": expected " << expected.Quote() << ", got " << actual.Quote() << Endl;
            return 1;
        }

        return 0;
    }

    void TestTransferSocketPair(const TString& socketDir) {
        SOCKET sockets[2];
        SocketPair(sockets);

        TSocketHolder transfer1(sockets[0]);
        TSocketHolder transfer2(sockets[1]);

        TProcessArgs child1Args = { .ProcessId = 0, .SocketPath = MakeTempName(socketDir.c_str()) };
        TProcessArgs child2Args = { .ProcessId = 1, .SocketPath = MakeTempName(socketDir.c_str()) };

        TProcess child1(ChildProcMain);
        TProcess child2(ChildProcMain);

        TSocketTransferServer server1(MakeLocalSocketAddress(child1Args.SocketPath), std::move(transfer1));
        TSocketTransferServer server2(MakeLocalSocketAddress(child2Args.SocketPath), std::move(transfer2));

        server1.Start();
        server2.Start();

        child1.Spawn(&child1Args);
        child2.Spawn(&child2Args);

        {
            auto status = child1.Wait();
            UNIT_ASSERT(status.IsExited());
            UNIT_ASSERT(!status.IsTerminated());
            UNIT_ASSERT_VALUES_EQUAL(status.GetExitStatus(), 0);

        }
        {
            auto status = child2.Wait();
            UNIT_ASSERT(status.IsExited());
            UNIT_ASSERT(!status.IsTerminated());
            UNIT_ASSERT_VALUES_EQUAL(status.GetExitStatus(), 0);
        }
        server1.Stop();
        server2.Stop();
    }
} // namespace

Y_UNIT_TEST_SUITE(TSocketTransferTest)
{
    Y_UNIT_TEST(TransferSocketToChildProcess) {
        SOCKET sockets[2];
        SocketPair(sockets);

        TSocketHolder toTransfer(sockets[0]);
        TLocalStreamSocket other(sockets[1]);
        TString sockPath = MakeTempName();

        TProcess child([](void *path) {
            TFsPath socketPath(static_cast<const char *>(path));
            auto socketAddr = MakeLocalSocketAddress(socketPath);
            TLocalStreamSocket sock(ReceiveSocket(socketAddr.Get()).Release());
            TStreamSocketOutput out(&sock);
            out.Write("done");
            return 0;
        });

        TSocketTransferServer server(MakeLocalSocketAddress(sockPath),
                                    std::move(toTransfer));
        server.Start();
        child.Spawn(const_cast<char *>(sockPath.c_str()));
        char buf[4];

        TStreamSocketInput in(&other);
        size_t r = in.Load(buf, sizeof(buf));

        UNIT_ASSERT_EQUAL(r, sizeof(buf));
        UNIT_ASSERT_STRINGS_EQUAL("done", TStringBuf(buf, sizeof(buf)));

        server.Stop();
        auto status = child.Wait();
        UNIT_ASSERT(status.IsExited());
        UNIT_ASSERT(!status.IsTerminated());
        UNIT_ASSERT_VALUES_EQUAL(status.GetExitStatus(), 0);
    }

    Y_UNIT_TEST(TransferSocketPairToChildren) {
        TestTransferSocketPair(GetSystemTempDir());
    }

    Y_UNIT_TEST(TransferSocketPairToChildrenWithLongDir) {
        auto tmpDir = TFsPath(GetSystemTempDir()) / TString(100, 'a');
        MakeDirIfNotExist(tmpDir.GetPath().c_str());
        TestTransferSocketPair(tmpDir.GetPath());
    }
}
