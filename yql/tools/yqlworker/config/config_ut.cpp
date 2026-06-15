#include "config.h"

#include <yql/essentials/utils/log/log_component.h>

#include <library/cpp/testing/unittest/registar.h>


using namespace NYql;

Y_UNIT_TEST_SUITE(TConfigTest)
{
    Y_UNIT_TEST(ParseFreeArgs) {
        TVector<TString> args = {
            "w.User=jamel",
            "w.Inspector.Hosts=host1,host2,host3",
            "w.Server.PortStart=7001",
            "w.Server.PortFinish=8001",
            "w.Executor.LoadUdfsDir=/some/path/to/udfs",
            "w.Executor.LoadUdfFiles=udf1,udf2",
            "w.Executor.LoadUdfFiles=udf3",

            "fs.MaxFiles=300",
            "fs.MaxSizeMb=512",

            "g.Yt.MrJobBin=",
        };

        TMainConfig config;
        ParseFreeArgsConfig(args, &config);

        // (1) worker config
        const auto& w = config.Worker;

        UNIT_ASSERT_STRINGS_EQUAL(w.GetUser(), "jamel");

        UNIT_ASSERT_EQUAL(w.GetLogging().LogDestSize(), 0);

        const auto& hosts = w.GetInspector().GetHosts();
        UNIT_ASSERT_EQUAL(hosts.size(), 3);
        UNIT_ASSERT_STRINGS_EQUAL(hosts.Get(0), "host1");
        UNIT_ASSERT_STRINGS_EQUAL(hosts.Get(1), "host2");
        UNIT_ASSERT_STRINGS_EQUAL(hosts.Get(2), "host3");

        UNIT_ASSERT_EQUAL(w.GetServer().GetPortStart(), 7001);
        UNIT_ASSERT_EQUAL(w.GetServer().GetPortFinish(), 8001);

        UNIT_ASSERT_STRINGS_EQUAL(
                    w.GetExecutor().GetLoadUdfsDir(),
                    "/some/path/to/udfs");

        const auto& udfsPaths = w.GetExecutor().GetLoadUdfFiles();
        UNIT_ASSERT_EQUAL(udfsPaths.size(), 3);
        UNIT_ASSERT_STRINGS_EQUAL(udfsPaths.Get(0), "udf1");
        UNIT_ASSERT_STRINGS_EQUAL(udfsPaths.Get(1), "udf2");
        UNIT_ASSERT_STRINGS_EQUAL(udfsPaths.Get(2), "udf3");

        // (2) file storage config
        const auto& fs = config.FileStorage;
        UNIT_ASSERT_EQUAL(fs.GetMaxFiles(), 300);
        UNIT_ASSERT_EQUAL(fs.GetMaxSizeMb(), 512);

        // (3) gateways config
        const auto& g = config.Gateways;

        UNIT_ASSERT_STRINGS_EQUAL(g.GetYt().GetMrJobBin(), "");
    }

    Y_UNIT_TEST(Components) {
        NLog::TComponentHelpers::ForEach([](NLog::EComponent c) {
            int cInt = NLog::TComponentHelpers::ToInt(c);
            UNIT_ASSERT(NProto::TLoggingConfig_EComponent_IsValid(cInt));
        });
    }
}
