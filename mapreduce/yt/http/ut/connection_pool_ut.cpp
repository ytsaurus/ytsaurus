#include "simple_server.h"

#include <mapreduce/yt/http/http.h>

#include <library/unittest/registar.h>
#include <library/unittest/tests_data.h>
#include <library/http/io/stream.h>

#include <util/string/builder.h>
#include <util/stream/tee.h>


SIMPLE_UNIT_TEST_SUITE(NConnectionPoolSuite) {
    SIMPLE_UNIT_TEST(TestReleaseUnread) {
        TPortManager pm;
        int port = pm.GetPort();
        TSimpleServer simpleServer(
            port,
            [] (TInputStream* input, TOutputStream* output) {
                try {
                    while (true) {
                        THttpInput httpInput(input);
                        httpInput.ReadAll();

                        THttpOutput httpOutput(output);
                        httpOutput.EnableKeepAlive(true);
                        httpOutput << "HTTP/1.1 200 OK\r\n";
                        httpOutput << "\r\n";
                        for (size_t i = 0; i != 10000; ++i) {
                            httpOutput << "The grass was greener";
                        }
                        httpOutput.Flush();
                    }
                } catch (const yexception&) {
                }
            });

        Stroka hostName = TStringBuilder() << "localhost:" << port;

        for (size_t i = 0; i != 10; ++i) {
            NYT::THttpRequest request(hostName);
            request.Connect();
            request.StartRequest(NYT::THttpHeader("GET", "foo"));
            request.FinishRequest();
            request.GetResponseStream();
        }
    }
}
