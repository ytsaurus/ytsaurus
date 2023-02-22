#include "simple_server.h"

#include <mapreduce/yt/http/http.h>

#include <mapreduce/yt/interface/config.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

using namespace NYT;

void WriteDataFrame(TStringBuf string, IOutputStream* stream)
{
    stream->Write("\x01");
    ui32 size = string.Size();
    auto littleEndianSize = HostToLittle(size);
    stream->Write(&littleEndianSize, sizeof(littleEndianSize));
    stream->Write(string);
}

THolder<TSimpleServer> CreateFramingEchoServer()
{
    TPortManager pm;
    int port = pm.GetPort();
    return MakeHolder<TSimpleServer>(
        port,
        [] (IInputStream* input, IOutputStream* output) {
            try {
                THttpInput httpInput(input);
                if (!httpInput.Headers().FindHeader("X-YT-Accept-Framing")) {
                    UNIT_FAIL("X-YT-Accept-Framing header not found");
                }
                auto input = httpInput.ReadAll();

                THttpOutput httpOutput(output);
                httpOutput << "HTTP/1.1 200 OK\r\n";
                httpOutput << "X-YT-Framing: 1\r\n";
                httpOutput << "\r\n";
                httpOutput << "\x02\x02"; // Two KeepAlive frames.
                WriteDataFrame("", &httpOutput);
                WriteDataFrame(TStringBuf(input).SubString(0, 10), &httpOutput);
                httpOutput << "\x02"; // KeepAlive.
                WriteDataFrame("", &httpOutput);
                WriteDataFrame(TStringBuf(input).SubString(10, std::string::npos), &httpOutput);
                httpOutput << "\x02"; // KeepAlive.

                httpOutput.Flush();
            } catch (const std::exception& exc) {
            }
        });
}

Y_UNIT_TEST_SUITE(HttpHeader)
{
    Y_UNIT_TEST(TestAddParameter)
    {
        THttpHeader header("POST", "/foo");
        header.AddMutationId();

        auto id1 = header.GetParameters()["mutation_id"].AsString();

        header.AddMutationId();

        auto id2 = header.GetParameters()["mutation_id"].AsString();

        UNIT_ASSERT(id1 != id2);
    }
}

Y_UNIT_TEST_SUITE(Framing)
{
    Y_UNIT_TEST(TestFramingSimple)
    {
        auto server = CreateFramingEchoServer();

        THttpRequest request;
        request.Connect(server->GetAddress());
        auto requestStream = request.StartRequest(THttpHeader("POST", "concatenate"));
        *requestStream << "Some funny data";
        request.FinishRequest();
        auto response = request.GetResponseStream()->ReadAll();
        UNIT_ASSERT_VALUES_EQUAL(response, "Some funny data");
    }

    Y_UNIT_TEST(TestFramingLarge)
    {
        auto server = CreateFramingEchoServer();

        THttpRequest request;
        request.Connect(server->GetAddress());
        auto requestStream = request.StartRequest(THttpHeader("POST", "concatenate"));
        auto data = TString(100000, 'x');
        *requestStream << data;
        request.FinishRequest();
        auto response = request.GetResponseStream()->ReadAll();
        UNIT_ASSERT_EQUAL(response, data);
    }
}
