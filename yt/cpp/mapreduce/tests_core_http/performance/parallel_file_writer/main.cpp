#include <yt/cpp/mapreduce/library/parallel_io/parallel_file_writer.h>

#include <yt/cpp/mapreduce/tests_core_http/yt_unittest_lib/yt_unittest_lib.h>

#include <util/datetime/cputimer.h>

#include <util/system/tempfile.h>

using namespace NYT;

int main() {
    auto client = CreateClient("freud", TCreateClientOptions().UseCoreHttpClient(true));
    auto fileName = "temp";
    TTempFileHandle file(fileName);
    TFileOutput output(file);
    TSimpleTimer timer;
    const i64 dataSize = 2ll * 1024 * 1024 * 1024;
    const int number_of_pieces = 32;
    for (int i = 0; i < number_of_pieces; ++i) {
        output << NTesting::GenerateRandomData(dataSize / number_of_pieces);
    }
    Cout << "Write data: " << timer.Get().SecondsFloat() << Endl;
    output.Finish();
    TRichYPath path = "//tmp/parallel_file_writer_performance_test";
    auto path2 = "//tmp/parallel_file_writer_performance_test2";

    timer.Reset();
    TFileInput input(fileName);
    char* buf;
    auto writer = client->CreateFileWriter(path2);
    while (size_t bufSize = input.Next(&buf))  {
        writer->Write(buf, bufSize);
    }
    writer->Finish();
    Cout << "FileWriter: " << timer.Get().SecondsFloat() << Endl;

    timer.Reset();
    WriteFileParallel(client, fileName, path, TParallelFileWriterOptions().ThreadCount(10));
    Cout << "WriteFileParallel: " << timer.Get().SecondsFloat() << Endl;
}


