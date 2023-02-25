#include <yt/cpp/mapreduce/interface/client.h>
#include <util/system/user.h>
#include <yt/cpp/mapreduce/library/parallel_io/parallel_writer.h>

#include <util/datetime/cputimer.h>

using namespace NYT;

// This very long test may show that
// ParallelUnorderedTableWriter with 5 threads
// can be faster then TableWriter in 3 times

void Prepare(TVector<TNode> &v) {
    v.clear();
    auto timer = TSimpleTimer();
    auto client = CreateClient("freud", TCreateClientOptions().UseCoreHttpClient(true));
//    in this test we use 2GB size table
    TIFStream stream("path_to_table");
    auto reader = CreateTableReader<TNode>(&stream);
    for (; reader->IsValid(); reader->Next()) {
        v.push_back(reader->GetRow());
    }
    auto duration = timer.Get().SecondsFloat();
    Cout << "Load data complete! " << duration << Endl;
}

int main(int argc, const char** argv) {
    NYT::Initialize(argc, argv);

    const TString table = "//tmp/" + GetUsername() + "-read-write1";
    const TString table2 = "//tmp/" + GetUsername() + "-read-write2";
    int n = 3;
    double sumtime1 = 0;
    double sumtime2 = 0;
    auto client = CreateClient("freud", TCreateClientOptions().UseCoreHttpClient(true));
    TVector<TNode> completed;
    Prepare(completed);
    auto timer = TSimpleTimer();
    Cout << "ParallelUnorderedWriter" << Endl;
    for (int i = 0; i < n; ++i) {
        Cout << "Iteration: " << i << Endl;
        {
            timer.Reset();
            auto v = completed;
            Cout << "Copy data complete " <<  timer.Get().SecondsFloat() << Endl;
            timer.Reset();
            {
                auto writer = CreateParallelUnorderedTableWriter<TNode>(client, table2);
                for (auto &row : v) {
                    writer->AddRow(std::move(row));
                }
            }
            auto duration = timer.Get().SecondsFloat();
            sumtime1 += duration;
            Cout << duration << Endl;
        }
        Cout << Endl;
    }
    Cout << Endl;

    Cout << "Writer" << Endl;
    for (int i = 0; i < n; ++i) {
        Cout << "Iteration: " << i << Endl;
        {
            timer.Reset();
            auto v = completed;
            Cout << "Copy data complete " << timer.Get().SecondsFloat() << Endl;
            timer.Reset();
            {
                auto writer = client->CreateTableWriter<TNode>(table2);
                for (auto &row : v) {
                    writer->AddRow(row);
                }
            }
            auto duration = timer.Get().SecondsFloat();
            sumtime2 += duration;
            Cout << duration << Endl;
        }
        Cout << Endl;
    }
    Cout << Endl;

    Cout << "Average time:" << Endl;
    Cout << "ParallelUnorderedWriter: " << sumtime1 / n << Endl;
    Cout << "Writer: " << sumtime2 / n << Endl;
    Cout << Endl << "ParallelUnorderedWriter faster then Writer in " << 1. * sumtime2 / sumtime1 << " times" << Endl;

    return 0;
}
