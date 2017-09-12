#include <mapreduce/yt/interface/client.h>
#include <util/system/user.h>

using namespace NYT;

int main(int argc, const char** argv) {
    NYT::Initialize(argc, argv);

    auto client = CreateClient("freud");

    const TString outputTable = "//tmp/" + GetUsername() + "-tutorial-statistics";

    auto operation = client->Sort(
        TSortOperationSpec()
            .AddInput("//home/ermolovd/yt-tutorial/staff_unsorted")
            .Output(outputTable)
            .SortBy({"login"}));

    auto jobStatistics = operation->GetJobStatistics();
    Cout << "time/total: " << jobStatistics.GetStatisticsAs<TDuration>("time/total").Sum() << Endl;
    Cout << "data/input/row_count: " << jobStatistics.GetStatistics("data/input/row_count").Sum() << Endl;
    return 0;
}
