#include <mapreduce/yt/examples/tutorial/simple_map_ydl/data.ydl.h>

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/common/config.h>

#include <util/stream/output.h>
#include <util/system/user.h>

using namespace NYT;

namespace NData = mapreduce::yt::examples::tutorial::simple_map_ydl::data;

class TComputeEmailsMapper
    : public IMapper<TTableReader<NData::TLoginRecord>, TTableWriter<NData::TEmailRecord>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        for (auto& cursor : *reader) {
            const auto& loginRecord = cursor.GetRow();

            NData::TEmailRecord emailRecord;
            emailRecord.SetName(loginRecord.GetName());
            emailRecord.SetEmail(loginRecord.GetLogin() + "@yandex-team.ru");

            writer->AddRow(emailRecord);
        }
    }
};
REGISTER_MAPPER(TComputeEmailsMapper);

int main(int argc, const char** argv) {
    NYT::Initialize(argc, argv);

    auto client = CreateClient("freud");

    // Выходная табличка у нас будет лежать в tmp и содержать имя текущего пользователя.
    const TString outputTable = "//tmp/" + GetUsername() + "-tutorial-emails-ydl";

    client->Map(
        TMapOperationSpec()
            .AddInput<NData::TLoginRecord>("//home/ermolovd/yt-tutorial/staff_unsorted-ydl")
            .AddOutput<NData::TEmailRecord>(outputTable),
        new TComputeEmailsMapper);

    Cout << "Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path=" << outputTable << Endl;

    return 0;
}

