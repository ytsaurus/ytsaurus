#include <yt/cpp/mapreduce/interface/client.h>
#include <util/stream/output.h>
#include <util/system/user.h>

using namespace NYT;

class TComputeEmailsMapper
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        for (auto& cursor : *reader) {
            const auto& row = cursor.GetRow();

            TNode outRow;
            outRow["name"] = row["name"].AsString();
            outRow["email"] = row["login"].AsString() + "@yandex-team.ru";

            for (int i = 0; i < 50; ++i) {
                AllNames_.push_back(row["name"].AsString() + ToString(i));
            }

            writer->AddRow(outRow);
        }
    }

private:
    std::vector<TString> AllNames_;
};
REGISTER_MAPPER(TComputeEmailsMapper)

int main() {
    Initialize();

    auto client = CreateClient("freud");

    const TString outputTable = "//tmp/" + GetUsername() + "-tutorial-emails";

    auto op = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>("//home/tutorial/staff_unsorted")
            .AddOutput<TNode>(outputTable)
            .MapperSpec(TUserJobSpec()
                .AddJobProfiler(TJobProfilerSpec()
                    // Указываем, что профилировться будет пользовательский код.
                    .ProfilingBinary(EProfilingBinary::UserJob)
                    // Указываем, что хотим профилировать потребление памяти.
                    .ProfilerType(EProfilerType::Memory)
                    // Указывем, что профилироваться будут все джобы.
                    // ВАЖНО! При большом количестве джобов операции необходимо указывать
                    // более низкую вероятность, чтобы предотвратить перегрузку архива операций.
                    .ProfilingProbability(1.0))),
        new TComputeEmailsMapper);

    Cout << "Operation: " << op->GetWebInterfaceUrl() << Endl;
    Cout << "Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path=" << outputTable << Endl;

    return 0;
}
