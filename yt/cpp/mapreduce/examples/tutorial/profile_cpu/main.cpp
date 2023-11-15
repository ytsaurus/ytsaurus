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
            outRow["name"] = ImproveName(row["name"].AsString());
            outRow["email"] = row["login"].AsString() + "@yandex-team.ru";

            writer->AddRow(outRow);
        }
    }

private:
    TString ImproveName(const TString& name)
    {
        for (int i = 0; ; i++) {
            auto newName = name + ToString(i);
            // Lucky name!
            if (CalcHash(newName) % 1000 == 0) {
                return newName;
            }
        }

        return name;
    }

    i64 CalcHash(const TString& string)
    {
        constexpr i64 P = 1337;
        constexpr int Q = 479001599;
        i64 value = 0;
        for (auto symbol : string) {
            value = (value * P + symbol) % Q;

            for (int i = 0; i < 10; i++) {
                value = value + i;
                value = value ^ P;
                value = value * 17;
                value = value % Q;
            }
        }

        return value;
    }
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
                    // Указываем, что хотим профилировать потребление CPU.
                    .ProfilerType(EProfilerType::Cpu)
                    // Указывем, что профилироваться будут все джобы.
                    // ВАЖНО! При большом количестве джобов операции необходимо указывать
                    // более низкую вероятность, чтобы предотвратить перегрузку архива операций.
                    .ProfilingProbability(1.0))),
        new TComputeEmailsMapper);

    Cout << "Operation: " << op->GetWebInterfaceUrl() << Endl;
    Cout << "Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path=" << outputTable << Endl;

    return 0;
}
