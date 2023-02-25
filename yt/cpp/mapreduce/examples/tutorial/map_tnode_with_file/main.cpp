#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/io.h>

#include <util/generic/hash_set.h>
#include <util/stream/output.h>
#include <util/system/user.h>

using namespace NYT;

class TFilterRobotsMap
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* loginReader, TWriter* writer) override {
        // Если мы прикрепляли табличку //path/to/table, то в операции мы будем видеть её под именем table
        TIFStream stream("is_robot_unsorted");
        auto isRobotReader = CreateTableReader<TNode>(&stream);
        THashSet<i64> robotIds;
        for (auto& cursor : *isRobotReader) {
            const auto& curRow = cursor.GetRow();
            if (curRow["is_robot"].AsBool()) {
                robotIds.insert(curRow["uid"].AsInt64());
            }
        }

        for (auto& cursor : *loginReader) {
            const auto& curRow = cursor.GetRow();
            if (robotIds.contains(curRow["uid"].AsInt64())) {
                writer->AddRow(curRow);
            }
        }
    }
};
REGISTER_MAPPER(TFilterRobotsMap)

int main() {
    NYT::Initialize();

    auto client = CreateClient("freud");


    const TString loginTable = "//home/dev/tutorial/staff_unsorted";
    const TString isRobotTable = "//home/dev/tutorial/is_robot_unsorted";
    const TString outputTable = "//tmp/" + GetUsername() + "-tutorial-robots";

    client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(loginTable)
            .MapperSpec(TUserJobSpec()
                .AddFile(TRichYPath(isRobotTable).Format("yson"))) // Таблицу с роботами добавляем в виде файла
            .AddOutput<TNode>(outputTable),
        new TFilterRobotsMap);

    Cout << "Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path=" << outputTable << Endl;
    return 0;
}
