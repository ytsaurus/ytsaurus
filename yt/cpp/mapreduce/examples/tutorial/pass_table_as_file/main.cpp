#include <yt/cpp/mapreduce/interface/client.h>

#include <util/generic/hash_set.h>
#include <util/stream/output.h>
#include <util/system/user.h>

using namespace NYT;

class TFilterRobotsMap
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    // Мы читаем дамп таблицы в методе Start, перед тем как обработать первую запись в Map-таблице.
    // Настройка доставки таблицы в джобу происходит в функции main.
    void Start(TWriter*) override {
        // Нам в джобу доставили файлик "robot_table" с дампом таблицы.
        TFileInput tableDump("robot_table");

        // Есть функция CreateTableReader<>, которая умеет создавать читателя из любого IInputStream'а.
        // Созданный читатель имеет интерфейс аналогичный другим читателям.
        auto reader = CreateTableReader<TNode>(&tableDump);
        for (auto& cursor : *reader) {
            const auto& curRow = cursor.GetRow();
            if (curRow["is_robot"].AsBool()) {
                RobotUids.insert(curRow["uid"].AsInt64());
            }
        }
    }

    void Do(TReader* reader, TWriter* writer) override {
        for (auto& cursor : *reader) {
            const auto& curRow = cursor.GetRow();
            if (RobotUids.contains(curRow["uid"].AsInt64())) {
                writer->AddRow(curRow);
            }
        }
    }

private:
    THashSet<i64> RobotUids;
};
REGISTER_MAPPER(TFilterRobotsMap)

int main() {
    NYT::Initialize();

    auto client = CreateClient("freud");

    const TString outputTable = "//tmp/" + GetUsername() + "-tutorial-robots";

    client->Map(
        TMapOperationSpec()
            .MapperSpec(
                TUserJobSpec()
                // Самое интересное -- мы просим YT доставить нам табличку в виде файла.
                .AddFile(
                    TRichYPath("//home/dev/tutorial/is_robot_unsorted") // Тут указываем таблицу, которую нам надо доставить.
                    .Format("yson") // Это формат, в котором таблица будет прочитана, нам нужен yson, чтобы TNode-читатель в джобе смог прочитать файл.
                    .FileName("robot_table") // Это имя файла, с дампом таблицы.
                    // Тут же можно было бы указать фильтрацию по колонкам или фильтрацию по номерам строк таблицы
                    // с помощью соответсвтующих методов TRichYPath, но нам ничего этого не надо.
                ))
            .AddInput<TNode>("//home/dev/tutorial/staff_unsorted")
            .AddOutput<TNode>(outputTable),
        new TFilterRobotsMap);

    Cout << "Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path=" << outputTable << Endl;
    return 0;
}
