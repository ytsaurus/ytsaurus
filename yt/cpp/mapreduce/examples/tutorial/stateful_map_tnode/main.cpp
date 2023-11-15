#include <yt/cpp/mapreduce/interface/client.h>

#include <library/cpp/string_utils/levenshtein_diff/levenshtein_diff.h>

#include <util/stream/output.h>
#include <util/system/user.h>

using namespace NYT;

class TFilterMapper
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    Y_SAVELOAD_JOB(Pattern_, MaxDistance_); // Заклинание, которое говорит, какие переменные нужно передавать на сервер.

    TFilterMapper() = default; // У джобы обязательно должен быть конструктор по умолчанию

    TFilterMapper(TString pattern, double maxDistance)
        : Pattern_(std::move(pattern))
        , MaxDistance_(maxDistance)
    { }


    void Do(TReader* reader, TWriter* writer) override {
        for (auto& cursor : *reader) {
            const auto& row = cursor.GetRow();
            const auto& name = row["name"].AsString();
            if (NLevenshtein::Distance(name, Pattern_) <= MaxDistance_) {
                writer->AddRow(row);
            }
        }
    }

private:
    TString Pattern_;
    size_t MaxDistance_ = 0;
};
REGISTER_MAPPER(TFilterMapper)

int main() {
    NYT::Initialize();

    auto client = CreateClient("freud");

    const TString outputTable = "//tmp/" + GetUsername() + "-tutorial-output-statefull-map";

    client->Map(
        TMapOperationSpec()
        .AddInput<TNode>("//home/tutorial/staff_unsorted")
        .AddOutput<TNode>(outputTable),
        new TFilterMapper("Arkady", 2)); // Мы создаём объект TFilterMapper, и конструктор заполняет поля Pattern_ и MaxDistance_.
                                         // Библиотека сериализует поля, указанные в Y_SAVELOAD_JOB, и загружает их на сервер.
                                         // На сервере вызывается конструктор по умолчанию и сериализованные поля загружаются.

    Cout << "Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path=" << outputTable << Endl;

    return 0;
}
