#include <yt/cpp/mapreduce/interface/client.h>

#include <library/cpp/regex/pcre/regexp.h>

#include <util/system/user.h>

using namespace NYT;

class TFilterVideoRegexp
    : public IReducer<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        // Так же как и с обычным Reducer'ом в каждый вызов метода Do
        // придут записи с общим JoinBy ключом.
        TMaybe<TRegExMatch> regex;
        for (auto& cursor : *reader) {
            auto row = cursor.GetRow();
            if (cursor.GetTableIndex() == 0) { // таблица с хостами
                const auto videoRegexp = row["video_regexp"].AsString();

                // Дебажная печать, stderr можно будет посмотреть в web интерфейсе
                Cerr << "Processing host: " << row["host"].AsString() << Endl;
                if (!videoRegexp.empty()) {
                    regex = TRegExMatch(videoRegexp);
                }
            } else { // таблица с урлами
                if (regex && regex->Match(row["path"].AsString().c_str())) {
                    writer->AddRow(row);
                }
            }
        }
    }
};
REGISTER_REDUCER(TFilterVideoRegexp);

int main() {
    NYT::Initialize();

    auto client = CreateClient("freud");

    const TString outputTable = "//tmp/" + GetUsername() + "-tutorial-join-reduce";

    client->Reduce(
        TReduceOperationSpec()
        .JoinBy({"host"})
        .AddInput<TNode>(
            TRichYPath("//home/dev/tutorial/host_video_regexp")
            .Foreign(true)) // важно отметить хостовую таблицу как foreign
        .AddInput<TNode>("//home/dev/tutorial/doc_title")
        .AddOutput<TNode>(outputTable)
        .EnableKeyGuarantee(false),
        new TFilterVideoRegexp);

    Cout << "Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path=" << outputTable << Endl;

    return 0;
}
