#include <yt/cpp/mapreduce/examples/tutorial/mapreduce_lambda/data.pb.h>

#include <yt/cpp/mapreduce/library/lambda/yt_lambda.h>

#include <util/stream/output.h>
#include <util/system/user.h>
#include <util/charset/utf8.h>

using namespace NYT;

int main() {
    NYT::Initialize();

    auto client = CreateClient("freud");

    // Эта таблица будет с некоторой вероятностью не отсортирована.
    // Если вместо MapReduce() использовать MapReduceSorted(), тогда
    // она будет дополнительно досортирована по "name" и это будет отражено в схеме
    const TString outputTable = "//tmp/" + GetUsername() + "-tutorial-output";

    MapReduce<TLoginRecord, TLoginRecord, TNameStatistics>(
        client,
        "//home/dev/tutorial/staff_unsorted",
        outputTable,
        {"name"}, // список ключей, по которым мы будем редьюсить
        [](auto& src, auto& dst) { // mapper
            dst.SetName(ToLowerUTF8(src.GetName()));
            return true;
        },
        [](auto& /*src*/, auto& dst) { // reducer
            // dst.SetName() не вызываем, т.к. когда по
            // "name" редьюсим, это поле уже заполнено.

            // так можно делать, т.к. конструктор протобуфа dst
            // проинициализирует .Count в 0 (по умолчанию):
            dst.SetCount(dst.GetCount() + 1);
        });

    Cout << "Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path=" << outputTable << Endl;

    return 0;
}
