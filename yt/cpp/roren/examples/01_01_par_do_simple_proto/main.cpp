// Этот пример можно собрать и запустить.

#include <yt/cpp/roren/examples/01_01_par_do_simple_proto/data.pb.h>

#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/yt/yt.h>

#include <util/system/user.h>

int main() {
    NYT::Initialize();

    auto pipeline = NRoren::MakeYtPipeline("freud", "//tmp");
    const TString outputTable = "//tmp/" + GetUsername() + "-roren-tutorial-output";

    pipeline

        // Таблицы YT можно читать в виде protobuf сообщений.
        | NRoren::YtRead<TLoginRecord>("//home/ermolovd/yt-tutorial/staff_unsorted")
        | NRoren::ParDo([] (const TLoginRecord& info) {
            TEmailRecord result;
            result.set_name(info.name());
            result.set_email(info.login() + "@yandex-team.ru");
            return result;
        })

        // Если мы записываем protobuf сообщения, как в этом случае, то указывать схему необязательно.
        // Она будет выведена автоматически.
        | NRoren::YtWrite(outputTable);

    pipeline.Run();

    Cout << "Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path=" << outputTable << Endl;

    return 0;
}
