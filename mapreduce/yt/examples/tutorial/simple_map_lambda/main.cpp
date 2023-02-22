#include <mapreduce/yt/examples/tutorial/simple_map_lambda/data.pb.h>

#include <mapreduce/yt/library/lambda/yt_lambda.h>
#include <mapreduce/yt/interface/config.h>

#include <util/stream/output.h>
#include <util/system/user.h>

struct TGlobalSettings {
    TString MailSuffix;
    Y_SAVELOAD_DEFINE(MailSuffix);
};
NYT::TSaveable<TGlobalSettings> GlobalSettings;

int main() {
    NYT::Initialize();

    auto client = NYT::CreateClient("freud");

    // Выходная табличка у нас будет лежать в tmp и содержать имя текущего пользователя.
    auto outputTable = NYT::TRichYPath("//tmp/" + GetUsername() + "-tutorial-emails-protobuf")
        .CompressionCodec("zlib_9");

    // TransformCopyIf запоминает содержимое GlobalSettings на момент своего запуска
    // и восстанавливает его в job'ах, в которых вызывается лямбда.
    // Этот способ требует аккуратности, если операции запускать из разных потоков.
    GlobalSettings.MailSuffix = "@yandex-team.ru";

    NYT::TransformCopyIf<TLoginRecord, TEmailRecord>(
        client,
        "//home/dev/tutorial/staff_unsorted",
        outputTable,
        [](auto& src, auto& dst) {
            dst.SetName(src.GetName());
            dst.SetEmail(src.GetLogin() + GlobalSettings.MailSuffix);
            return true;
        });

    Cout << "Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path=" << outputTable.Path_ << Endl;

    return 0;
}
