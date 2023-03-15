#include <yt/cpp/roren/examples/play/resharder/data.pb.h>

#include <yt/cpp/roren/bigrt/bigrt.h>
#include <yt/cpp/roren/interface/type_tag.h>
#include <yt/cpp/roren/interface/fns.h>

#include <bigrt/lib/queue/message_batch/message_batch.h>
#include <ads/bsyeti/libs/events/proto/order_update_time.pb.h>
#include <ads/bsyeti/resharder/lib/rows_processor/row.h>

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/json/json2proto.h>

#include <util/generic/ptr.h>

using namespace NBigRT;
using namespace NRoren;
using NResharder::TRowMeta;

auto ContextTag = TTypeTag<NJson::TJsonValue>("context");
auto OrderTag = TTypeTag<NJson::TJsonValue>("order");
auto OrderUpdateTimeDestinationQueue = NRoren::TTypeTag<NBSYeti::NEvent::TOrderUpdateTime>("order-update-time");

struct TQueueAndShard
{
    TString Queue;
    TString ShardKey;
};

struct TUserRow
{
    TRowMeta RowMeta;
    TMyProto ProtoData;
    std::vector<TQueueAndShard> Destinations;
};

void ChunkSplit(const std::pair<TStringBuf, TRowMeta>& input, IOutput<std::pair<TStringBuf, TRowMeta>>& output);

class TUserParserParDo : public IDoFn<std::pair<TStringBuf, TRowMeta>, TUserRow>
{
public:
    void Start(IOutput<TUserRow>& output)
    {
        Y_UNUSED(output);
    }

    void Do(const std::pair<TStringBuf, TRowMeta>& value, IOutput<TUserRow>& output) override
    {
        TUserRow row;
        row.RowMeta = value.second;
        NProtobufJson::Json2Proto(value.first, row.ProtoData);
        output.Add(row);
    }

    void Finish(IOutput<TUserRow>& output)
    {
        Y_UNUSED(output);
    }
};

struct TDeviceData
{};

struct TCryptaData
{};

const auto MainTag = TTypeTag<TUserRow>("main");
const auto DeviceTag = TTypeTag<TKV<TString, TDeviceData>>("devices");
const auto CryptaTag = TTypeTag<TKV<TString, TCryptaData>>("crypta");

TString ResolveCryptaKey(const TUserRow& userRow);
TString ResolveDeviceKey(const TUserRow& userRow);

void Joiner(NRoren::TMultiInput& multiInput, IOutput<TUserRow>& output)
{
    auto userRow = GetSingleEntry(multiInput.GetInput(MainTag));

    auto maybeCrypta = GetOptionalEntry(multiInput.GetInput(CryptaTag));
    auto maybeDevice = GetOptionalEntry(multiInput.GetInput(DeviceTag));

    // Do something with all that data;

    output.Add(userRow);
}


auto TaxiTag = TTypeTag<TKV<std::pair<TRowMeta, TString>, TTaxiProto>>("taxi-stream");
auto EdaTag = TTypeTag<TKV<std::pair<TRowMeta, TString>, TEdaProto>>("eda-stream");

void SplitToFinalQueues(const TUserRow& userRow, TMultiOutput& output)
{
    auto& taxiOut = output.GetOutput(TaxiTag);
    auto& edaOut = output.GetOutput(EdaTag);

    Y_UNUSED(userRow);

    for (const auto& destination : userRow.Destinations) {
        if (destination.Queue == "taxi") {
            TTaxiProto proto;
            taxiOut.Add({{userRow.RowMeta, destination.ShardKey}, proto});
        } else if (destination.Queue == "eda") {
            TEdaProto proto;
            edaOut.Add({{userRow.RowMeta, destination.ShardKey}, proto});
        }
    }
}

TUserRow UserPreprocess(const TUserRow& userRow);

int main(int argc, const char** argv)
{
    auto pipeline = MakeBigRtResharderPipeline(argc, argv);

    TPDictionary<TKV<TString, TDeviceData>> deviceDict = pipeline.SomehowGetDictionary("name from config or //path/to/some/yt/table");
    TPDictionary<TKV<TString, TCryptaData>> cryptaDict = pipeline.SomehowGetDictionary();

    // Почанковое чтение из LogBroker'а
    TPCollection<std::pair<TStringBuf, TRowMeta>> lbChunks = pipeline.Apply(ReadLbChunk());

    // Разбиение внутри одно LbChunk'а.
    // Точно надо сделать библиотечной функцией, объединённой с чтением из Lb,
    // но я не знаю, существуют ли пользователи, которые хотят пропускать этот этап.
    auto lbSplitChunk = lbChunks | ParDo(ChunkSplit);

    // Пользовательский парсинг отдельных сообщений, возможно какой-то препроцессинг.
    auto parsed = lbSplitChunk | ParDo(MakeIntrusive<TUserParserParDo>());

    // Лукапы во внешние дин.таблицы.
    // Планируется, что решардер будет собирать ключи со всего батча, делать лукап,
    // потом выполнять пользовательский код.
    TPCollection<TUserRow> joined = parsed | MultiDictJoin({
        .JoinFn = Joiner, // Пользовательская функция обрабатывающая результаты лукапов.
        .MainTag = MainTag,
        .Dictionaries = {
            {
                .Dictionary = cryptaDict,
                .KeyGetter = ResolveCryptaKey,
                .Tag = CryptaTag,
            },
            {
                .Dictionary = deviceDict,
                .KeyGetter = ResolveDeviceKey,
                .Tag = DeviceTag,
            }
    }});

    auto splitted = joined | ParDo(SplitToFinalQueues, {TaxiTag, EdaTag});

    const auto& [taxiStream, edaStream] = splitted.Unpack(TaxiTag, EdaTag);

    // Запись в выходные очереди, число шардов определяется конфигом.
    // Номер шарда вычисляется из строкового ключа какой-то функцией по умолчанию, которую можно переопределить.
    edaStream | BigRtWriteSharded("eda-queue");
    taxiStream | BigRtWriteSharded("taxi-queue");

    pipeline.Run();
}

/*
struct TRow
{
    TInstant Timestamp;
    TRowMeta RowMeta_;
    TUniversalProto Proto;
    std::vector<TQueueAndShard> Destinations_;
};

class TReshardSwitcherFunction : public IDoFn<NJson::TJsonValue, NRoren::TMultiRow>
{
public:
    void Do(const NJson::TJsonValue& input, TMultiOutput& output) override
    {
        for (const auto& item : input.GetArray()) {
            const auto& level = item["level"].GetString();

            if (level == "CONTEXT") {
                ParseContextLevel(item, output);
            } else {
                ParseOrderLevel(item);
            }
        }
    }

private:
    void ParseContextLevel(const NJson::TJsonValue& item, TMultiOutput& output) {
        const ui64 unixTime = item["actual_in_direct_db_at"].GetUIntegerRobust();
        const auto timestamp = TInstant::Seconds(unixTime);
        Y_ENSURE(TInstant::Zero() != timestamp, "Missing TimeStamp!");

        const i32 orderID = item["OrderID"].GetIntegerRobust();
        const ui64 iterID = item["iter_id"].GetUIntegerRobust();
        const ui32 engineID = item["EngineID"].GetUIntegerRobust();
        const bool reexportFlag = item["full_export_flag"].GetIntegerRobust() > 0;
        if (HasBannerDestinationQueue_) {
            if (!reexportFlag) {
                NBSYeti::NEvent::TOrderUpdateTime message;
                message.SetUpdateTime(unixTime);
                output.GetOutput(OrderUpdateTimeDestinationQueue).Add(message);
            }
        }
    }

    void ParseOrderLevel(const NJson::TJsonValue& item) {
    }

    static NBSYeti::NEvent::TOrderUpdateTime
    ParseOrdersUpdateTime(ui64 orderID, ui64 updateTime, TInstant ts, TString& profileId)
    {
    }

private:
    bool HasBannerDestinationQueue_;
};
*/
