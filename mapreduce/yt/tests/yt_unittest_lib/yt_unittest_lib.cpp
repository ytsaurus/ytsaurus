#include "yt_unittest_lib.h"

#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/debug_metrics.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/interface/client.h>

#include <util/system/env.h>

#include <util/random/fast.h>

////////////////////////////////////////////////////////////////////

template<>
void Out<NYT::TNode>(IOutputStream& s, const NYT::TNode& node)
{
    s << "TNode:" << NodeToYsonString(node);
}

template<>
void Out<TGUID>(IOutputStream& s, const TGUID& guid)
{
    s << GetGuidAsString(guid);
}

template <>
void Out<NYT::NTesting::TOwningYaMRRow>(IOutputStream& out, const NYT::NTesting::TOwningYaMRRow& row) {
    out << "Row{" << row.Key << ", " << row.SubKey << ", " << row.Value << "}";
}

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NTesting {

////////////////////////////////////////////////////////////////////

IClientPtr CreateTestClient()
{
    TString ytProxy = GetEnv("YT_PROXY");
    if (ytProxy.empty()) {
        ythrow yexception() << "YT_PROXY env variable must be set";
    }
    auto client = CreateClient(ytProxy);
    client->Remove("//testing", TRemoveOptions().Recursive(true).Force(true));
    client->Create("//testing", ENodeType::NT_MAP, TCreateOptions());
    return client;
}

TString GenerateRandomData(size_t size, ui64 seed)
{
    TReallyFastRng32 rng(seed);

    TString result;
    result.reserve(size + sizeof(ui64));
    while (result.size() < size) {
        ui64 value = rng.GenRand64();
        result += TStringBuf(reinterpret_cast<const char*>(&value), sizeof(value));
    }

    result.resize(size);

    return result;
}

TVector<TNode> ReadTable(const IClientBasePtr& client, const TString& tablePath)
{
    TVector<TNode> result;
    auto reader = client->CreateTableReader<TNode>(tablePath);
    for (; reader->IsValid(); reader->Next()) {
        result.push_back(reader->GetRow());
    }
    return result;
}

////////////////////////////////////////////////////////////////////

TZeroWaitLockPollIntervalGuard::TZeroWaitLockPollIntervalGuard()
    : OldWaitLockPollInterval_(TConfig::Get()->WaitLockPollInterval)
{
    TConfig::Get()->WaitLockPollInterval = TDuration::Zero();
}

TZeroWaitLockPollIntervalGuard::~TZeroWaitLockPollIntervalGuard()
{
    TConfig::Get()->WaitLockPollInterval = OldWaitLockPollInterval_;
}

////////////////////////////////////////////////////////////////////////////////

TConfigSaverGuard::TConfigSaverGuard()
    : Config_(*TConfig::Get())
{ }

TConfigSaverGuard::~TConfigSaverGuard()
{
    *TConfig::Get() = Config_;
}

////////////////////////////////////////////////////////////////////////////////

TDebugMetricDiff::TDebugMetricDiff(TString name)
    : Name_(std::move(name))
    , InitialValue_(NDetail::GetDebugMetric(Name_))
{ }

ui64 TDebugMetricDiff::GetTotal() const
{
    return NDetail::GetDebugMetric(Name_) - InitialValue_;
}

////////////////////////////////////////////////////////////////////////////////

TOwningYaMRRow::TOwningYaMRRow(const TYaMRRow& row)
    : Key(row.Key.ToString())
    , SubKey(row.SubKey.ToString())
    , Value(row.Value.ToString())
{}

TOwningYaMRRow::TOwningYaMRRow(TString key, TString subKey, TString value)
    : Key(std::move(key))
    , SubKey(std::move(subKey))
    , Value(std::move(value))
{ }

bool operator == (const TOwningYaMRRow& row1, const TOwningYaMRRow& row2) {
    return row1.Key == row2.Key
        && row1.SubKey == row2.SubKey
        && row1.Value == row2.Value;
}

////////////////////////////////////////////////////////////////////////////////

TTabletFixture::TTabletFixture()
    : Client_(CreateTestClient())
{
    WaitForTabletCell();
}

IClientPtr TTabletFixture::Client()
{
    return Client_;
}

void TTabletFixture::WaitForTabletCell()
{
    const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
    while (TInstant::Now() < deadline) {
        auto tabletCellList = Client()->List(
            "//sys/tablet_cells",
            TListOptions().AttributeFilter(
                TAttributeFilter().AddAttribute("health")));
        if (!tabletCellList.empty()) {
            bool good = true;
            for (const auto& tabletCell : tabletCellList) {
                const auto health = tabletCell.GetAttributes()["health"].AsString();
                if (health != "good") {
                    good = false;
                    break;
                }
            }
            if (good) {
                return;
            }
        }
        Sleep(TDuration::MilliSeconds(100));
    }
    ythrow yexception() << "WaitForTabletCell timeout";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTesting
} // namespace NYT
