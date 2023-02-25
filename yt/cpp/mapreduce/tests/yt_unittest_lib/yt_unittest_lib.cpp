#include "yt_unittest_lib.h"

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/common/debug_metrics.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/error_codes.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <yt/cpp/mapreduce/client/client.h>

#include <util/stream/printf.h>

#include <util/system/env.h>
#include <util/system/hostname.h>

#include <util/random/fast.h>

////////////////////////////////////////////////////////////////////

template<>
void Out<NYT::TNode>(IOutputStream& s, const NYT::TNode& node)
{
    s << "TNode:" << NodeToCanonicalYsonString(node);
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

static void VerifyLocalMode(TStringBuf proxy, const IClientBasePtr& client)
{
    static const TVector<TString> AllowedClusters = {"socrates"};
    if (std::find(begin(AllowedClusters), end(AllowedClusters), proxy) != end(AllowedClusters)) {
        return;
    }

    TString fqdn;
    try {
        fqdn = client->Get("//sys/@local_mode_fqdn").AsString();
    } catch (const TErrorResponse& error) {
        Y_FAIL("Attribute //sys/@local_mode_fqdn not found; are you trying to run tests on a real cluster?");
    }
    Y_ENSURE(
        fqdn == ::FQDNHostName(),
        "FQDN from cluster differs from host name: " << fqdn << ' ' << ::FQDNHostName()
            << "; are you trying to run tests on a real cluster?");
}

IClientPtr CreateTestClient(TString proxy, const TCreateClientOptions& options)
{
    if (proxy.empty()) {
        proxy = GetEnv("YT_PROXY");
    }
    Y_ENSURE(!proxy.empty(), "YT_PROXY env variable must be set or 'proxy' argument nonempty");
    auto client = CreateClient(proxy, options);
    VerifyLocalMode(proxy, client);
    client->Remove("//testing", TRemoveOptions().Recursive(true).Force(true));
    client->Create("//testing", ENodeType::NT_MAP, TCreateOptions());
    return client;
}

TYPath CreateTestDirectory(const IClientBasePtr& client)
{
    TYPath path = "//testing-dir/" + CreateGuidAsString();
    client->Remove(path, TRemoveOptions().Recursive(true).Force(true));
    client->Create(path, ENodeType::NT_MAP, TCreateOptions().Recursive(true));
    return path;
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
    : Key(row.Key)
    , SubKey(row.SubKey)
    , Value(row.Value)
{}

TOwningYaMRRow::TOwningYaMRRow(TString key, TString subKey, TString value)
    : Key(std::move(key))
    , SubKey(std::move(subKey))
    , Value(std::move(value))
{ }

TOwningYaMRRow::operator TYaMRRow() const
{
    return {Key, SubKey, Value};
}

bool operator == (const TOwningYaMRRow& row1, const TOwningYaMRRow& row2) {
    return row1.Key == row2.Key
        && row1.SubKey == row2.SubKey
        && row1.Value == row2.Value;
}

////////////////////////////////////////////////////////////////////////////////

TTestFixture::TTestFixture(const TCreateClientOptions& options)
    : Client_(CreateClient(options))
    , WorkingDir_(CreateTestDirectory(Client_))
{ }

TTestFixture::~TTestFixture()
{
    YT_LOG_INFO("Completing test and aborting all operations");
    while (true) {
        auto result = Client_->ListOperations(
            TListOperationsOptions()
                .State("running")
                .Limit(100));
        for (const auto& op : result.Operations) {
            Y_VERIFY(op.Id);
            try {
                Client_->AttachOperation(*op.Id)->AbortOperation();
            } catch (const TErrorResponse& ex) {
                if (ex.GetError().ContainsErrorCode(NClusterErrorCodes::NScheduler::NoSuchOperation)) {
                    YT_LOG_ERROR("Error aborting operation %v: %v",
                        *op.Id,
                        ex.what());
                } else {
                    Y_FAIL("Unexpected error: %s", ex.what());
                }
            }
        }
        if (!result.Incomplete) {
            break;
        }
    }
}

IClientPtr TTestFixture::GetClient() const
{
    return Client_;
}

IClientPtr TTestFixture::CreateClient(const TCreateClientOptions& options) const
{
    return CreateTestClient("", options);
}

IClientPtr TTestFixture::CreateClientForUser(const TString& user, TCreateClientOptions options)
{
    TString token = CreateGuidAsString();
    if (!Client_->Exists("//sys/users/" + user)) {
        Client_->Create("", NT_USER,
            TCreateOptions().Attributes(TNode()("name", user)));
    }
    Client_->Set("//sys/tokens/" + token, user);
    return CreateTestClient("", options.Token(token));
}

TYPath TTestFixture::GetWorkingDir() const
{
    return WorkingDir_;
}

////////////////////////////////////////////////////////////////////////////////

TTabletFixture::TTabletFixture()
{
    WaitForTabletCell();
}

void TTabletFixture::WaitForTabletCell()
{
    const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
    while (TInstant::Now() < deadline) {
        auto tabletCellList = GetClient()->List(
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

bool AreSchemasEqual(const TTableSchema& lhs, const TTableSchema& rhs)
{
    if (lhs.Columns().size() != rhs.Columns().size()) {
        return false;
    }
    for (int i = 0; i < static_cast<int>(lhs.Columns().size()); ++i) {
        if (lhs.Columns()[i].Name() != rhs.Columns()[i].Name()) {
            return false;
        }
        if (lhs.Columns()[i].Type() != rhs.Columns()[i].Type()) {
            return false;
        }
    }
    return true;
}

void WaitForPredicate(const std::function<bool()>& predicate, TDuration timeout)
{
    auto deadline = TInstant::Now() + timeout;
    while (true) {
        if (predicate()) {
            return;
        }
        if (TInstant::Now() > deadline) {
            ythrow TWaitFailedException();
        }
        ::Sleep(TDuration::MilliSeconds(100));
    }
}

////////////////////////////////////////////////////////////////////////////////

TStreamTeeLogger::TStreamTeeLogger(ELevel cutLevel, IOutputStream* stream, ILoggerPtr oldLogger)
    : OldLogger_(oldLogger)
    , Stream_(stream)
    , Level_(cutLevel)
{ }

void TStreamTeeLogger::Log(ELevel level, const ::TSourceLocation& sourceLocation, const char* format, va_list args)
{
    OldLogger_->Log(level, sourceLocation, format, args);
    if (level <= Level_) {
        Printf(*Stream_, format, args);
        *Stream_ << Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTesting
} // namespace NYT
