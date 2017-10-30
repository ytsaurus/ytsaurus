#pragma once

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/common/config.h>
#include <util/datetime/base.h>

////////////////////////////////////////////////////////////////////////////////

template<>
void Out<NYT::TNode>(IOutputStream& s, const NYT::TNode& node);

template<>
void Out<TGUID>(IOutputStream& s, const TGUID& guid);

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NTesting {

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateTestClient();

////////////////////////////////////////////////////////////////////////////////

class TZeroWaitLockPollIntervalGuard
{
public:
    TZeroWaitLockPollIntervalGuard();

    ~TZeroWaitLockPollIntervalGuard();

private:
    TDuration OldWaitLockPollInterval_;
};

////////////////////////////////////////////////////////////////////////////////

class TConfigSaverGuard
{
public:
    TConfigSaverGuard();
    ~TConfigSaverGuard();

private:
    TConfig Config_;
};

////////////////////////////////////////////////////////////////////////////////

class TDebugMetricDiff
{
public:
    TDebugMetricDiff(TString name);
    ui64 GetTotal() const;

private:
    TString Name_;
    ui64 InitialValue_;
};

////////////////////////////////////////////////////////////////////////////////

struct TOwningYaMRRow {
    TString Key;
    TString SubKey;
    TString Value;

    TOwningYaMRRow(const TYaMRRow& row);
    TOwningYaMRRow(TString key, TString subKey, TString value);
};

bool operator == (const TOwningYaMRRow& row1, const TOwningYaMRRow& row2);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTesting
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

template <>
void Out<NYT::NTesting::TOwningYaMRRow>(IOutputStream& out, const NYT::NTesting::TOwningYaMRRow& row);

////////////////////////////////////////////////////////////////////////////////

