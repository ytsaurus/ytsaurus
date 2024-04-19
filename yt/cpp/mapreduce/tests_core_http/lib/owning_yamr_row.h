#pragma once

#include <yt/cpp/mapreduce/interface/fwd.h>

#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NYT {
namespace NTest {

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

} // namespace NTest
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

template <>
void Out<NYT::NTest::TOwningYaMRRow>(IOutputStream& out, const NYT::NTest::TOwningYaMRRow& row);

////////////////////////////////////////////////////////////////////////////////
