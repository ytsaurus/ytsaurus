#pragma once

#include <mapreduce/yt/interface/fwd.h>

#include <util/generic/stroka.h>
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
void Out<NYT::NTest::TOwningYaMRRow>(TOutputStream& out, const NYT::NTest::TOwningYaMRRow& row);

////////////////////////////////////////////////////////////////////////////////
