#pragma once

#include <string>
#include <vector>

#include <util/generic/strbuf.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

struct TLogRecord
{
    std::string Level;
    std::string Text;

    bool operator==(const TLogRecord&) const = default;
};

int SeverityRank(TStringBuf level);

std::string SeverityName(int rank);

std::vector<TLogRecord> ParseLogLine(const std::string& line);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
