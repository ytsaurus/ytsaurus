#include "log_line_parser.h"

#include <util/string/split.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

int SeverityRank(TStringBuf level)
{
    if (level == "info") {
        return 0;
    }
    if (level == "warning") {
        return 1;
    }
    if (level == "error") {
        return 2;
    }
    return -1;
}

std::string SeverityName(int rank)
{
    switch (rank) {
        case 0:
            return "info";
        case 1:
            return "warning";
        case 2:
            return "error";
        default:
            return {};
    }
}

std::vector<TLogRecord> ParseLogLine(const std::string& line)
{
    std::vector<TLogRecord> records;
    for (const auto& entry : StringSplitter(line).Split(';').SkipEmpty()) {
        TStringBuf token = entry.Token();
        auto separator = token.find(':');
        if (separator == TStringBuf::npos) {
            continue;
        }
        auto level = token.substr(0, separator);
        auto text = token.substr(separator + 1);
        if (text.empty() || SeverityRank(level) < 0) {
            continue;
        }
        records.push_back({.Level = std::string(level), .Text = std::string(text)});
    }
    return records;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
