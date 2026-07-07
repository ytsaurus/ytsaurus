#pragma once

#include <util/datetime/base.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <optional>
#include <string>

namespace NYT::NLogSlice {

////////////////////////////////////////////////////////////////////////////////

//! Parses a user-supplied query time into an absolute instant.
/*!
 *  Supported formats:
 *    - "now"                          -- the current instant;
 *    - "14:30"                        -- today's date at HH:MM (local/Moscow time);
 *    - "12:23:34"                     -- today's date at HH:MM:SS (local/Moscow time);
 *    - "2018-11-09"                   -- a bare date, i.e. that day's midnight in local/Moscow time;
 *    - "2018-11-09 05"                -- date and hour in local/Moscow time;
 *    - "2018-11-09 05:10"             -- date down to the minute in local/Moscow time;
 *    - "2018-11-09 05:10:43"          -- full timestamp in local/Moscow time;
 *    - "2018-11-09 05:10:43,341345"   -- full timestamp in local/Moscow time, with fraction of a second;
 *    - "16 Nov 2018 13:56:14"         -- full timestamp in local/Moscow time (web UI format);
 *    - "2019-09-19T11:46:04.848360Z"  -- full timestamp in UTC (YT server-side format).
 *
 *  For the bare-date and date-and-hour/minute forms the absent finer fields
 *  default to zero.
 *
 *  Throws on an unrecognized format.
 */
TInstant ParseQueryTime(TStringBuf input);

//! Parses the leading timestamp of a plain-text YT log line.
/*!
 *  The expected prefix is "YYYY-MM-DD HH:MM:SS,uuuuuu" interpreted in local
 *  (Moscow) time -- exactly the format produced by TPlainTextEventFormatter.
 *  Returns null if the line does not start with a valid timestamp.
 */
std::optional<TInstant> ParseLogLineTime(TStringBuf line);

//! Formats [instant] as "YYYY-MM-DD HH:MM:SS,uuuuuu" in local (Moscow) time --
//! the exact log-line timestamp format and the inverse of ParseLogLineTime.
std::string FormatLogTime(TInstant instant);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogSlice
