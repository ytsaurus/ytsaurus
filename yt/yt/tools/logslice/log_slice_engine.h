#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/stream/output.h>
#include <util/system/file.h>

#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace NYT::NLogSlice {

////////////////////////////////////////////////////////////////////////////////

//! Compression codec of a log file.
enum class ECompressionCodec
{
    //! Detect the codec from the file name extension.
    Auto,
    //! zstd-compressed log (".zst"), as produced by TZstdLogCompressionCodec.
    Zstd,
    //! gzip-compressed log (".gz"), as produced by TRandomAccessGZipFile.
    Gzip,
    //! Uncompressed plain-text log (e.g. ".log").
    PlainText,
};

//! Parses a codec name accepted by the command line: "auto", "zstd", "gzip" or
//! "plain" (with a few common aliases). Throws on an unknown name.
ECompressionCodec ParseCompressionCodec(TStringBuf name);

//! Chooses a codec for [fileName] based on its extension: ".zst" -> Zstd,
//! ".gz" -> Gzip, everything else -> PlainText.
ECompressionCodec DetectCompressionCodec(TStringBuf fileName);

////////////////////////////////////////////////////////////////////////////////

//! A pluggable engine that extracts the lines of a log file within a time window.
struct ILogSliceEngine
{
    virtual ~ILogSliceEngine() = default;

    //! Writes every plain-text log line whose timestamp lies within the inclusive
    //! window [from, to] to [output].
    virtual void Slice(TFile& file, TInstant from, TInstant to, IOutputStream& output) = 0;

    //! Returns the timestamps of the first and last timestamped log lines of [file],
    //! or null if the file is empty or contains no timestamped lines.
    virtual std::optional<std::pair<TInstant, TInstant>> GetTimeRange(TFile& file) = 0;
};

using ILogSliceEnginePtr = std::unique_ptr<ILogSliceEngine>;

//! Creates the slice engine for [codec]. When [codec] is Auto, the codec is
//! detected from [fileName] via DetectCompressionCodec.
ILogSliceEnginePtr CreateLogSliceEngine(ECompressionCodec codec, TStringBuf fileName);

////////////////////////////////////////////////////////////////////////////////

//! Splits a single command-line string into argv-style tokens, honoring single
//! and double quotes (so a multi-word grep pattern can be passed as one -g value)
//! and backslash escapes. Throws on an unbalanced quote.
std::vector<TString> SplitCommandLine(TStringBuf line);

//! Runs the system `grep` with [grepArgs] over [input], feeding [input] on grep's
//! stdin and returning its stdout. An empty result (grep exit code 1, "no match")
//! is returned as an empty string; a genuine grep failure throws.
TString FilterWithGrep(const std::vector<TString>& grepArgs, const TString& input);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogSlice
