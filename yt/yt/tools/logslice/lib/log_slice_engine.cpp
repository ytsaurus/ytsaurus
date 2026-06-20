#include "log_slice_engine.h"

#include "time_parser.h"

#include <library/cpp/streams/zstd/zstd.h>

#include <library/cpp/yt/assert/assert.h>
#include <library/cpp/yt/error/error.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/stream/zlib.h>
#include <util/system/shellcommand.h>

#include <algorithm>
#include <iterator>
#include <memory>
#include <optional>
#include <vector>

namespace NYT::NLogSlice {

////////////////////////////////////////////////////////////////////////////////

namespace {

//! Magic number that starts every zstd data frame. A zstd log is a sequence of
//! such data frames interleaved with 32-byte skippable "sync tag" frames; the
//! streaming decoder transparently skips the latter.
constexpr TStringBuf ZstdFrameMagic = TStringBuf("\x28\xb5\x2f\xfd", 4);

//! Magic number that starts every gzip member. A gzip log is a sequence of
//! independent gzip members (each carrying its own block size in a "YT" extra
//! subfield); the decompressor concatenates them transparently.
constexpr TStringBuf GzipMemberMagic = TStringBuf("\x1f\x8b", 2);

//! Upper bound on how many bytes we read while probing a candidate block start
//! for its first timestamp. The first decompressed bytes always appear well
//! within this prefix.
constexpr i64 ProbeLimit = 1 << 20;

//! Window used when scanning the file for a boundary marker.
constexpr i64 ScanWindowSize = 1 << 20;

//! Buffer size for the final decode pass.
constexpr i64 DecodeChunkSize = 256 << 10;

TString ReadFileRange(TFile& file, i64 offset, i64 size)
{
    TString buffer;
    buffer.ReserveAndResize(size);
    i64 done = 0;
    while (done < size) {
        size_t read = file.Pread(buffer.begin() + done, size - done, offset + done);
        if (read == 0) {
            break;
        }
        done += static_cast<i64>(read);
    }
    buffer.resize(done);
    return buffer;
}

void EmitIfInRange(TStringBuf line, TInstant from, TInstant to, IOutputStream& output)
{
    auto instant = ParseLogLineTime(line);
    if (!instant) {
        return;
    }
    if (*instant < from || *instant > to) {
        return;
    }
    output.Write(line.data(), line.size());
    output.Write('\n');
}

////////////////////////////////////////////////////////////////////////////////

//! An input stream over a fixed byte range [start, end) of a file.
class TFileRangeInput
    : public IInputStream
{
public:
    TFileRangeInput(TFile& file, i64 start, i64 end)
        : File_(file)
        , Position_(start)
        , End_(end)
    { }

private:
    TFile& File_;
    i64 Position_;
    const i64 End_;

    size_t DoRead(void* buf, size_t length) override
    {
        if (Position_ >= End_) {
            return 0;
        }
        size_t toRead = std::min<i64>(length, End_ - Position_);
        size_t read = File_.Pread(buf, toRead, Position_);
        Position_ += static_cast<i64>(read);
        return read;
    }
};

//! A trivial pass-through used as the "decoder" for plain-text logs.
class TPassThroughInput
    : public IInputStream
{
public:
    explicit TPassThroughInput(IInputStream* underlying)
        : Underlying_(underlying)
    { }

private:
    IInputStream* const Underlying_;

    size_t DoRead(void* buf, size_t length) override
    {
        return Underlying_->Read(buf, length);
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Base for all engines: binary-searches over format-specific block boundaries to
//! locate the slice and then decodes only the overlapping byte range.
/*!
 *  Subclasses provide just two format-specific operations:
 *    - FindCandidateAtOrAfter: the next potential block/line boundary;
 *    - MakeDecoder: how to turn a raw byte range into a decompressed stream.
 *
 *  Candidate boundaries are validated by decoding their first line and parsing its
 *  timestamp, which both rejects spurious magic-number matches inside compressed
 *  data and yields the per-block timestamp the binary search needs.
 */
class TBlockSliceEngine
    : public ILogSliceEngine
{
public:
    void Slice(TFile& file, TInstant from, TInstant to, IOutputStream& output) final
    {
        i64 fileSize = file.GetLength();
        if (fileSize == 0) {
            return;
        }

        auto startBlock = FindStartBlock(file, fileSize, from);
        i64 startOffset = startBlock ? startBlock->Offset : 0;
        i64 endOffset = FindEndOffset(file, fileSize, to);
        if (endOffset <= startOffset) {
            return;
        }

        DecodeAndFilter(file, startOffset, endOffset, from, to, output);
    }

    std::optional<std::pair<TInstant, TInstant>> GetTimeRange(TFile& file) final
    {
        i64 fileSize = file.GetLength();
        if (fileSize == 0) {
            return std::nullopt;
        }

        auto first = FindBlockAtOrAfter(file, 0, fileSize);
        if (!first) {
            return std::nullopt;
        }

        // The last log line lives in the physically last decodable block. Find
        // that block directly by scanning backward from the end of the file for
        // a boundary marker -- no need to walk the whole file or binary-search.
        i64 lastOffset = FindLastBlockOffset(file, fileSize).value_or(first->Offset);
        auto last = DecodeLastTime(file, lastOffset, fileSize);

        return std::make_pair(first->FirstTime, last.value_or(first->FirstTime));
    }

protected:
    //! Offset of the next block/line boundary at or after [pos], or null if none.
    virtual std::optional<i64> FindCandidateAtOrAfter(TFile& file, i64 pos, i64 fileSize) = 0;

    //! Offset of the block/line boundary nearest the end of the file but strictly
    //! before [endPos], or null if none. Used to locate the last block without
    //! scanning forward from the start of the file.
    virtual std::optional<i64> FindCandidateAtOrBefore(TFile& file, i64 endPos, i64 fileSize) = 0;

    //! Wraps a raw byte stream into a stream of decompressed log text.
    virtual std::unique_ptr<IInputStream> MakeDecoder(IInputStream* raw) = 0;

    //! Scans the file for the next occurrence of [pattern] at or after [pos].
    static std::optional<i64> FindBytes(TFile& file, i64 pos, i64 fileSize, TStringBuf pattern)
    {
        i64 overlap = static_cast<i64>(pattern.size()) - 1;
        i64 scan = std::max<i64>(pos, 0);
        while (scan < fileSize) {
            i64 length = std::min<i64>(ScanWindowSize, fileSize - scan);
            TString window = ReadFileRange(file, scan, length);
            size_t idx = TStringBuf(window).find(pattern);
            if (idx != TStringBuf::npos) {
                return scan + static_cast<i64>(idx);
            }
            if (length < ScanWindowSize) {
                break;
            }
            scan += ScanWindowSize - overlap;
        }
        return std::nullopt;
    }

    //! Scans the file for the last occurrence of [pattern] that starts strictly
    //! before [endPos], i.e. the boundary nearest the end of the file. Mirrors
    //! FindBytes but walks the file backward window by window.
    static std::optional<i64> FindBytesBackward(TFile& file, i64 endPos, TStringBuf pattern)
    {
        i64 overlap = static_cast<i64>(pattern.size()) - 1;
        i64 scanEnd = std::min<i64>(endPos, file.GetLength());
        while (scanEnd > 0) {
            i64 length = std::min<i64>(ScanWindowSize, scanEnd);
            i64 scanStart = scanEnd - length;
            TString window = ReadFileRange(file, scanStart, length);
            size_t idx = TStringBuf(window).rfind(pattern);
            if (idx != TStringBuf::npos) {
                return scanStart + static_cast<i64>(idx);
            }
            if (scanStart == 0) {
                break;
            }
            // Overlap with the just-scanned window so a pattern straddling the
            // boundary is not missed.
            scanEnd = scanStart + overlap;
        }
        return std::nullopt;
    }

private:
    struct TBlockInfo
    {
        i64 Offset;
        TInstant FirstTime;
    };

    //! Decodes the timestamp of the first log line of the block supposedly starting
    //! at [offset]. Returns null if the bytes there do not decode into a timestamped
    //! line (which is how spurious boundary matches are rejected).
    std::optional<TInstant> TryDecodeFirstTime(TFile& file, i64 offset, i64 fileSize)
    {
        i64 end = std::min<i64>(offset + ProbeLimit, fileSize);
        TFileRangeInput raw(file, offset, end);

        std::unique_ptr<IInputStream> decoder;
        try {
            decoder = MakeDecoder(&raw);
        } catch (const std::exception&) {
            return std::nullopt;
        }

        // We only need the leading timestamp (26 bytes).
        TString head;
        char buffer[256];
        while (std::ssize(head) < 64) {
            size_t read = 0;
            try {
                read = decoder->Read(buffer, sizeof(buffer));
            } catch (const std::exception&) {
                return std::nullopt;
            }
            if (read == 0) {
                break;
            }
            head.append(buffer, read);
        }

        return ParseLogLineTime(head);
    }

    std::optional<TBlockInfo> FindBlockAtOrAfter(TFile& file, i64 pos, i64 fileSize)
    {
        i64 cursor = std::max<i64>(pos, 0);
        while (cursor < fileSize) {
            auto candidate = FindCandidateAtOrAfter(file, cursor, fileSize);
            if (!candidate) {
                break;
            }
            if (auto firstTime = TryDecodeFirstTime(file, *candidate, fileSize)) {
                return TBlockInfo{*candidate, *firstTime};
            }
            cursor = *candidate + 1;
        }
        return std::nullopt;
    }

    //! Largest block start whose first timestamp is <= [key], or null if none.
    std::optional<TBlockInfo> FindStartBlock(TFile& file, i64 fileSize, TInstant key)
    {
        std::optional<TBlockInfo> result;
        i64 lo = 0;
        i64 hi = fileSize;
        while (lo < hi) {
            i64 mid = lo + (hi - lo) / 2;
            auto block = FindBlockAtOrAfter(file, mid, fileSize);
            if (block && block->FirstTime <= key) {
                result = block;
                lo = block->Offset + 1;
            } else {
                hi = mid;
            }
        }
        return result;
    }

    //! Offset of the physically last decodable block in the file, located by
    //! scanning backward for a boundary marker and validating each candidate by
    //! decoding its first timestamp. Returns null if no valid block is found.
    std::optional<i64> FindLastBlockOffset(TFile& file, i64 fileSize)
    {
        i64 endPos = fileSize;
        while (endPos > 0) {
            auto candidate = FindCandidateAtOrBefore(file, endPos, fileSize);
            if (!candidate) {
                break;
            }
            if (TryDecodeFirstTime(file, *candidate, fileSize)) {
                return *candidate;
            }
            // Spurious marker (or undecodable tail): keep looking before it.
            endPos = *candidate;
        }
        return std::nullopt;
    }

    //! Offset of the first block whose first timestamp is > [key], or [fileSize].
    i64 FindEndOffset(TFile& file, i64 fileSize, TInstant key)
    {
        i64 result = fileSize;
        i64 lo = 0;
        i64 hi = fileSize;
        while (lo < hi) {
            i64 mid = lo + (hi - lo) / 2;
            auto block = FindBlockAtOrAfter(file, mid, fileSize);
            if (block && block->FirstTime <= key) {
                lo = block->Offset + 1;
            } else {
                if (block) {
                    result = block->Offset;
                }
                hi = mid;
            }
        }
        return result;
    }

    //! Decodes [startOffset, endOffset) and feeds the decoded bytes, in order, to
    //! [sink] (invoked as sink(const char*, size_t)), recovering as much of a
    //! truncated tail as possible. Shared by every decode path so they stay
    //! byte-for-byte consistent.
    /*!
     *  The streaming decoders raise on a premature end (an actively written log
     *  ends mid-block) and discard the output of the very read that hit it -- which
     *  is exactly the final, most interesting line. To match what `zstd -dc` emits
     *  we decode twice on truncation: the first pass delivers everything that
     *  decodes cleanly, the second replays that prefix silently and then drains the
     *  now-isolated final chunk one byte at a time so no decodable byte is lost. A
     *  cleanly terminated range (the common case) finishes in the first pass.
     */
    template <class TSink>
    void DecodeRange(TFile& file, i64 startOffset, i64 endOffset, const TSink& sink)
    {
        i64 cleanBytes = 0;
        bool truncated = false;
        {
            TFileRangeInput raw(file, startOffset, endOffset);
            std::unique_ptr<IInputStream> decoder = MakeDecoder(&raw);
            std::vector<char> buffer(DecodeChunkSize);
            while (true) {
                size_t read = 0;
                try {
                    read = decoder->Read(buffer.data(), buffer.size());
                } catch (const std::exception&) {
                    truncated = true;
                    break;
                }
                if (read == 0) {
                    break;
                }
                sink(buffer.data(), read);
                cleanBytes += static_cast<i64>(read);
            }
        }
        if (!truncated) {
            return;
        }

        // Recover the tail the throwing read discarded: replay the bytes already
        // delivered (without re-delivering them), then read the remainder one byte
        // at a time so the last decodable byte arrives before the decoder raises.
        TFileRangeInput raw(file, startOffset, endOffset);
        std::unique_ptr<IInputStream> decoder = MakeDecoder(&raw);
        std::vector<char> buffer(DecodeChunkSize);
        try {
            i64 skipped = 0;
            while (skipped < cleanBytes) {
                i64 want = std::min<i64>(std::ssize(buffer), cleanBytes - skipped);
                size_t read = decoder->Read(buffer.data(), want);
                if (read == 0) {
                    break;
                }
                skipped += static_cast<i64>(read);
            }
            char byte;
            while (decoder->Read(&byte, 1) != 0) {
                sink(&byte, 1);
            }
        } catch (const std::exception&) {
            // Reached the premature end again: stop with what was recovered.
        }
    }

    void DecodeAndFilter(
        TFile& file,
        i64 startOffset,
        i64 endOffset,
        TInstant from,
        TInstant to,
        IOutputStream& output)
    {
        TString carry;
        DecodeRange(file, startOffset, endOffset, [&] (const char* data, size_t size) {
            carry.append(data, size);
            size_t lineStart = 0;
            while (true) {
                size_t newline = carry.find('\n', lineStart);
                if (newline == TString::npos) {
                    break;
                }
                EmitIfInRange(TStringBuf(carry.data() + lineStart, newline - lineStart), from, to, output);
                lineStart = newline + 1;
            }
            if (lineStart > 0) {
                carry.remove(0, lineStart);
            }
        });

        if (!carry.empty()) {
            EmitIfInRange(TStringBuf(carry), from, to, output);
        }
    }

    //! Returns the timestamp of the last timestamped line in [text], searching for
    //! line breaks from the end. The very last line may be "broken" -- truncated
    //! mid-write so that its leading timestamp lacks the microseconds part -- in
    //! which case ParseLogLineTime rejects it and we fall back to the previous line.
    static std::optional<TInstant> FindLastLineTime(TStringBuf text)
    {
        size_t end = text.size();
        while (true) {
            size_t newline = (end == 0) ? TStringBuf::npos : text.rfind('\n', end - 1);
            size_t lineStart = (newline == TStringBuf::npos) ? 0 : newline + 1;
            if (auto instant = ParseLogLineTime(text.SubStr(lineStart, end - lineStart))) {
                return instant;
            }
            if (newline == TStringBuf::npos) {
                return std::nullopt;
            }
            end = newline;
        }
    }

    //! Decodes the last block and returns the timestamp of its last timestamped
    //! line, or null if there is none.
    std::optional<TInstant> DecodeLastTime(TFile& file, i64 startOffset, i64 fileSize)
    {
        TString text;
        DecodeRange(file, startOffset, fileSize, [&] (const char* data, size_t size) {
            text.append(data, size);
        });
        return FindLastLineTime(text);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TZstdSliceEngine
    : public TBlockSliceEngine
{
protected:
    std::optional<i64> FindCandidateAtOrAfter(TFile& file, i64 pos, i64 fileSize) override
    {
        return FindBytes(file, pos, fileSize, ZstdFrameMagic);
    }

    std::optional<i64> FindCandidateAtOrBefore(TFile& file, i64 endPos, i64 /*fileSize*/) override
    {
        return FindBytesBackward(file, endPos, ZstdFrameMagic);
    }

    std::unique_ptr<IInputStream> MakeDecoder(IInputStream* raw) override
    {
        return std::make_unique<TZstdDecompress>(raw);
    }
};

class TGzipSliceEngine
    : public TBlockSliceEngine
{
protected:
    std::optional<i64> FindCandidateAtOrAfter(TFile& file, i64 pos, i64 fileSize) override
    {
        return FindBytes(file, pos, fileSize, GzipMemberMagic);
    }

    std::optional<i64> FindCandidateAtOrBefore(TFile& file, i64 endPos, i64 /*fileSize*/) override
    {
        return FindBytesBackward(file, endPos, GzipMemberMagic);
    }

    std::unique_ptr<IInputStream> MakeDecoder(IInputStream* raw) override
    {
        return std::make_unique<TZLibDecompress>(raw, ZLib::GZip);
    }
};

class TPlainTextSliceEngine
    : public TBlockSliceEngine
{
protected:
    std::optional<i64> FindCandidateAtOrAfter(TFile& file, i64 pos, i64 fileSize) override
    {
        // Each line is a "block"; a probe rounds up to the next line start.
        if (pos <= 0) {
            return fileSize > 0 ? std::optional<i64>(0) : std::nullopt;
        }
        auto newline = FindBytes(file, pos, fileSize, TStringBuf("\n"));
        if (!newline) {
            return std::nullopt;
        }
        i64 lineStart = *newline + 1;
        return lineStart < fileSize ? std::optional<i64>(lineStart) : std::nullopt;
    }

    std::optional<i64> FindCandidateAtOrBefore(TFile& file, i64 endPos, i64 /*fileSize*/) override
    {
        // Each line is a "block"; the candidate is the start of the last line
        // beginning before [endPos]. Ignore a terminating newline at the very end
        // of the range so "...\nLAST\n" yields the start of LAST, not the empty
        // line after it.
        i64 top = endPos;
        if (top > 0) {
            TString last = ReadFileRange(file, top - 1, 1);
            if (!last.empty() && last[0] == '\n') {
                --top;
            }
        }
        auto newline = FindBytesBackward(file, top, TStringBuf("\n"));
        i64 lineStart = newline ? *newline + 1 : 0;
        return lineStart < endPos ? std::optional<i64>(lineStart) : std::nullopt;
    }

    std::unique_ptr<IInputStream> MakeDecoder(IInputStream* raw) override
    {
        return std::make_unique<TPassThroughInput>(raw);
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

ECompressionCodec ParseCompressionCodec(TStringBuf name)
{
    if (name == "auto") {
        return ECompressionCodec::Auto;
    }
    if (name == "zstd" || name == "zst") {
        return ECompressionCodec::Zstd;
    }
    if (name == "gzip" || name == "gz") {
        return ECompressionCodec::Gzip;
    }
    if (name == "plain" || name == "plaintext" || name == "text" || name == "none" || name == "raw") {
        return ECompressionCodec::PlainText;
    }
    THROW_ERROR_EXCEPTION("Unknown compression codec %Qv (expected one of \"auto\", \"zstd\", \"gzip\", \"plain\")",
        name);
}

ECompressionCodec DetectCompressionCodec(TStringBuf fileName)
{
    if (fileName.EndsWith(".zst")) {
        return ECompressionCodec::Zstd;
    }
    if (fileName.EndsWith(".gz")) {
        return ECompressionCodec::Gzip;
    }
    return ECompressionCodec::PlainText;
}

std::vector<TString> SplitCommandLine(TStringBuf line)
{
    std::vector<TString> args;
    TString current;
    bool inToken = false;
    enum { None, Single, Double } quote = None;

    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        if (quote == Single) {
            if (c == '\'') {
                quote = None;
            } else {
                current.push_back(c);
            }
            continue;
        }
        if (quote == Double) {
            if (c == '"') {
                quote = None;
            } else if (c == '\\' && i + 1 < line.size() && (line[i + 1] == '"' || line[i + 1] == '\\')) {
                current.push_back(line[++i]);
            } else {
                current.push_back(c);
            }
            continue;
        }
        if (c == '\'') {
            quote = Single;
            inToken = true;
        } else if (c == '"') {
            quote = Double;
            inToken = true;
        } else if (c == '\\' && i + 1 < line.size()) {
            current.push_back(line[++i]);
            inToken = true;
        } else if (c == ' ' || c == '\t' || c == '\n') {
            if (inToken) {
                args.push_back(current);
                current.clear();
                inToken = false;
            }
        } else {
            current.push_back(c);
            inToken = true;
        }
    }

    if (quote != None) {
        THROW_ERROR_EXCEPTION("Unbalanced quote in grep argument string %Qv", line);
    }
    if (inToken) {
        args.push_back(current);
    }
    return args;
}

void FilterWithGrep(
    const std::vector<TString>& grepArgs,
    const std::function<void(IOutputStream&)>& produce,
    IOutputStream& output)
{
    TList<TString> args(grepArgs.begin(), grepArgs.end());

    // Stream grep's stdin from [produce] and grep's stdout into [output] rather
    // than buffering the whole slice in memory: a multi-gigabyte log would
    // otherwise be materialized several times over and get the process OOM-killed
    // (SIGKILL, exit code 137). Async mode lets us feed grep's stdin from this
    // thread while a watcher thread drains grep's stdout into [output], so the two
    // pipes can never deadlock.
    TShellCommandOptions options;
    options.SetUseShell(false);
    options.SetAsync(true);
    options.PipeInput();
    options.SetOutputStream(&output);

    TShellCommand command("grep", args, options);
    command.Run();

    {
        TFileOutput grepInput(TFile(command.GetInputHandle().Release()));
        produce(grepInput);
        grepInput.Finish();
    }

    command.Wait();

    // grep exits 1 when nothing matched, which is not an error for us; only a
    // code of 2 or above (or a failure to launch) signals a real problem.
    auto exitCode = command.GetExitCode();
    if (command.GetStatus() == TShellCommand::SHELL_INTERNAL_ERROR ||
        (exitCode.Defined() && *exitCode >= 2))
    {
        THROW_ERROR_EXCEPTION("grep failed: %v", command.GetError());
    }
}

ILogSliceEnginePtr CreateLogSliceEngine(ECompressionCodec codec, TStringBuf fileName)
{
    if (codec == ECompressionCodec::Auto) {
        codec = DetectCompressionCodec(fileName);
    }
    switch (codec) {
        case ECompressionCodec::Zstd:
            return std::make_unique<TZstdSliceEngine>();
        case ECompressionCodec::Gzip:
            return std::make_unique<TGzipSliceEngine>();
        case ECompressionCodec::PlainText:
            return std::make_unique<TPlainTextSliceEngine>();
        case ECompressionCodec::Auto:
            break;
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogSlice
