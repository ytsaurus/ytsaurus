#include <yt/yt/tools/trigrep/lib/index_builder.h>
#include <yt/yt/tools/trigrep/lib/matcher.h>
#include <yt/yt/tools/trigrep/lib/uncompressed_reader.h>
#include <yt/yt/tools/trigrep/lib/zstd_reader.h>

#include <yt/yt/library/program/program.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/finally.h>

#include <library/cpp/yt/system/exit.h>

#include <util/stream/file.h>

#include <unistd.h>

namespace NYT::NTrigrep {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsStdoutTerminal()
{
    return isatty(fileno(stdout));
}

DEFINE_ENUM(EHighlighting,
    ((Reset)              ( 0))
    ((Bold)               ( 1))
    ((RedForeground)      (31))
    ((GreenForeground)    (32))
);

static constexpr auto ResetHighlightings = std::array{EHighlighting::Reset};

struct IHighlighter
{
    virtual ~IHighlighter() = default;

    virtual void Highlight(TRange<EHighlighting> highlightings) = 0;
};

class TAnsiHighlighter
    : public IHighlighter
{
public:
    ~TAnsiHighlighter() = default;

    void Highlight(TRange<EHighlighting> highlightings) final
    {
        std::string ansi = "\033[";
        ansi.reserve(16);
        for (int index = 0; index < std::ssize(highlightings); ++index) {
            if (index > 0) {
                ansi += ";";
            }
            ansi += std::to_string(ToUnderlying(highlightings[index]));
        }
        ansi += "m";
        Cout << ansi;
    }
};

class TNullHighlighter
    : public IHighlighter
{
public:
    ~TNullHighlighter() = default;

    void Highlight(TRange<EHighlighting> /*highlightings*/) final
    { }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TProgram
    : public NYT::TProgram
{
public:
    TProgram()
    {
        Opts_.SetTitle("trigrep: TRIGRam-Empowered Pattern matcher and indexer");

        Opts_
            .AddLongOption(
                "build-index",
                "Build index file")
            .AddShortName('b')
            .StoreTrue(&BuildIndexFlag_);
        Opts_
            .AddLongOption(
                "frame-size",
                Format("Frame size estimate in MB (for uncompressed input only; defaults to %v)", DefaultUncompressedFrameSize / 1_MBs))
            .StoreResult(&FrameSizeMB_);
        Opts_
            .AddLongOption(
                "chunk-size",
                Format("Chunk size estimate in MB (defaults to %v)", DefaultChunkSize / 1_MBs))
            .StoreResult(&ChunkSizeMB_);
        Opts_
            .AddLongOption(
                "block-size",
                Format("Block size estimate in MB (defaults to %v)", DefaultBlockSize / 1_MBs))
            .StoreResult(&BlockSizeMB_);
        Opts_
            .AddLongOption(
                "index-segment-size",
                Format("Index segment size in KB (defaults to %v)", DefaultIndexSegmentSize / 1_KBs))
            .StoreResult(&IndexSegmentSizeKB_);
        Opts_
            .AddLongOption(
                "index-size-factor",
                Format("Index size estimate relative to the input size (defaults to %v)", DefaultIndexSizeFactor))
            .StoreResult(&IndexSizeFactor_);
        Opts_
            .AddLongOption(
                "index-path",
                Format("Path to index file (defaults to input with additional %v extension)", DefaultIndexExtension))
            .AddShortName('i')
            .RequiredArgument("PATH")
            .StoreResult(&IndexFile_);
        Opts_
            .AddLongOption(
                "line-number",
                "Print line number with output lines")
            .AddShortName('n')
            .StoreTrue(&PrintLineNumbersFlag_);
        Opts_
            .AddLongOption(
                "codec",
                "Input compression codec (supported options are: uncompressed, zstd)")
            .StoreMappedResultT<TStringBuf>(&Codec_, &ParseEnumArgMapper<ECompressionCodec>);
        Opts_
            .AddLongOption(
                "no-terminal",
                "Suppress terminal features (e.g. highlighting and progress reporting)")
            .StoreTrue(&NoTerminalFlag_);

        Opts_.SetFreeArgsMin(0);
        Opts_.SetFreeArgsMax(NLastGetopt::TOpts::UNLIMITED_ARGS);
    }

private:
    bool BuildIndexFlag_ = false;
    std::string IndexFile_;
    i64 FrameSizeMB_ = DefaultUncompressedFrameSize / 1_MBs;
    i64 ChunkSizeMB_ = DefaultChunkSize / 1_MBs;
    i64 BlockSizeMB_ = DefaultBlockSize / 1_MBs;
    i64 IndexSegmentSizeKB_ = DefaultIndexSegmentSize / 1_KBs;
    double IndexSizeFactor_ = DefaultIndexSizeFactor;
    bool PrintLineNumbersFlag_ = false;
    std::optional<ECompressionCodec> Codec_;
    bool NoTerminalFlag_ = !IsStdoutTerminal();

    void DoRun() override
    {
        Configure();

        const auto& parseResult = GetOptsParseResult();
        if (parseResult.GetFreeArgCount() == 0) {
            parseResult.PrintUsage();
            Exit(EProcessExitCode::ArgumentsError);
        }

        if (BuildIndexFlag_) {
            BuildIndex();
        } else {
            RunMatcher();
        }
    }

    void Configure()
    {
        if (NLogging::TLogManager::Get()->IsConfiguredFromEnv()) {
            // Disable terminal features when logging is configured.
            NoTerminalFlag_ = true;
        } else {
            // Log warnings to stderr unless explicitly configured.
            NLogging::TLogManager::Get()->Configure(
                NLogging::TLogManagerConfig::CreateStderrLogger(NLogging::ELogLevel::Warning));
        }
    }

    std::string GetIndexPath(const std::string& inputPath)
    {
        return IndexFile_.empty() ? inputPath + DefaultIndexExtension : IndexFile_;
    }

    ECompressionCodec GetCodec(const std::string& path)
    {
        if (Codec_) {
            return *Codec_;
        }

        if (path.ends_with(DefaultZstdExtension)) {
            return ECompressionCodec::Zstd;
        }

        return ECompressionCodec::Uncompressed;
    }

    std::unique_ptr<ISequentialReader> CreateSequentialReader(
        const std::string& path,
        ECompressionCodec codec)
    {
        switch (codec) {
            case ECompressionCodec::Uncompressed:
                return CreateSequentialUncompressedReader(path, FrameSizeMB_ * 1_MBs);

            case ECompressionCodec::Zstd:
                return CreateSequentialZstdReader(path);
        }
        YT_ABORT();
    }

    std::unique_ptr<IRandomReader> CreateRandomReader(
        const std::string& path,
        ECompressionCodec codec)
    {
        switch (codec) {
            case ECompressionCodec::Uncompressed:
                return CreateRandomUncompressedReader(path);

            case ECompressionCodec::Zstd:
                return CreateRandomZstdReader(path);
        }
        YT_ABORT();
    }

    void WriteProgressLine(TStringBuf message)
    {
        if (NoTerminalFlag_) {
            return;
        }
        Cout << "\r" << message;
        Cout.Flush();
    }

    void ClearProgressLine()
    {
        WriteProgressLine(std::string(64, ' '));
        WriteProgressLine("");
    }

    class TIndexBuilderCallbacks
        : public IIndexBuilderCallbacks
    {
    public:
        explicit TIndexBuilderCallbacks(TProgram* program)
            : Program_(program)
        { }

        void OnProgress(i64 bytesIndexed, i64 bytesTotal) final
        {
            Program_->WriteProgressLine(Format("Building index: %v%% done...", bytesIndexed * 100 / bytesTotal));
        }

    private:
        TProgram* const Program_;
    };

    void BuildIndex()
    {
        const auto& parseResult = GetOptsParseResult();
        if (parseResult.GetFreeArgCount() != 1) {
            THROW_ERROR_EXCEPTION("Must specify exactly one free argument, i.e. the input file path");
        }

        auto inputPath = parseResult.GetFreeArgs()[0];
        auto codec = GetCodec(inputPath);
        auto reader = CreateSequentialReader(inputPath, codec);

        auto indexPath = GetIndexPath(inputPath);
        TFileOutput output((TString(indexPath)));

        TBuildIndexOptions options{
            .ChunkSize = ChunkSizeMB_ * 1_MBs,
            .BlockSize = BlockSizeMB_ * 1_MBs,
            .IndexSegmentSize = IndexSegmentSizeKB_ * 1_KBs,
            .IndexSizeFactor = IndexSizeFactor_,
        };
        TIndexBuilderCallbacks callbacks(this);
        auto finallyGuard = Finally([this] { ClearProgressLine(); });
        NTrigrep::BuildIndex(reader.get(), &output, options, &callbacks);
    }

    class TMatcherCallbacks
        : public IMatcherCallbacks
    {
    public:
        explicit TMatcherCallbacks(TProgram* program)
            : Program_(program)
            , Highlighter_(
                Program_->NoTerminalFlag_
                ? std::unique_ptr<IHighlighter>(std::make_unique<TNullHighlighter>())
                : std::unique_ptr<IHighlighter>(std::make_unique<TAnsiHighlighter>()))
        { }

    private:
        TProgram* const Program_;
        const std::unique_ptr<IHighlighter> Highlighter_;

        static constexpr auto LineNumberHighlightings = std::array{EHighlighting::GreenForeground};
        static constexpr auto MatchHighlightings = std::array{EHighlighting::Bold, EHighlighting::RedForeground};

        void OnMatch(i64 lineIndex, TStringBuf line, TRange<std::pair<int, int>> matchingRanges) final
        {
            if (Program_->PrintLineNumbersFlag_) {
                Highlighter_->Highlight(LineNumberHighlightings);
                // NB: Print line index as 1-based.
                Cout << lineIndex + 1 << ":";
                Highlighter_->Highlight(ResetHighlightings);
            }

            int currentRangeIndex = 0;
            for (int charIndex = 0; charIndex < std::ssize(line); ++charIndex) {
                if (currentRangeIndex < std::ssize(matchingRanges) && charIndex == matchingRanges[currentRangeIndex].first) {
                    Highlighter_->Highlight({MatchHighlightings});
                }
                Cout << line[charIndex];
                if (currentRangeIndex < std::ssize(matchingRanges) && charIndex + 1 == matchingRanges[currentRangeIndex].second) {
                    ++currentRangeIndex;
                    Highlighter_->Highlight(ResetHighlightings);
                }
            }

            Cout << Endl;
        }
    };

    void RunMatcher()
    {
        const auto& parseResult = GetOptsParseResult();
        auto freeArgs = parseResult.GetFreeArgs();
        if (freeArgs.size() < 2) {
            THROW_ERROR_EXCEPTION("Must specify at least one search pattern and input file");
        }

        auto inputPath = freeArgs.back();
        auto codec = GetCodec(inputPath);
        auto reader = CreateRandomReader(inputPath, codec);

        auto indexPath = GetIndexPath(inputPath);
        TFile indexFile(TString(indexPath), OpenExisting | RdOnly);

        std::vector<std::string> patterns(freeArgs.begin(), freeArgs.end() - 1);

        TMatcherCallbacks callbacks(this);
        NTrigrep::RunMatcher(reader.get(), &indexFile, patterns, &callbacks);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep

int main(int argc, const char** argv)
{
    return NYT::NTrigrep::TProgram().Run(argc, argv);
}
