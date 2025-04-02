#include <yql/essentials/sql/v1/complete/sql_complete.h>
#include <yql/essentials/sql/v1/complete/text/word.h>

#include <contrib/restricted/patched/replxx/include/replxx.hxx>

#include <library/cpp/getopt/last_getopt.h>

#include <util/stream/file.h>

replxx::Replxx::Color ColorOf(NSQLComplete::ECandidateKind kind) {
    switch (kind) {
        case NSQLComplete::ECandidateKind::Keyword:
            return replxx::Replxx::Color::BLUE;
        case NSQLComplete::ECandidateKind::TypeName:
            return replxx::Replxx::Color::GREEN;
        case NSQLComplete::ECandidateKind::FunctionName:
            return replxx::Replxx::Color::MAGENTA;
        default:
            return replxx::Replxx::Color::DEFAULT;
    }
}

void Repl() {
    auto engine = NSQLComplete::MakeSqlCompletionEngine();

    replxx::Replxx rx;
    rx.install_window_change_handler();
    rx.enable_bracketed_paste();

    rx.set_word_break_characters(NSQLComplete::WordBreakCharacters);
    rx.set_complete_on_empty(true);

    rx.set_completion_callback([&](const std::string& prefix, int&) {
        replxx::Replxx::completions_t completions;
        for (auto& candidate : engine->Complete({prefix}).Candidates) {
            completions.emplace_back(std::move(candidate.Content), ColorOf(candidate.Kind));
        }
        return completions;
    });

    rx.set_hint_callback([&](const std::string& prefix, int&, replxx::Replxx::Color&) {
        replxx::Replxx::hints_t hints;
        for (auto& candidate : engine->Complete({prefix}).Candidates) {
            hints.emplace_back(std::move(candidate.Content));
        }
        return hints;
    });

    for (;;) {
        const auto* input = rx.input(":) ");
        if (input == nullptr && errno == EAGAIN) {
            continue;
        } else if (input == nullptr) {
            return;
        }
    }
}

void Basic(TString inFileName, TMaybe<ui64> pos) {
    THolder<TUnbufferedFileInput> inFile;
    if (!inFileName.empty()) {
        inFile.Reset(new TUnbufferedFileInput(inFileName));
    }
    IInputStream& in = inFile ? *inFile.Get() : Cin;

    auto queryString = in.ReadAll();
    auto engine = NSQLComplete::MakeSqlCompletionEngine();
    NSQLComplete::TCompletionInput input;
    input.Text = queryString;
    if (pos) {
        input.CursorPosition = *pos;
    } else {
        input.CursorPosition = queryString.size();
    }

    auto output = engine->Complete(input);
    for (const auto& c : output.Candidates) {
        Cout << "[" << c.Kind << "] " << c.Content << "\n";
    }
}

int Run(int argc, char* argv[]) {
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();

    TString inFileName;
    TMaybe<ui64> pos;
    bool isRepl;
    opts.AddLongOption('i', "input", "input file").RequiredArgument("input").StoreResult(&inFileName);
    opts.AddLongOption('p', "pos", "position").StoreResult(&pos);
    opts.AddLongOption('r', "repl", "REPL Mode ON/OFF").RequiredArgument("repl").SetFlag(&isRepl);
    opts.SetFreeArgsNum(0);
    opts.AddHelpOption();

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    if (isRepl) {
        Repl();
    } else {
        Basic(std::move(inFileName), std::move(pos));
    }

    return 0;
}

int main(int argc, char* argv[]) {
    try {
        return Run(argc, argv);
    } catch (const yexception& e) {
        Cerr << "Caught exception:" << e.what() << Endl;
        return 1;
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
    return 0;
}
