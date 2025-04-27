#include <yql/essentials/sql/v1/highlight/sql_highlight.h>

#include <library/cpp/resource/resource.h>
#include <library/cpp/getopt/last_getopt.h>

enum class ETarget {
    Readme,
};

const char* ToString(ETarget target) {
    switch (target) {
        case ETarget::Readme:
            return "readme";
    }
}

struct TArgs {
    ETarget Target;

    static TArgs Parse(int argc, char* argv[]) {
        TString Target;

        NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
        opts
            .AddLongOption('t', "target", "generation target")
            .Choices({ToString(ETarget::Readme)})
            .Required()
            .StoreResult(&Target);
        opts.SetFreeArgsNum(0);
        opts.AddHelpOption();

        NLastGetopt::TOptsParseResult parsed(&opts, argc, argv);

        TArgs args;
        for (auto target : {ETarget::Readme}) {
            if (Target == ToString(target)) {
                args.Target = target;
            }
        }
        return args;
    }
};

template <ETarget T>
void Generate(IOutputStream& out);

TString Format(const NSQLHighlight::TMetaToken& token) {
    return "``` " + JoinSeq(" ", token) + " ```";
}

TString Format(const TVector<NSQLHighlight::TMetaToken>& tokens) {
    TVector<TString> list;
    for (const auto& token : tokens) {
        list.emplace_back(Format(token));
    }
    return JoinSeq(", ", list);
}

TString Format(const TVector<NSQLHighlight::THumanHighlighting::TUnit>& units) {
    TStringBuilder s;
    for (const auto& unit : units) {
        s << "- `" << unit.Kind << "` - " << Format(unit.Tokens) << ".\n\n";
    }
    Y_ENSURE(s.EndsWith("\n\n"));
    s.pop_back();
    s.pop_back();
    return s;
}

TString Format(const TMap<TString, TString>& map) {
    TStringBuilder s;
    for (const auto& [k, v] : map) {
        s << "- ```" << k << "``` - ``` " << v << " ```.\n\n";
    }
    Y_ENSURE(s.EndsWith("\n\n"));
    s.pop_back();
    s.pop_back();
    return s;
}

template <>
void Generate<ETarget::Readme>(IOutputStream& out) {
    TString text;
    Y_ENSURE(NResource::FindExact("README.md", &text));

    auto grammar = NSQLReflect::LoadLexerGrammar();
    auto highlighting = NSQLHighlight::MakeHighlighting(std::move(grammar));

    Y_ENSURE(SubstGlobal(text, "@UNITS_MD_FRAGMENT@", Format(highlighting.Units)) == 1);
    Y_ENSURE(SubstGlobal(text, "@REFERENCES_MD_FRAGMENT@", Format(highlighting.References)) == 1);

    out << text;
}

void GenerateG(ETarget target, IOutputStream& out) {
    switch (target) {
        case ETarget::Readme:
            Generate<ETarget::Readme>(out);
    }
}

int Run(int argc, char* argv[]) {
    TArgs args = TArgs::Parse(argc, argv);
    GenerateG(args.Target, Cout);
    return 0;
}

int main(int argc, char* argv[]) try {
    return Run(argc, argv);
} catch (const yexception& e) {
    Cerr << "Caught exception:" << e.what() << Endl;
    return 1;
} catch (...) {
    Cerr << CurrentExceptionMessage() << Endl;
    return 1;
}
