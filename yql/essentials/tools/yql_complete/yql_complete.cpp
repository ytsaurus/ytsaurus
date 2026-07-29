#include "completion_factory.h"

#include <yql/essentials/sql/v1/ide/completion/sql_complete.h>
#include <yql/essentials/sql/v1/ide/completion/name/service/ranking/frequency.h>

#include <yql/essentials/utils/utf8.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/vector.h>
#include <util/charset/utf8.h>
#include <util/stream/file.h>

#include <cctype>

NSQLComplete::TFrequencyData LoadFrequencyDataFromFile(TString filepath) {
    TString text = TUnbufferedFileInput(filepath).ReadAll();
    return NSQLComplete::Pruned(NSQLComplete::ParseJsonFrequencyData(text));
}

NJson::TJsonMap LoadSchemaJsonFromFile(TString filepath) {
    TString text = TUnbufferedFileInput(filepath).ReadAll();
    NJson::TJsonMap map;
    if (!NJson::ReadJsonTree(text, &map)) {
        ythrow yexception() << "Failed to parse JSON: '" << text << "'";
    }
    return map;
}

size_t UTF8PositionToBytes(const TStringBuf text, size_t position) {
    const TStringBuf substr = SubstrUTF8(text, position, text.length());
    return substr.begin() - text.begin();
}

NSQLComplete::TCompletionInput MakeCompletionInput(TString& text, TMaybe<ui64> pos) {
    size_t lengthUtf8 = GetNumberOfUTF8Chars(text);

    if (!pos) {
        if (auto count = Count(text, '#'); 1 < count) {
            ythrow yexception() << "provided input contains " << count << " '#', expected 0 or 1";
        }

        return NSQLComplete::SharpedInput(text);
    }

    if (lengthUtf8 < *pos) {
        ythrow yexception() << "provided position " << *pos << " is out of range " << lengthUtf8;
    }

    return {
        .Text = text,
        .CursorPosition = UTF8PositionToBytes(text, *pos),
    };
}

enum class EReadDocumentResult {
    Complete,
    EndOfStream,
    Incomplete,
};

EReadDocumentResult ReadJsonDocument(IInputStream& input, TString& document) {
    document.clear();

    for (char current; input.ReadChar(current);) {
        if (!std::isspace(static_cast<unsigned char>(current))) {
            document.push_back(current);
            break;
        }
    }

    if (document.empty()) {
        return EReadDocumentResult::EndOfStream;
    }

    const char first = document.front();
    if (first != '{' && first != '[' && first != '"') {
        for (char current; input.ReadChar(current);) {
            if (std::isspace(static_cast<unsigned char>(current))) {
                break;
            }
            document.push_back(current);
        }
        return EReadDocumentResult::Complete;
    }

    bool isInString = (first == '"');
    bool isEscaped = false;
    TVector<char> closingCharacters;
    if (first == '{') {
        closingCharacters.push_back('}');
    } else if (first == '[') {
        closingCharacters.push_back(']');
    }

    for (char current; input.ReadChar(current);) {
        document.push_back(current);

        if (isInString) {
            if (isEscaped) {
                isEscaped = false;
            } else if (current == '\\') {
                isEscaped = true;
            } else if (current == '"') {
                isInString = false;
                if (closingCharacters.empty()) {
                    return EReadDocumentResult::Complete;
                }
            }
            continue;
        }

        if (current == '"') {
            isInString = true;
        } else if (current == '{') {
            closingCharacters.push_back('}');
        } else if (current == '[') {
            closingCharacters.push_back(']');
        } else if (current == '}' || current == ']') {
            if (!closingCharacters.empty() && closingCharacters.back() == current) {
                closingCharacters.pop_back();
                if (closingCharacters.empty()) {
                    return EReadDocumentResult::Complete;
                }
            } else if (!closingCharacters.empty() && closingCharacters.front() == current) {
                // Let the JSON parser report mismatched nested brackets, but
                // terminate the malformed top-level document for recovery.
                return EReadDocumentResult::Complete;
            }
        }
    }

    return EReadDocumentResult::Incomplete;
}

void WriteStreamResponse(const TVector<NSQLComplete::TCandidate>& candidates) {
    NJson::TJsonArray response;
    for (const auto& candidate : candidates) {
        response.AppendValue(NJson::TJsonMap{
            {"word", candidate.Content},
            {"type", ToString(candidate.Kind)},
        });
    }

    NJson::WriteJson(&Cout, &response, false);
    Cout << Endl;
}

void WriteEmptyStreamResponse() {
    WriteStreamResponse({});
}

void LogStreamError(TStringBuf message) {
    Cerr << "Failed to process stream request: " << message << Endl;
}

void ReadRequest(TStringBuf document, const TCompletionFactory& completionFactory) {
    try {
        NJson::TJsonValue request;
        NJson::ReadJsonTree(document, &request, true);
        if (!request.IsMap()) {
            ythrow yexception() << "request must be a map";
        }

        TString query;
        if (request.Has("query")) {
            if (!request["query"].IsString()) {
                ythrow yexception() << "query must be a string";
            }
            query = request["query"].GetStringSafe();
        }

        const ui64 queryLength = GetNumberOfUTF8Chars(query);
        ui64 position = queryLength;
        if (request.Has("position")) {
            if (!request["position"].IsUInteger()) {
                ythrow yexception() << "position must be a non-negative integer";
            }
            position = Min<ui64>(request["position"].GetUIntegerSafe(), queryLength);
        }

        const NJson::TJsonValue* schema = nullptr;
        if (request.Has("schema")) {
            schema = &request["schema"];
        }

        auto engine = completionFactory.MakeEngine(schema);
        auto input = MakeCompletionInput(query, position);
        auto output = engine->CompleteAsync(input).ExtractValueSync();
        WriteStreamResponse(output.Candidates);
    } catch (const std::exception& error) {
        LogStreamError(error.what());
        WriteEmptyStreamResponse();
    } catch (...) {
        LogStreamError(CurrentExceptionMessage());
        WriteEmptyStreamResponse();
    }
}

void RunStream(const TCompletionFactory& completionFactory) {
    for (;;) {
        TString document;
        const auto readResult = ReadJsonDocument(Cin, document);
        if (readResult == EReadDocumentResult::EndOfStream) {
            return;
        }
        if (readResult == EReadDocumentResult::Incomplete) {
            LogStreamError("unexpected end of input inside JSON");
            WriteEmptyStreamResponse();
            return;
        }

        ReadRequest(document, completionFactory);
    }
}

int Run(int argc, char** argv) {
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();

    TString inFileName;
    TString inQueryText;
    TString freqFileName;
    TString schemaFileName;
    TMaybe<ui64> pos;
    bool streamMode = false;
    opts.AddLongOption('i', "input", "input file").RequiredArgument("input").StoreResult(&inFileName);
    opts.AddLongOption('q', "query", "input query text").RequiredArgument("query").StoreResult(&inQueryText);
    opts.AddLongOption('f', "freq", "frequences file").StoreResult(&freqFileName);
    opts.AddLongOption('s', "schema", "schema file").StoreResult(&schemaFileName);
    opts.AddLongOption('p', "pos", "position").StoreResult(&pos);
    opts.AddLongOption("stream", "process a stream of JSON requests from stdin").NoArgument().StoreTrue(&streamMode);
    opts.SetFreeArgsNum(0);
    opts.AddHelpOption();

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    if (res.Has("input") && res.Has("query")) {
        ythrow yexception() << "use either 'input' or 'query', not both";
    }

    NSQLComplete::TFrequencyData frequency;
    if (freqFileName.empty()) {
        frequency = NSQLComplete::LoadFrequencyData();
    } else {
        frequency = LoadFrequencyDataFromFile(freqFileName);
    }
    TCompletionFactory completionFactory(std::move(frequency));

    if (streamMode) {
        if (res.Has("input") || res.Has("query") || res.Has("schema") || res.Has("pos")) {
            ythrow yexception() << "--stream cannot be combined with --input, --query, --schema, or --pos";
        }
        RunStream(completionFactory);
        return 0;
    }

    TString queryString;
    if (res.Has("query")) {
        queryString = std::move(inQueryText);
    } else {
        THolder<TUnbufferedFileInput> inFile;
        if (!inFileName.empty()) {
            inFile.Reset(new TUnbufferedFileInput(inFileName));
        }
        IInputStream& in = inFile ? *inFile.Get() : Cin;
        queryString = in.ReadAll();
    }

    TMaybe<NJson::TJsonMap> schema;
    if (!schemaFileName.empty()) {
        schema = LoadSchemaJsonFromFile(schemaFileName);
    }
    auto engine = completionFactory.MakeEngine(schema ? &*schema : nullptr);
    auto input = MakeCompletionInput(queryString, pos);
    auto output = engine->CompleteAsync(input).ExtractValueSync();
    for (const auto& c : output.Candidates) {
        Cout << "[" << c.Kind << "] " << c.Content << "\n";
    }

    return 0;
}

int main(int argc, char** argv) {
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
