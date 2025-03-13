#include "frequency.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/resource/resource.h>

#include <util/string/ascii.h>
#include <util/string/split.h>

#include <regex>

namespace NSQLComplete {

    constexpr const char* JsonKeyParent = "parent";
    constexpr const char* JsonKeyRule = "rule";
    constexpr const char* JsonKeySum = "sum";

    constexpr const char* JsonParentFunc = "FUNC";
    constexpr const char* JsonParentModule = "MODULE";
    constexpr const char* JsonParentModuleFunc = "MODULE_FUNC";

    constexpr std::string_view RuleTypePrefix = "Rule_";

    struct TFrequencyItem {
        TString Parent;
        TString Rule;
        size_t Sum;

        static TFrequencyItem FromJson(NJson::TJsonValue::TMapType&& jMap) {
            return {
                .Parent = jMap.at(JsonKeyParent).GetStringSafe(),
                .Rule = jMap.at(JsonKeyRule).GetStringSafe(),
                .Sum = jMap.at(JsonKeySum).GetUIntegerSafe(),
            };
        }
    };

    TVector<TFrequencyItem> LoadFrequencyItems(const TStringBuf text) {
        NJson::TJsonValue json = NJson::ReadJsonFastTree(text);

        TVector<TFrequencyItem> items;

        for (auto& jVal : json.GetArraySafe()) {
            auto item = TFrequencyItem::FromJson(std::move(jVal.GetMapSafe()));
            items.emplace_back(std::move(item));
        }

        return items;
    }

    void ToLowerInplace(TString& text) {
        for (char& ch : text) {
            ch = ToLower(ch);
        }
    }

    void PurifyParserCallStack(TVector<TStringBuf>& frames) {
        static std::regex DetailFrameRegex(R"(T?((Alt|Block|Token)\d+)|Alt_.*)");
        const auto [first, last] = std::ranges::remove_if(frames, [&](const TStringBuf f) {
            std::smatch match;
            return std::regex_match(std::begin(f), std::end(f), match, DetailFrameRegex);
        });
        frames.erase(first, last);
    }

    TParserCallStack ParseRuleNames(const TVector<TStringBuf>& frames) {
        static const auto& ruleIdByName = 
            GetSqlGrammar(/* isAnsiLexer = */ false).GetRuleIdsByName();

        TParserCallStack stack;
        for (auto frame : frames) {
            if (frame == "TSQLv1ParserAST") {
                continue;
            }

            if (frame.StartsWith('T')) {
                frame = frame.SubStr(1);
            }

            Y_ENSURE(frame.StartsWith(RuleTypePrefix));
            frame = frame.SubStr(RuleTypePrefix.length());

            if (IsNumeric(frame.at(frame.length() - 1))) {
                frame = frame.Chop(1);
            }
            Y_ENSURE(!IsNumeric(frame.at(frame.length() - 1)));

            stack.emplace_back(ruleIdByName.at(frame));
        }
        return stack;
    }

    TFrequencyData LoadFrequencyData() {
        TString text;
        Y_ENSURE(NResource::FindExact("rules_corr_basic.json", &text));

        TFrequencyData data;

        for (auto& item : LoadFrequencyItems(text)) {
            if (item.Parent == JsonParentFunc || item.Parent == JsonParentModuleFunc) {
                auto& name = item.Rule;
                ToLowerInplace(name);
                data.Functions[name] += item.Sum;
            } else if (item.Parent == JsonParentModule) {
                auto& name = item.Rule;
                ToLowerInplace(name);
                data.Modules[name] += item.Sum;
            } else {
                TString& stack = item.Rule;

                TVector<TStringBuf> frames;
                Split(TStringBuf(stack), ".", frames);

                PurifyParserCallStack(frames);

                auto ruleIdStack = ParseRuleNames(frames);
                data.Stacks[ruleIdStack] += item.Sum;
            }
        }

        return data;
    }

}
