#include "getoptpb.h"
#include "camel2hyphen.h"

#include <library/cpp/colorizer/colors.h>
#include <library/cpp/getoptpb/proto/confoption.pb.h>
#include <library/cpp/protobuf/json/json2proto.h>

#include <google/protobuf/text_format.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/tokenizer.h>

#include <util/stream/file.h>
#include <util/string/ascii.h>
#include <util/string/printf.h>

namespace NGetoptPb {
    namespace {
#define FOREACH_PLAIN_TYPE(what)           \
    what(INT32, Int32, int32, i32);        \
    what(INT64, Int64, int64, i64);        \
    what(UINT32, UInt32, uint32, ui32);    \
    what(UINT64, UInt64, uint64, ui64);    \
    what(DOUBLE, Double, double, double);  \
    what(FLOAT, Float, float, float);      \
    what(STRING, String, string, TString); \
    /**/

#define INIT_DEFVALUE(typeU, type0, typeL, type1)                       \
    case T::CPPTYPE_##typeU:                                            \
        if (fdescr->has_default_value()) {                              \
            DefaultValue = ::ToString(fdescr->default_value_##typeL()); \
        }                                                               \
        if (!TypeStr) {                                                 \
            TypeStr = "<" #typeL ">";                                   \
        }                                                               \
        break

        class TGetOptPbErrorCollector : public google::protobuf::io::ErrorCollector {
        public:
            void AddError(int line, google::protobuf::io::ColumnNumber column, const TString& message) override {
                Stream << "line: " << line << "; column: " << column << "; " << message << "\n";
            }

            TString GetError() {
                return Stream.Str();
            }

        private:
            TStringStream Stream;
        };

        struct TFieldOptions {
            TString Descr;
            TString LongOpt;
            char ShortOpt = '\0';
            TString TypeStr;
            TString DefaultValue;
            bool Required = false;
            bool NoArgument = false;
            bool SubMessage = false;
            bool Repeated = false;
            bool SubCommand = false;
            bool Hidden = false;
            bool FreeArg = false;
            TString Path;

            bool Init(const google::protobuf::FieldDescriptor* fdescr, bool prettyOpts, const TString& optPrefix, const TString& path, bool optional) {
                auto& ann = fdescr->options().GetExtension(Conf);
                if (ann.GetIgnore()) {
                    return false;
                }
                TString autoName = (prettyOpts ? CamelToHyphen(fdescr->name()) : fdescr->name());

                Repeated = fdescr->is_repeated();

                if (ann.GetDescr()) {
                    Descr = ann.GetDescr();
                } else if (Repeated) {
                    Descr = "(repeatable)";
                }
                LongOpt = optPrefix + path + (ann.GetLong() ? ann.GetLong() : autoName);
                Path = path + (ann.GetPath() ? ann.GetPath() : (ann.HasPath() ? "" : (autoName + "-")));
                ShortOpt = ann.GetShort() ? ann.GetShort()[0] : '\0';
                TypeStr = ann.GetType();
                DefaultValue = "";
                Required = !optional && fdescr->is_required();
                NoArgument = false;
                Hidden = ann.GetHidden();
                FreeArg = ann.GetFreeArg();

                switch (fdescr->cpp_type()) {
                    using T = ::google::protobuf::FieldDescriptor::CppType;

                    FOREACH_PLAIN_TYPE(INIT_DEFVALUE);

                    case T::CPPTYPE_BOOL:
                        if (fdescr->default_value_bool()) {
                            DefaultValue = "yes";
                            if (!TypeStr) {
                                TypeStr = "yes|no";
                            }
                        } else {
                            if (!ann.GetTogglable()) {
                                NoArgument = true;
                            }
                        }
                        break;

                    case T::CPPTYPE_ENUM:
                        if (fdescr->has_default_value()) {
                            TString altVal = fdescr->default_value_enum()->options().GetExtension(Val);
                            DefaultValue = altVal ? altVal : fdescr->default_value_enum()->name();
                        }
                        if (!TypeStr) {
                            TypeStr = Sprintf("<%s>", fdescr->enum_type()->name().data());
                        }
                        break;

                    case T::CPPTYPE_MESSAGE:
                        if (Repeated) {
                            return false;
                        }
                        if (FreeArg) {
                            return false;
                        }
                        if (ann.GetSubCommand()) {
                            SubCommand = true;
                        } else {
                            SubMessage = true;
                        }
                        return true;
                    default:
                        //unsupported type
                        return false;
                }
                return true;
            }

        }; // struct TFieldOptions

        const google::protobuf::EnumValueDescriptor* FindEnumValue(const google::protobuf::EnumDescriptor* descr, const TString val) {
            if (!val) {
                return nullptr;
            }
            int intVal = 0;
            bool doIntVal = TryFromString<int>(val, intVal);

            TString v = to_lower(val);
            for (int i = 0; i < descr->value_count(); ++i) {
                auto vdescr = descr->value(i);
                if (doIntVal) {
                    if (intVal == vdescr->number()) {
                        return vdescr;
                    } else {
                        continue;
                    }
                }
                TString lval = to_lower(vdescr->name());
                if (v == lval) {
                    return vdescr;
                }
                lval = to_lower(vdescr->options().GetExtension(Val));
                if (v == lval) {
                    return vdescr;
                }
            }
            return nullptr;
        }
    }

    void TGetoptPb::AddOptions(const google::protobuf::Descriptor& mdescr) {
        if (Settings.UseLetterHelpShortcut) {
            Opts.AddHelpOption('h');
        }

        TString title = mdescr.options().GetExtension(Title);
        if (title) {
            Opts.SetTitle(title);
        }

        TString examples = mdescr.options().GetExtension(Examples);
        if (examples) {
            Opts.SetExamples(examples);
        }

        if (Settings.ConfPathLong) {
            TString descr = Sprintf("config file path (proto-text %s)", mdescr.full_name().data());
            TString defConfPath = (Settings.DefaultConfPath ?
                                   Settings.DefaultConfPath :
                                   mdescr.options().GetExtension(DefaultConfPath));
            auto &o = Opts.AddLongOption(Settings.ConfPathShort,
                                         Settings.ConfPathLong,
                                         descr)
                .RequiredArgument("<path>");
            if (defConfPath) {
                o.DefaultValue(defConfPath);
            }
        }
        if (Settings.ConfPathJson) {
            TString descr = Sprintf("config file path (json %s)", mdescr.full_name().data());
            Opts.AddLongOption(Settings.ConfPathJsonShort,
                               Settings.ConfPathJson,
                               descr)
                .RequiredArgument("<path>");
        }
        if (Settings.ConfText) {
            TString descr = Sprintf("text-serialized config %s", mdescr.full_name().data());
            Opts.AddLongOption(Settings.ConfText,
                               descr)
                .RequiredArgument("<string>");
        }

        if (Settings.AllowUnknownFieldsInConfFileLong) {
            TString descr = Sprintf("allow unknown fields in config %s", mdescr.full_name().data());
            Opts.AddLongOption(Settings.AllowUnknownFieldsInConfFileLong,
                               descr)
                .OptionalArgument("bool")
                .DefaultValue(Settings.AllowUnknownFieldsInConfFile)
                .StoreResult(&Settings.AllowUnknownFieldsInConfFile, true);
        }

        if (Settings.DumpProtoLong) {
            TString descr = Sprintf("dump %s proto and exit", mdescr.full_name().data());

            struct TDumpProtoOptHandler: public NLastGetopt::IOptHandler {
                void HandleOpt(const NLastGetopt::TOptsParser*) override {
                    Cerr << Msg << Endl;
                    exit(17);
                }
                TString Msg;
            };

            TDumpProtoOptHandler* hndl = new TDumpProtoOptHandler;
            ::google::protobuf::DebugStringOptions o;
            o.include_comments = true;
            hndl->Msg = mdescr.DebugStringWithOptions(o);

            Opts.AddLongOption(Settings.DumpProtoShort,
                               Settings.DumpProtoLong,
                               descr)
                .NoArgument()
                .Handler(hndl);
        }

        THashSet<TString> parsedMessages;
        AddOptionsInt(mdescr, TString(), false, parsedMessages, SubCommands);

        if (SubCommands.size()) {
            Opts.SetFreeArgsMin(1);
            Opts.ArgPermutation_ = NLastGetopt::REQUIRE_ORDER; // dont trigger on arguments after command name
            TString help;
            for (auto& sc : SubCommands) {
                if (help) {
                    help += " | ";
                }
                help += sc.first;
            }
            Opts.SetFreeArgTitle(0, "command", help);
            Opts.SetFreeArgDefaultTitle("args", "type <command> --help for details");
        } else {
            Opts.SetFreeArgsMin(MinFreeArgs);
            Opts.SetFreeArgsMax(MaxFreeArgs);
        }
    }

    void TGetoptPb::AddOptionsInt(const google::protobuf::Descriptor& mdescr,
                                  const TString& path,
                                  bool optional,
                                  THashSet<TString> parsedMessages,
                                  TSubCommands& subCommands) {
        auto p = parsedMessages.insert(mdescr.full_name());
        Y_ABORT_UNLESS(p.second);

        for (int i = 0; i < mdescr.field_count(); ++i) {
            auto fdescr = mdescr.field(i);

            TFieldOptions fopts;
            if (!fopts.Init(fdescr, Settings.PrettyOpts, Settings.OptPrefix, path, optional)) {
                continue;
            }

            if (fopts.SubMessage) {
                auto& mdescr0 = *fdescr->message_type();
                if (parsedMessages.find(mdescr0.full_name()) == parsedMessages.end()) {
                    // parsedMessages should be copied
                    TSubCommands scUnused;
                    AddOptionsInt(mdescr0, fopts.Path, !fopts.Required, parsedMessages, scUnused);
                }
            } else if (fopts.SubCommand) {
                auto& mdescr0 = *fdescr->message_type();
                if (parsedMessages.find(mdescr0.full_name()) == parsedMessages.end()) {
                    subCommands.push_back(TSubCommand(fopts.LongOpt, *fdescr));
                }
            } else if (fopts.FreeArg) {
                bool optional = !fopts.Required || fopts.DefaultValue || fopts.NoArgument;
                if (!optional) {
                    MinFreeArgs++;
                }
                if (fopts.Repeated) {
                    Opts.SetDefaultFreeArgTitle(fopts.TypeStr);
                    MaxFreeArgs = NLastGetopt::TEasySetup::UNLIMITED_ARGS;
                } else {
                    Opts.SetFreeArgTitle(FreeArgIdx, fopts.TypeStr, fopts.Descr, optional);
                    if (MaxFreeArgs != NLastGetopt::TEasySetup::UNLIMITED_ARGS) {
                        MaxFreeArgs++;
                    }
                }
            } else {
                auto& o = Opts.AddLongOption(fopts.ShortOpt, fopts.LongOpt, fopts.Descr);
                if (fopts.TypeStr) {
                    o.RequiredArgument(fopts.TypeStr);
                }
                if (fopts.DefaultValue) {
                    o.DefaultValue(fopts.DefaultValue);
                }
                if (fopts.NoArgument) {
                    o.NoArgument();
                }
            }
        }
    }

    bool TGetoptPb::ParseArgs(int argc, const char** argv,
                              google::protobuf::Message& msg,
                              TString& errorMsg) {
        try {
            OptsParseResult.Reset(new NLastGetopt::TOptsParseResultException(&Opts, argc, argv));
        } catch (const yexception& e) {
            errorMsg = Sprintf("Couldn't parse command line arguments:\n%s", e.what());
            return false;
        } catch (...) {
            errorMsg = Sprintf("Couldn't parse command line arguments: unknown exception");
            return false;
        }

        // try to read from file
        if (Settings.ConfPathLong) {
            if (OptsParseResult->Has(Settings.ConfPathLong, true)) { // if path is set in argc OR default is set
                bool res = false;
                TString parseError;
                TString confPath = OptsParseResult->Get(Settings.ConfPathLong);
                try {
                    TUnbufferedFileInput cFile(confPath);
                    TString txt = cFile.ReadAll();
                    res = ParseTextConfig(txt, msg, parseError);
                } catch (TFileError &) {
                }
                if (!res && !Settings.IgnoreConfFileReadErrors) {
                    errorMsg = Sprintf("Couldn't parse config file %s: %s\n", confPath.data(), parseError.c_str());
                    return false;
                }
            }
        }
        if (Settings.ConfPathJson) {
            TString confPath = OptsParseResult->GetOrElse(Settings.ConfPathJson, "");
            if (confPath) {
                TUnbufferedFileInput cFile(confPath);
                NProtobufJson::TJson2ProtoConfig cfg;
                cfg.AllowUnknownFields = Settings.AllowUnknownFieldsInConfFile;
                cfg.CheckRequiredFields = false;
                cfg.MapAsObject = Settings.UseMapAsObject;
                try {
                    NProtobufJson::Json2Proto(cFile, msg, cfg);
                } catch (std::exception& e) {
                    if (!Settings.IgnoreConfFileReadErrors) {
                        errorMsg = Sprintf("Couldn't parse config from --%s parameter: %s\n", confPath.c_str(), e.what());
                        return false;
                    }
                }
            }
        }
        if (Settings.ConfText) {
            TString confText = OptsParseResult->GetOrElse(Settings.ConfText, "");
            if (confText) {
                TString parseError;
                bool res = ParseTextConfig(confText, msg, parseError);
                if (!res) {
                    errorMsg = Sprintf("Couldn't parse config from --%s parameter: %s\n", Settings.ConfText.data(), parseError.c_str());
                    return false;
                }
            }
        }

        // cmdline options override options from config
        THashSet<TString> parsedMessages;
        bool foundAnything;
        size_t freeArgIdx = 0;
        bool success = ParseArgsInt(*OptsParseResult, msg, errorMsg, "", false, parsedMessages, foundAnything, freeArgIdx);
        if (!success) {
            return false;
        }

        if (!SubCommands.empty()) {
            auto args = OptsParseResult->GetFreeArgs();
            size_t pos = OptsParseResult->GetFreeArgsPos();
            Y_ASSERT(!args.empty());
            for (auto& sc : SubCommands) {
                if (sc.first == args[0]) {
                    // TODO: do we need to change TGetoptPbSettings?
                    TGetoptPbSettings subSettings;
                    if (Settings.SubcommandsInheritSettings) {
                        subSettings = Settings;
                    } else {
                        subSettings.UseLetterHelpShortcut = Settings.UseLetterHelpShortcut;
                        subSettings.CheckRepeated = Settings.CheckRepeated;
                        subSettings.DontRequireRequired = Settings.DontRequireRequired;
                    }
                    TGetoptPb subGetOpt(subSettings);
                    subGetOpt.AddOptions(*sc.second.message_type());
                    return subGetOpt.ParseArgs(argc - pos, argv + pos,
                                               *msg.GetReflection()->MutableMessage(&msg, &sc.second),
                                               errorMsg);
                }
            }
            errorMsg = Sprintf("Unknown command: %s", args[0].data());
            return false;
        } else {
            if (freeArgIdx != OptsParseResult->GetFreeArgCount()) {
                errorMsg = TString::Join("too many free arguments");
                return false;
            }
        }

        return true;
    }

#define SET_OPT_VALUE(typeU, typeL, type0, typeC)                           \
    case T::CPPTYPE_##typeU: {                                              \
        if (fopts.Repeated) {                                               \
            for (const char* val : opr->Values()) {                         \
                refl->Add##typeL(&msg, fdescr, FromString<typeC>(val));     \
            }                                                               \
        } else {                                                            \
            refl->Set##typeL(&msg, fdescr, FromString<typeC>(opr->Back())); \
        }                                                                   \
        break;                                                              \
    }

#define SET_FREE_ARG_VALUE(typeU, typeL, type0, typeC)                                   \
    case T::CPPTYPE_##typeU: {                                                           \
        if (fopts.Repeated) {                                                            \
            for (; freeArgIdx < freeArgs.size(); freeArgIdx++) {                         \
                refl->Add##typeL(&msg, fdescr, FromString<typeC>(freeArgs[freeArgIdx])); \
            }                                                                            \
        } else {                                                                         \
            refl->Set##typeL(&msg, fdescr, FromString<typeC>(freeArgs[freeArgIdx++]));   \
        }                                                                                \
        break;                                                                           \
    }

    bool TGetoptPb::ParseArgsInt(NLastGetopt::TOptsParseResult& optsParseResult,
                                 google::protobuf::Message& msg,
                                 TString& errorMsg,
                                 const TString& path, bool optional,
                                 THashSet<TString> parsedMessages,
                                 bool& foundAnything, size_t& freeArgIdx) {
        foundAnything = false;

        const google::protobuf::Descriptor& mdescr = *msg.GetDescriptor();
        const google::protobuf::Reflection* refl = msg.GetReflection();
        const auto freeArgs = optsParseResult.GetFreeArgs();

        auto p = parsedMessages.insert(mdescr.full_name());
        Y_ABORT_UNLESS(p.second);

        for (int i = 0; i < mdescr.field_count(); ++i) {
            auto fdescr = mdescr.field(i);

            TFieldOptions fopts;
            if (!fopts.Init(fdescr, Settings.PrettyOpts, Settings.OptPrefix, path, optional)) {
                continue;
            }

            if (fopts.SubCommand) {
                continue;
            }
            if (fopts.SubMessage) {
                if (parsedMessages.find(fdescr->message_type()->full_name()) == parsedMessages.end()) {
                    const google::protobuf::Reflection* refl = msg.GetReflection();

                    THolder< ::google::protobuf::Message> msg0(refl->GetMessage(msg, fdescr).New());
                    msg0->CopyFrom(refl->GetMessage(msg, fdescr));
                    bool foundAnything2 = false;
                    bool ok = ParseArgsInt(optsParseResult, *msg0, errorMsg, fopts.Path, !fopts.Required, parsedMessages, foundAnything2, freeArgIdx);
                    if (!ok) {
                        return false;
                    }
                    if (foundAnything2) {
                        refl->MutableMessage(&msg, fdescr)->CopyFrom(*msg0);
                        foundAnything = true;
                    }
                }
                continue;
            }
            if (fopts.FreeArg) {
                if (freeArgIdx == freeArgs.size()) {
                    if (fopts.Required && !fopts.DefaultValue && !fopts.NoArgument) {
                        errorMsg = Sprintf("field %s is required, but there are not enough free arguments",
                                           fdescr->name().c_str());
                        return false;
                    }
                    continue;
                }

                foundAnything = true;
                switch (fdescr->cpp_type()) {
                    using T = ::google::protobuf::FieldDescriptor::CppType;

                    FOREACH_PLAIN_TYPE(SET_FREE_ARG_VALUE);
                    SET_FREE_ARG_VALUE(BOOL, Bool, bool, bool);

                    case T::CPPTYPE_ENUM: {
                        if (fdescr->is_repeated()) {
                            for (; freeArgIdx < freeArgs.size(); freeArgIdx++) {
                                const auto& val = freeArgs[freeArgIdx];
                                auto enumValue = FindEnumValue(fdescr->enum_type(), val);
                                if (enumValue) {
                                    refl->AddEnum(&msg, fdescr, enumValue);
                                } else {
                                    errorMsg = Sprintf("there is no value %s in enum %s",
                                                       val.c_str(), fdescr->enum_type()->name().c_str());
                                    return false;
                                }
                            }
                        } else {
                            const auto& val = freeArgs[freeArgIdx++];
                            auto enumValue = FindEnumValue(fdescr->enum_type(), val);
                            if (enumValue) {
                                refl->SetEnum(&msg, fdescr, enumValue);
                            } else {
                                errorMsg = Sprintf("there is no value %s in enum %s",
                                                   val.c_str(), fdescr->enum_type()->name().c_str());
                                return false;
                            }
                        }
                    } break;

                    default:
                        // unsupported type? but we already checked
                        Y_ABORT_UNLESS(false);
                }
                continue;
            }

            const NLastGetopt::TOptParseResult* opr = optsParseResult.FindLongOptParseResult(fopts.LongOpt);
            if (opr) {
                if (Settings.CheckRepeated && !fopts.Repeated && opr->Count() > 1) {
                    errorMsg = Sprintf("field %s is not repeated", fdescr->name().c_str());
                    return false;
                }
                foundAnything = true;
                switch (fdescr->cpp_type()) {
                    using T = ::google::protobuf::FieldDescriptor::CppType;

                    FOREACH_PLAIN_TYPE(SET_OPT_VALUE);

                    case T::CPPTYPE_BOOL:
                        if (fopts.Repeated) {
                            for (const char* val : opr->Values()) {
                                refl->AddBool(&msg, fdescr, FromString<bool>(val));
                            }
                        } else if (fopts.NoArgument) {
                            refl->SetBool(&msg, fdescr, true);
                        } else {
                            refl->SetBool(&msg, fdescr, FromString<bool>(opr->Back()));
                        }
                        break;

                    case T::CPPTYPE_ENUM: {
                        if (fopts.Repeated) {
                            for (const char* val : opr->Values()) {
                                auto enumValue = FindEnumValue(fdescr->enum_type(), val);
                                if (enumValue) {
                                    refl->AddEnum(&msg, fdescr, enumValue);
                                } else {
                                    errorMsg = Sprintf("there is no value %s in enum %s",
                                                       val, fdescr->enum_type()->name().data());
                                    return false;
                                }
                            }
                        } else {
                            auto enumValue = FindEnumValue(fdescr->enum_type(), opr->Back());
                            if (enumValue) {
                                refl->SetEnum(&msg, fdescr, enumValue);
                            } else {
                                errorMsg = Sprintf("there is no value %s in enum %s",
                                                   opr->Back(), fdescr->enum_type()->name().data());
                                return false;
                            }
                        }
                    } break;

                    default:
                        // unsupported type? but we already checked
                        Y_ABORT_UNLESS(false);
                }
            }
            if (!Settings.DontRequireRequired && fopts.Required && !refl->HasField(msg, fdescr)) {
                errorMsg = Sprintf("Required field '%s'(--%s option) is missing\n", fdescr->name().c_str(), fopts.LongOpt.c_str());
                return false;
            }
        }

        return true;
    }

    bool TGetoptPb::ParseTextConfig(const TString& serializedConfig, google::protobuf::Message& msg, TString& error) const {
        ::google::protobuf::TextFormat::Parser parser;
        if (Settings.AllowUnknownFieldsInConfFile) {
            parser.AllowUnknownField(true);
        }
        TGetOptPbErrorCollector collector;
        parser.RecordErrorsTo(&collector);

        auto result = parser.ParseFromString(serializedConfig, &msg);
        error = collector.GetError();
        return result;
    }

    void TGetoptPb::DumpMsg(const google::protobuf::Message& msg, IOutputStream& out) {
        THashSet<TString> parsedMessages;
        DumpMsgInt(msg, out, 0, parsedMessages);
    }

    void TGetoptPb::DumpMsgInt(const google::protobuf::Message& msg, IOutputStream& out, int ident,
                               THashSet<TString> parsedMessages) {
        const google::protobuf::Descriptor& mdescr = *msg.GetDescriptor();

        auto p = parsedMessages.insert(mdescr.full_name());
        Y_ABORT_UNLESS(p.second);

        for (int i1 = 0; i1 < mdescr.field_count(); ++i1) {
            auto fdescr = mdescr.field(i1);

            TFieldOptions fopts;
            if (!fopts.Init(fdescr, false, "", "", false)) {
                continue;
            }

            auto& colors = NColorizer::AutoColors(out);

            const google::protobuf::Reflection* refl = msg.GetReflection();

            if (fopts.SubMessage || fopts.SubCommand) {
                bool hasField = refl->HasField(msg, fdescr);
                if (fopts.SubCommand && !hasField) {
                    continue;
                }
                if (parsedMessages.find(fdescr->message_type()->full_name()) == parsedMessages.end()) {
                    const google::protobuf::Message& msg0 = refl->GetMessage(msg, fdescr);

                    const TStringBuf b = (hasField ? colors.GreenColor() : "# ");
                    const TStringBuf e = (hasField ? colors.OldColor() : "");
                    out << TString(ident * 2, ' ')
                        << b << fdescr->name() << " {" << e << Endl;
                    DumpMsgInt(msg0, out, ident + 1, parsedMessages);
                    out << TString(ident * 2, ' ')
                        << b << "}" << e << Endl;
                }
                continue;
            }

            google::protobuf::TextFormat::Printer printer;
            int repsize = fopts.Repeated ? refl->FieldSize(msg, fdescr) : 0;
            bool hasfield = fopts.Repeated ? repsize : refl->HasField(msg, fdescr);

            for (int i2 = repsize ? 0 : -1; repsize && i2 < repsize || i2 == -1; ++i2) {
                TString str;
                if (!fopts.Repeated || repsize > 0) {
                    if (!fopts.Hidden) {
                        printer.PrintFieldValueToString(msg, fdescr, i2, &str);
                    } else {
                        str = TStringBuf{"***hidden***"};
                    }
                }

                const TStringBuf b = (hasfield ? colors.GreenColor() : "# ");
                const TStringBuf e = (hasfield ? colors.OldColor() : "");

                out << TString(ident * 2, ' ')
                    << b << fdescr->name() << ": " << str << e << Endl;
            }
        }
    }

    bool GetoptPb(int argc, const char** argv,
                  google::protobuf::Message& msg,
                  TString& errorMsg,
                  const TGetoptPbSettings& settings) {
        TGetoptPb c(settings);
        c.AddOptions(msg);
        bool ret = c.ParseArgs(argc, argv, msg, errorMsg);
        if (ret && settings.DumpConfig) {
            Cerr << "Using settings:\n"
                 << "====================\n";
            c.DumpMsg(msg, Cerr);
            Cerr << "====================\n";
        }
        return ret;
    }

    NPrivate::THelper GetoptPbOrAbort(int argc, const char** argv, const TGetoptPbSettings& settings) {
        return {argc, argv, settings};
    }

}
