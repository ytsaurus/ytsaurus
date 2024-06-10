#pragma once

#include <library/cpp/getopt/small/last_getopt.h>
#include <google/protobuf/message.h>

namespace NGetoptPb {
    // https://clubs.at.yandex-team.ru/arcadia/15089

    // With this functions you can automatically initialize your protobuf message from cmdline.
    // You can specify description, customize long and short options, and do other things
    // using protobuf extended field options.
    //
    // VZHUH, and ...
    //
    //   message TMyProto {
    //       enum EMode {
    //           MODE_SIMPLE = 1;
    //           MODE_COMPLEX = 2;
    //       }
    //
    //       optional string HomeDir    = 1 [default = "/Berkanavt/spider", (NGetoptPb.Conf).Descr = "home dir", (NGetoptPb.Conf).Type = "<path>"];
    //       optional bool   Verbose    = 2 [(NGetoptPb.Conf).Short = "v", (NGetoptPb.Conf).Descr = "be verbose"];
    //       optional uint32 NumThreads = 3;
    //       optional EMode  Mode       = 4 [default = MODE_SIMPLE];
    //   }
    //
    // ...magically turns into...
    //
    //   {-c|--config} <path>          config file path (proto-text TMyProto)
    //   --home-dir <path>             home dir (default: "/Berkanavt/spider")
    //   {-v|--verbose}                be verbose
    //   --num-threads <uint32>
    //   --mode <EMode>                Default: MODE_SIMPLE
    //
    // See ut/testconf.proto for more details
    //

    //
    // Most users need just GetoptPb or GetoptPbOrAbort functions
    //

    struct TGetoptPbSettings {
        // add --config option
        // read proto-text from this config
        // empty ConfPathLong switches off reading from file
        TString ConfPathLong = "config";
        TString ConfPathJson = "config-json";
        TString ConfText = "config-text"; // for text-serialized protobuf in command-line parameter
        char ConfPathShort = 'c';
        char ConfPathJsonShort = '\0';
        bool AllowUnknownFieldsInConfFile = false;
        // command-line option to enable flag AllowUnknownFieldsInConfFile
        TString AllowUnknownFieldsInConfFileLong = "";
        // default path to look for config (i. e. ~/.myprogconf)
        TString DefaultConfPath;
        // don't fail if conf file was not read
        // (usually you'll want it with non-empty DefaultConfPath)
        bool IgnoreConfFileReadErrors = false;

        // add --dump-proto option for dumping config proto to stderr and quit
        TString DumpProtoLong = "dump-proto";
        char DumpProtoShort = '\0';

        // prepend this to long options
        TString OptPrefix;

        // don't require required fields in cmdline (if they are set in config file)
        bool DontRequireRequired = false;

        // fail if non-repeated option repeats
        bool CheckRepeated = false;

        // generate --lowercase-with-hyphens option
        // instead of --CamelCase_LikeInCppStyleGuide (as in .proto file)
        bool PrettyOpts = true;

        // output resulting config to stderr upon parsing cmdline
        // fields with default values are commented
        // actually set fields are hilighted
        bool DumpConfig = true;

        /// use help shortcut as -h instead of -?
        bool UseLetterHelpShortcut = false;

        /// Consider map to be an object, otherwise consider it to be an array of key/value objects
        bool UseMapAsObject = false;

        /// use the same TGetoptPbSettings for top level and subcommands
        bool SubcommandsInheritSettings = false;
    };

    bool GetoptPb(int argc, const char** argv,             // [IN]  cmdline options to parse
                  google::protobuf::Message& msg,          // [OUT] message to initialize with cmdline options
                  TString& errorMsg,                       // [OUT] parsing error explanation
                  const TGetoptPbSettings& settings = {}); // [IN]  parsing options

    namespace NPrivate {
        class THelper;
    }

    //
    // Usage:
    // const TZoraConfig pbZoraConf = GetoptPbOrAbort(argc, argv /*, optional settings here*/);
    //
    NPrivate::THelper GetoptPbOrAbort(int argc, const char** argv, const TGetoptPbSettings& settings = {});

    //
    // Users, who need access to TOpts and TOptsParseResult can use this lower level class
    //
    class TGetoptPb {
    public:
        explicit TGetoptPb(const TGetoptPbSettings& settings = {})
            : Settings(settings)
        {
        }

        // adds options for message descriptor
        void AddOptions(const google::protobuf::Descriptor& descr);
        // the same, but takes a protobuf message
        inline void AddOptions(const google::protobuf::Message& msg) {
            AddOptions(*msg.GetDescriptor());
        }
        // the same, but takes protobuf type
        template <class TProto>
        inline void AddOptions() {
            AddOptions(*TProto::descriptor());
        }

        // parses arguments from cmdline
        bool ParseArgs(int argc, const char** argv, google::protobuf::Message& msg, TString& errorMsg);

        static void DumpMsg(const google::protobuf::Message& msg, IOutputStream& out);

        inline NLastGetopt::TEasySetup& GetOpts() {
            return Opts;
        }

        inline NLastGetopt::TOptsParseResult& GetOptsParseResult() {
            Y_ABORT_UNLESS(OptsParseResult, "TGetoptPb::GetOptsParseResult: no parse result yet - call ParseArgs first");
            return *OptsParseResult;
        }

    private:
        using TSubCommand = std::pair<TString, const google::protobuf::FieldDescriptor&>;
        using TSubCommands = std::vector<TSubCommand>;

        void AddOptionsInt(const google::protobuf::Descriptor& descr,
                           const TString& path,
                           bool optional,
                           THashSet<TString> parsedMessages,
                           TSubCommands& subCommands);

        bool ParseArgsInt(NLastGetopt::TOptsParseResult& optsParseResult,
                          google::protobuf::Message& msg,
                          TString& errorMsg,
                          const TString& path, bool optional,
                          THashSet<TString> parsedMessages,
                          bool& foundAnything,
                          size_t& freeArgIdx);

        bool ParseTextConfig(const TString& serializedConfig, google::protobuf::Message& msg, TString& error) const;

        static void DumpMsgInt(const google::protobuf::Message& msg, IOutputStream& out,
                               int ident, THashSet<TString> parsedMessages);

    private:
        TGetoptPbSettings Settings;
        NLastGetopt::TEasySetup Opts;
        THolder<NLastGetopt::TOptsParseResult> OptsParseResult;
        TSubCommands SubCommands;
        ui32 FreeArgIdx = 0;
        ui32 MinFreeArgs = 0;
        ui32 MaxFreeArgs = 0;
    };

    namespace NPrivate {
        class THelper {
        public:
            const int Argc;
            const char** Argv;
            const TGetoptPbSettings Settings;

            template <class T>
            operator T() const {
                TString errorMsg;
                std::decay_t<T> pbConfig;
                const bool ok = GetoptPb(Argc, Argv, pbConfig, errorMsg, Settings);
                if (!ok) {
                    Cerr << "Can not parse command line options and/or prototext config, explanation: " << errorMsg << Endl;
                    Cerr << "Exiting..." << Endl;
                    exit(17);
                }
                return pbConfig;
            }
        };
    }

}
