#include "config.h"

#include <yql/essentials/core/file_storage/file_storage.h>
#include <yql/essentials/utils/log/log.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/string/split.h>
#include <util/system/yassert.h>

#include <utility>


#define SET_PROTO_VALUE_CASE(ProtoTypeEnum, CppType, TypeMethod, Value) \
    case FieldDescriptor::ProtoTypeEnum: { \
        if (field->is_repeated()) { \
            auto splitter = StringSplitter(Value) \
                       .SplitBySet(",") \
                       .SkipEmpty(); \
            for (const auto& it: splitter) { \
                TStringBuf singleValueStr = it.Token(); \
                CppType value = FromString<CppType>(singleValueStr); \
                reflection->Add##TypeMethod(message, field, value); \
            } \
        } else { \
            CppType value = FromString<CppType>(Value); \
            reflection->Set##TypeMethod(message, field, value); \
        } \
        break; \
    }


using namespace ::google::protobuf;

namespace NYql {
namespace {

static const char KEY_VALUE_DELIM = '=';
static const char FIELD_NAME_DELIM = '.';

std::pair<Message*, const FieldDescriptor*> FindFieldDescriptor(
        Message* message, TStringBuf fieldName)
{
    TStringBuf left, right;
    fieldName.Split(FIELD_NAME_DELIM, left, right);

    const Descriptor* descriptor = message->GetDescriptor();
    const FieldDescriptor* field = descriptor->FindFieldByName(TString(left));
    if (right.empty() || field == nullptr) {
        return { message, field };
    }

    if (field->is_repeated()) {
        // XXX: fields with repeated messages not supported 'cause it is
        //      ambiguous how to create multiple messages from flatten
        //      list of fields.
        ythrow yexception() << "repeated fields of messages not supported, "
                               "field name: " << left;
    }

    const Reflection* reflection = message->GetReflection();
    Message* subMessage = reflection->MutableMessage(message, field);
    return FindFieldDescriptor(subMessage, right);
}

void SetFieldValue(
        Message* message, const FieldDescriptor* field,
        TStringBuf fieldName, TStringBuf valueStr)
{
    const Reflection* reflection = message->GetReflection();

    switch (field->cpp_type()) {
    SET_PROTO_VALUE_CASE(CPPTYPE_INT32, int32, Int32, valueStr);
    SET_PROTO_VALUE_CASE(CPPTYPE_INT64, int64, Int64, valueStr);
    SET_PROTO_VALUE_CASE(CPPTYPE_UINT32, uint32, UInt32, valueStr);
    SET_PROTO_VALUE_CASE(CPPTYPE_UINT64, uint64, UInt64, valueStr);
    SET_PROTO_VALUE_CASE(CPPTYPE_DOUBLE, double, Double, valueStr);
    SET_PROTO_VALUE_CASE(CPPTYPE_FLOAT, float, Float, valueStr);
    SET_PROTO_VALUE_CASE(CPPTYPE_BOOL, bool, Bool, valueStr);
    SET_PROTO_VALUE_CASE(CPPTYPE_STRING, TString, String, valueStr);

    case FieldDescriptor::CPPTYPE_ENUM: {
        const EnumDescriptor* enumDescriptor = field->enum_type();
        const EnumValueDescriptor* enumValue =
                enumDescriptor->FindValueByName(TString(valueStr));
        if (enumValue == nullptr) {
            ythrow yexception() << "unknown enum " << enumDescriptor->name()
                                << " value " << valueStr;
        }
        if (field->is_repeated()) {
            reflection->AddEnum(message, field, enumValue);
        } else {
            reflection->SetEnum(message, field, enumValue);
        }
        break;
    }
    default:
        ythrow yexception() << "can't set value of field " << fieldName
                            << " unsupported type " << field->cpp_type_name();
    }
}

void ParseProtoConfig(
        const TVector<TString>& args, TStringBuf preffix, Message* message)
{
    for (const TString& arg: args) {
        if (!arg.StartsWith(preffix)) continue;

        TStringBuf argSB(arg);
        argSB.Skip(preffix.size());

        TStringBuf key, value;
        argSB.Split(KEY_VALUE_DELIM, key, value);

        Message* fieldMsg;
        const FieldDescriptor* fieldDscr;
        std::tie(fieldMsg, fieldDscr) = FindFieldDescriptor(message, key);

        if (fieldDscr) {
            SetFieldValue(fieldMsg, fieldDscr, key, value);
        } else {
            ythrow yexception() << "unknown configuration field: " << key;
        }
    }
}

void PrintUsageAndExit(const NLastGetopt::TOptsParser* parser) {
    parser->PrintUsage();
    Cerr << "You can override configuration options with command line free "
            " args. Use this patterns:\n"
            "    w.{field}={value}  - worker config\n"
            "    g.{field}={value}  - gateways config\n"
            "    fs.{field}={value} - file storage config\n"
         << Endl;
    exit(0);
}

} // namespace

void ParseFreeArgsConfig(const TVector<TString>& args, TMainConfig* config)
{
    ParseProtoConfig(args, TStringBuf("w."), &config->Worker);
    ParseProtoConfig(args, TStringBuf("g."), &config->Gateways);
    ParseProtoConfig(args, TStringBuf("fs."), &config->FileStorage);
}

TMainConfig::TMainConfig(bool forQt)
    : ForQt_(forQt)
{
    AddLongOption("cfg", "worker configuration")
            .Required().RequiredArgument("PATH");
    AddLongOption("gateways-cfg", "gateways configuration")
            .Optional().RequiredArgument("PATH");
    AddLongOption("fs-cfg", "file storage configuration")
            .Optional().RequiredArgument("PATH");
    AddLongOption("stderr-file", "stderr output file")
            .Optional().RequiredArgument("PATH");
    if (ForQt_) {
        AddLongOption("role", "worker role")
            .Required().RequiredArgument("master|core|forker|dq");
    } else {
        AddLongOption("daemonize", "daemonize worker").NoArgument();
    }
    AddLongOption('h', "help", "print usage")
            .NoArgument().Handler(&PrintUsageAndExit);
    AddVersionOption();
}

void TMainConfig::ParseFromCommandLineArgs(int argc, const char* argv[])
{
    // save command line options here because SetProcTitle() will
    // modify memory allocated under them

    int size = 0, count = 0;
    for (int i = 0; argv[i]; i++) {
        size += strlen(argv[i]) + 1;
        count++;
    }

    SavedArgc_ = argc;
    SavedArgv_.reserve(count);
    SavedArgvBuf_.ReserveAndResize(size);

    char* p = SavedArgvBuf_.begin();
    for (int i = 0; argv[i]; i++) {
        size_t size = strlcpy(p, argv[i], SavedArgvBuf_.end() - p);
        SavedArgv_.push_back(p);
        p += (size + 1);
    }
    SavedArgv_.push_back(nullptr);

    ParseAgain();
}

void TMainConfig::ParseAgain()
{
    Y_ABORT_UNLESS(Opts_, "command line options is not parsed yet");

    try {
        NLastGetopt::TOptsParseResult r(this, SavedArgc_, SavedArgv_.data());
        ParseFromTextFormat(r.Get("cfg"), Worker, EParseFromTextFormatOption::AllowUnknownField);
        if (r.Has("gateways-cfg")) {
            ParseFromTextFormat(r.Get("gateways-cfg"), Gateways, EParseFromTextFormatOption::AllowUnknownField);
        }

        if (r.Has("fs-cfg")) {
            LoadFsConfigFromFile(r.Get("fs-cfg"), FileStorage);
        }

        TMainConfig overlapConfig(ForQt_);
        ParseFreeArgsConfig(r.GetFreeArgs(), &overlapConfig);
        Worker.MergeFrom(overlapConfig.Worker);
        Gateways.MergeFrom(overlapConfig.Gateways);
        FileStorage.MergeFrom(overlapConfig.FileStorage);
        if (r.Has("stderr-file")) {
            StdErrFile = r.Get("stderr-file");
        }
        if (ForQt_) {
            QtRole = r.Get<EQtWorkerRole>("role");
            Daemonize = false;
        } else {
            QtRole = {};
            Daemonize = r.Has("daemonize");
        }
        if (!Daemonize) {
            CleanUsernames();
        }

    } catch (...) {
        YQL_LOG(ERROR) << "cannot parse configs: " << CurrentExceptionMessage();
    }
}

void TMainConfig::CleanUsernames() {
    Worker.ClearUser();
    Worker.ClearGroup();
    if (auto executor = Worker.MutableExecutor()) {
        executor->ClearUdfResolverUser();
        executor->ClearUdfResolverGroup();
    }
}

void TMainConfig::ReopenStdErr() {
    if (!StdErrFile.empty()) {
#ifdef __unix__
        TFileHandle fh(StdErrFile, OpenAlways | WrOnly | ForAppend | Seq);
        fh.Duplicate2Posix(STDERR_FILENO);
        fh.Duplicate2Posix(STDOUT_FILENO);
#endif
    }
}

} // namespace NYql
