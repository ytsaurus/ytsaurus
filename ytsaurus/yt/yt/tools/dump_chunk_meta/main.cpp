#include <yt/yt/server/lib/io/chunk_file_reader.h>
#include <yt/yt/server/lib/io/io_engine.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/yt/library/program/program.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NTools::NDumpChunkMeta {

using namespace NYT::NIO;
using namespace NYT::NChunkClient;

////////////////////////////////////////////////////////////////////////////////

class TProgram
    : public NYT::TProgram
{
public:
    TProgram()
    {
        Opts_
            .AddFreeArgBinding("chunk-meta-file", ChunkMetaFileName_, "Path to chunk meta file");
        Opts_
            .SetFreeArgsNum(1);
    }

private:
    TString ChunkMetaFileName_;

    template <class T>
    void PrintExtension(const TRefCountedChunkMetaPtr& chunkMeta)
    {
        if (auto ext = FindProtoExtension<T>(chunkMeta->extensions())) {
            Cout << "Extension " << ext->GetTypeName() << " (" << TProtoExtensionTag<T>::Value << "):" << Endl;
            Cout << ext->DebugString() << Endl;
        }
    }

    void DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/) override
    {
        auto reader = New<TChunkFileReader>(
            CreateIOEngine(EIOEngineType::ThreadPool, NYTree::INodePtr()),
            NullChunkId,
            ChunkMetaFileName_);

        if (auto chunkId = reader->GetChunkId()) {
            Cout << "ID: " << ToString(reader->GetChunkId()) << Endl;
        }

        auto chunkMeta = reader->GetMeta(/*chunkReadOptions*/ {})
            .Get()
            .ValueOrThrow();

        Cout << "Type: " << ToString(FromProto<EChunkType>(chunkMeta->type())) << Endl;
        Cout << "Format: " << ToString(FromProto<EChunkFormat>(chunkMeta->format())) << Endl;
        Cout << Endl;

        for (const auto& protoExtension : chunkMeta->extensions().extensions()) {
            const auto* extensionDescriptor = IProtobufExtensionRegistry::Get()->FindDescriptorByTag(protoExtension.tag());
            if (!extensionDescriptor) {
                Cout
                    << "WARNING: Unknown protobuf extension " << protoExtension.tag() << ", skipped"
                    << Endl
                    << Endl;
                continue;
            }

            Cout << "Extension " << extensionDescriptor->MessageDescriptor->full_name() << Endl;

            auto* messageFactory = google::protobuf::MessageFactory::generated_factory();
            const auto* extensionPrototype = messageFactory->GetPrototype(extensionDescriptor->MessageDescriptor);

            std::unique_ptr<google::protobuf::Message> extension(extensionPrototype->New());
            DeserializeProto(extension.get(), TRef::FromString(protoExtension.data()));

            Cout << extension->DebugString() << Endl;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools::NDumpChunkMeta

int main(int argc, const char** argv)
{
    return NYT::NTools::NDumpChunkMeta::TProgram().Run(argc, argv);
}
