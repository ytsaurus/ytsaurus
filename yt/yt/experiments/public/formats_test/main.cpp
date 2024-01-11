#include "yt/yt/core/logging/log.h"
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/rpc_proxy/helpers.h>
#include <yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.pb.h>

#include <yt/yt/client/table_client/adapters.h>
#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/value_consumer.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/table_output.h>
#include <yt/yt/client/table_client/versioned_writer.h>
#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt/core/misc/blob.h>
#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/yson/string.h>

#include <util/stream/buffered.h>
#include <util/stream/file.h>

using namespace NYT;
using namespace NYT::NConcurrency;
using namespace NYT::NTableClient;
using namespace NYT::NTableClient::NProto;
using namespace NYT::NYson;
using namespace NYT::NYTree;
using namespace NYT::NFormats;

int main(int argc, char** argv)
{
    if (argc != 6) {
        Cerr << "Usage: " << argv[0] << "(towire|fromwire) FORMAT SCHEMA (unversioned|versioned) ROWSETFILE" << Endl;
        return 1;
    }

    try {
        auto format = ConvertTo<TFormat>(TYsonString(TString(argv[2])));
        auto schema = ConvertTo<TTableSchemaPtr>(TYsonString(TString(argv[3])));

        if (argv[1] == TString{"fromwire"}) {
            NApi::NRpcProxy::NProto::TRowsetDescriptor rowsetDescriptor;
            Y_PROTOBUF_SUPPRESS_NODISCARD rowsetDescriptor.ParseFromString(TFileInput{argv[5]}.ReadAll());

            auto attachments = UnpackRefs(TSharedRef::FromString(Cin.ReadAll()));

            auto output = CreateAsyncAdapter(&Cout);

            if (argv[4] == TString{"versioned"}) {
                auto rowset = NApi::NRpcProxy::DeserializeRowset<TVersionedRow>(
                    rowsetDescriptor,
                    MergeRefsToRef<TDefaultBlobTag>(attachments));

                auto writer = CreateVersionedWriterForFormat(format, schema, output);
                writer->Write(rowset->GetRows());
                WaitFor(writer->Close())
                    .ThrowOnError();
            } else {
                auto rowset = NApi::NRpcProxy::DeserializeRowset<TUnversionedRow>(
                    rowsetDescriptor,
                    MergeRefsToRef<TDefaultBlobTag>(attachments));

                auto writer = CreateSchemafulWriterForFormat(format, schema, output);
                writer->Write(rowset->GetRows());
                WaitFor(writer->Close())
                    .ThrowOnError();
            }
        } else if (argv[1] == TString{"towire"}) {
            TBuildingValueConsumer valueConsumer(
                schema,
                NLogging::TLogger("Wtf"),
                /*convertNullToEntity*/ true, // we are backward compatible here
                ConvertTo<TTypeConversionConfigPtr>(format.Attributes()));
            valueConsumer.SetAggregate(format.Attributes().Get<bool>("aggregate"));
            valueConsumer.SetTreatMissingAsNull(!format.Attributes().Get<bool>("update"));

            TTableOutput output(CreateParserForFormat(
                format,
                &valueConsumer));

            PipeInputToOutput(&Cin, &output, 64_KB);

            NApi::NRpcProxy::NProto::TRowsetDescriptor rowsetDescriptor;

            auto rows = valueConsumer.GetRows();

            auto attachments = NApi::NRpcProxy::SerializeRowset(
                *schema,
                TRange<TUnversionedRow>(rows),
                &rowsetDescriptor);

            Cout << ToString(PackRefs(attachments));

            TFileOutput outputRowSet{argv[5]};
            outputRowSet << rowsetDescriptor.SerializeAsString();
            outputRowSet.Finish();
        } else {
            THROW_ERROR_EXCEPTION("Invalid mode");
        }
    } catch (const std::exception& ex) {
        Cerr << ex.what() << Endl;
        return 2;
    }

    return 0;
}
