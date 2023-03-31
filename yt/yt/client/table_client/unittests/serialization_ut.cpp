#include <yt/yt/client/formats/public.h>
#include <yt/yt/client/formats/format.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/blob_output.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSchemaSerialization, RoundTrip)
{
    const char* schemaString = "<strict=%true;unique_keys=%false>"
        "[{name=a;required=%false;type=int64;};]";
    TTableSchema schema;
    Deserialize(schema, NYTree::ConvertToNode(NYson::TYsonString(TString(schemaString))));

    EXPECT_TRUE(schema.GetStrict());
    EXPECT_FALSE(schema.GetUniqueKeys());
    EXPECT_EQ(1, std::ssize(schema.Columns()));
    EXPECT_EQ("a", schema.Columns()[0].Name());

    NYT::NFormats::TFormat format(NFormats::EFormatType::Json);

    TBlobOutput buffer;
    auto consumer = CreateConsumerForFormat(format, NFormats::EDataType::Tabular, &buffer);

    Serialize(schema, consumer.get());
    consumer->Flush();
    auto ref = buffer.Flush();
    auto buf = ref.ToStringBuf();

    EXPECT_EQ("{\"$attributes\":{\"strict\":true,\"unique_keys\":false},\"$value\":[{\"name\":\"a\",\"required\":false,\"type\":\"int64\",\"type_v3\":{\"type_name\":\"optional\",\"item\":\"int64\"}}]}\n",
             TString(buf.data(), buf.size()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
