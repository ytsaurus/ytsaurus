#include <yt/core/test_framework/framework.h>

#include <yt/server/http_proxy/helpers.h>

#include <yt/core/misc/error.h>

namespace NYT {
namespace NHttpProxy {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TTestParseQuery, Sample)
{
    ParseQueryString("path[10]=0");

    EXPECT_THROW(ParseQueryString("path[10]=0&path[a]=1"), TErrorException);

    ParseQueryString("path=//tmp/home/comdep-analytics/chaos-ad/adhoc/advertanalytics/4468/round_geo_detailed2&output_format[$value]=schemaful_dsv&output_format[$attributes][enable_column_names_header]=true&output_format[$attributes][missing_value_mode]=print_sentinel&output_format[$attributes][missing_value_sentinel]=&output_format[$attributes][columns][0]=auctions_media&output_format[$attributes][columns][1]=auctions_perf&output_format[$attributes][columns][2]=auctions_unsold&output_format[$attributes][columns][3]=block_id&output_format[$attributes][columns][4]=block_shows_media&output_format[$attributes][columns][5]=block_shows_perf&output_format[$attributes][columns][6]=block_shows_unsold&output_format[$attributes][columns][7]=cpm_media&output_format[$attributes][columns][8]=cpm_perf&output_format[$attributes][columns][9]=domain_root&output_format[$attributes][columns][10]=has_geo_cpms");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NHttpProxy
} // namespace NYT
