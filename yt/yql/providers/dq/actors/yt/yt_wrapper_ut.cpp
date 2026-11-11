#include "yt_wrapper.h"
#include "local_job_proxy.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/file.h>

#include <util/system/file.h>
#include <util/system/tempfile.h>

namespace NYql {
namespace {

Y_UNIT_TEST_SUITE(TYtWrapperTest) {
    Y_UNIT_TEST(ResourceDownloadProxyRequiresItsOwnFlagAndSocket) {
        NProto::TDqConfig::TYtBackend backend;
        for (const TString clusterAddress : {"my_great_proxy_address1", "my_great_proxy_address2"}) {
            for (const bool tables : {false, true}) {
                backend.SetUseLocalJobProxyForTableReads(tables);
                backend.SetUseLocalJobProxyForResourceDownloads(false);
                backend.SetProxyAddress(clusterAddress);
                UNIT_ASSERT(GetResourceDownloadProxySocket(backend, "my_great_proxy_address1", "/slot/proxy.sock").empty());
                backend.SetUseLocalJobProxyForResourceDownloads(true);
                UNIT_ASSERT_VALUES_EQUAL(GetResourceDownloadProxySocket(backend, "my_great_proxy_address1", "/slot/proxy.sock"), clusterAddress == "my_great_proxy_address1" ? "/slot/proxy.sock" : "");
                UNIT_ASSERT(GetResourceDownloadProxySocket(backend, "my_great_proxy_address1", {}).empty());
            }
        }
    }

    Y_UNIT_TEST(ComputesFileMd5WithoutHint)
    {
        TTempFileHandle file;
        {
            TFileOutput output(file.GetName());
            output << "abc";
        }

        const auto result = ComputeFileMd5(
            TFile(file.GetName(), OpenExisting | RdOnly),
            /*contentMd5*/ {});

        UNIT_ASSERT_VALUES_EQUAL(result.Digest, "900150983cd24fb0d6963f7d28e17f72");
        UNIT_ASSERT(!result.ContentMd5Matches.Defined());
    }

    Y_UNIT_TEST(ValidatesFileMd5Hint)
    {
        TTempFileHandle file;
        {
            TFileOutput output(file.GetName());
            output << "abc";
        }

        const auto matchingResult = ComputeFileMd5(
            TFile(file.GetName(), OpenExisting | RdOnly),
            "900150983cd24fb0d6963f7d28e17f72");
        const auto mismatchingResult = ComputeFileMd5(
            TFile(file.GetName(), OpenExisting | RdOnly),
            "900150983CD24FB0D6963F7D28E17F72");

        UNIT_ASSERT_VALUES_EQUAL(matchingResult.Digest, "900150983cd24fb0d6963f7d28e17f72");
        UNIT_ASSERT(matchingResult.ContentMd5Matches.Defined());
        UNIT_ASSERT(*matchingResult.ContentMd5Matches);
        UNIT_ASSERT_VALUES_EQUAL(mismatchingResult.Digest, "900150983cd24fb0d6963f7d28e17f72");
        UNIT_ASSERT(mismatchingResult.ContentMd5Matches.Defined());
        UNIT_ASSERT(!*mismatchingResult.ContentMd5Matches);
    }
}

} // namespace
} // namespace NYql
