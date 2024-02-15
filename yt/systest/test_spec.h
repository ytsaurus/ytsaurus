#pragma once

#include <yt/systest/config.h>
#include <yt/systest/proto/test_spec.pb.h>

namespace NYT::NTest {

NProto::TSystestSpec GenerateSystestSpec(const TTestConfig& config);

NProto::TSystestSpec GenerateShortSystestSpec(const TTestConfig& config);

}
