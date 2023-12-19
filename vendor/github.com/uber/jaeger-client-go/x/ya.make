GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    doc.go
    priority_sampler.go
    tag_sampler.go
)

GO_TEST_SRCS(
    priority_sampler_test.go
    sampler_custom_updater_test.go
    tag_sampler_test.go
)

END()

RECURSE(gotest)
