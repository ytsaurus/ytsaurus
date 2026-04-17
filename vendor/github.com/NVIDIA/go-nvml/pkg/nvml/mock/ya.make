GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.13.0-1)

SRCS(
    computeinstance.go
    device.go
    eventset.go
    extendedinterface.go
    gpmsample.go
    gpuinstance.go
    interface.go
    unit.go
    vgpuinstance.go
    vgputypeid.go
)

END()

RECURSE(
    dgxa100
)
