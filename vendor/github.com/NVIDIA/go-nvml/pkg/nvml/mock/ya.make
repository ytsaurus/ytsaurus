GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.13.4-0)

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
    dgxb200
    dgxh100
    dgxh200
    gpus
    server
)
