GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    project1.go
    project2.go
    project3.go
    project4.go
    structs.go
)

END()

RECURSE(
    foo1
    foo2
)
