GO_LIBRARY()

RESOURCE_FILES(
    PREFIX library/go/httputil/swaggerui/
    swagger-ui-dist/index.html
    swagger-ui-dist/favicon-16x16.png
    swagger-ui-dist/favicon-32x32.png
    swagger-ui-dist/swagger-ui-bundle.js
    swagger-ui-dist/swagger-ui.css
    swagger-ui-dist/swagger-ui-standalone-preset.js
)

SRCS(
    dir.go
    options.go
    swagger.go
)

GO_XTEST_SRCS(
    swagger_example_test.go
    swagger_test.go
)

END()

RECURSE(
    example
    gotest
)
