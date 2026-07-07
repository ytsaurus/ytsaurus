GO_LIBRARY()

SRCS(
    embed.go
)

GO_EMBED_PATTERN(
    swagger.json
)

END()
