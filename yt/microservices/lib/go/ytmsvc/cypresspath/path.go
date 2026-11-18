package cypresspath

import (
	"strings"

	"go.ytsaurus.tech/yt/go/ypath"
)

// IsCanonical accepts "/" or an absolute Cypress path like //a/b without YPath operators whose node
// names are written in the canonical escaping.
func IsCanonical(path string) bool {
	if path == "/" {
		return true
	}
	tokens, err := ypath.SplitTokens(path)
	if err != nil || len(tokens) < 2 || tokens[0] != "/" || strings.Join(tokens, "") != path {
		return false
	}
	for _, token := range tokens[1:] {
		name, isChild := strings.CutPrefix(token, "/")
		if !isChild {
			return false
		}
		literal, err := ypath.UnescapeLiteral(name)
		if err != nil || literal == "" || ypath.EscapeLiteral(literal) != name {
			return false
		}
	}
	return true
}
