package gofmt

import "golang.org/x/tools/imports"

func init() {
	// goimports so configurable...mmmm...
	imports.LocalPrefix = "a.yandex-team.ru"

}

func Format(filename string, src []byte) ([]byte, error) {
	return imports.Process(filename, src, &imports.Options{
		Comments:  true,
		TabIndent: true,
		TabWidth:  8,
		// optimize imports too. It's dangerous, but useful
		FormatOnly: false,
	})
}
