package swaggerui

type options struct {
	schemeURL     string
	schemeHandler string
	scheme        []byte
}

type Option func(*options)

func WithJSONScheme(scheme []byte) Option {
	return func(o *options) {
		o.scheme = scheme
		o.schemeURL = "." + jsonSchemePath
		o.schemeHandler = jsonSchemePath
	}
}

func WithYAMLScheme(scheme []byte) Option {
	return func(o *options) {
		o.scheme = scheme
		o.schemeURL = "." + yamlSchemePath
		o.schemeHandler = yamlSchemePath
	}
}

func WithRemoteScheme(url string) Option {
	return func(o *options) {
		o.scheme = nil
		o.schemeHandler = ""
		o.schemeURL = url
	}
}
