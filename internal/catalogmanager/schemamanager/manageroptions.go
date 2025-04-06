package schemamanager

type OptionsConfig struct {
	Validate bool
}

type Options func(*OptionsConfig)

func WithValidation(validate ...bool) Options {
	return func(cfg *OptionsConfig) {
		if len(validate) > 0 {
			cfg.Validate = validate[0]
		} else {
			cfg.Validate = true
		}
	}
}
