package schemamanager

type OptionsConfig struct {
	Validate             bool
	ValidateDependencies bool
	SetDefaultValues     bool
	ObjectLoaders        ObjectLoaders
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

func WithValidateDependencies(validate ...bool) Options {
	return func(cfg *OptionsConfig) {
		if len(validate) > 0 {
			cfg.ValidateDependencies = validate[0]
		} else {
			cfg.ValidateDependencies = true
		}
	}
}

func WithObjectLoaders(loaders ObjectLoaders) Options {
	return func(cfg *OptionsConfig) {
		cfg.ObjectLoaders = loaders
	}
}

func WithDefaultValues(set ...bool) Options {
	return func(cfg *OptionsConfig) {
		if len(set) > 0 {
			cfg.SetDefaultValues = set[0]
		} else {
			cfg.SetDefaultValues = true
		}
	}
}
