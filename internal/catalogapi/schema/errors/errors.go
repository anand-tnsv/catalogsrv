package errors

func ErrMissingRequiredAttribute(attr string, value ...any) ValidationError {
	return ValidationError{
		Field:  attr,
		Value:  value,
		ErrStr: "missing required attribute",
	}
}

func ErrInvalidStepValue(attr string, value ...any) ValidationError {
	return ValidationError{
		Field: attr,
		Value: value,
		ErrStr: "step value is invalid: must be non-zero and must be in sequence with maxValue and minValue; " +
			"if positive, minValue must be present; if negative, maxValue must be present",
	}
}

func ErrMaxValueLessThanMinValue(attr string, value ...any) ValidationError {
	return ValidationError{
		Field:  attr,
		Value:  value,
		ErrStr: "maxValue must be greater than minValue",
	}
}

func ErrValidationFailed(attr string, value ...any) ValidationError {
	return ValidationError{
		Field:  attr,
		Value:  value,
		ErrStr: "validation failed",
	}
}

func ErrInvalidFieldSchema(attr string, value ...any) ValidationError {
	return ValidationError{
		Field:  attr,
		Value:  value,
		ErrStr: "invalid schema",
	}
}

func ErrInvalidNameFormat(attr string, value ...string) ValidationError {
	var errStr string
	if len(value) == 0 {
		errStr = "invalid name format; allowed characters: [A-Za-z0-9_-]"
	} else {
		errStr = "invalid name format " + InQuotes(value[0]) + "; allowed characters: [A-Za-z0-9_-]"
	}
	return ValidationError{
		Field:  attr,
		Value:  value,
		ErrStr: errStr,
	}
}

func ErrInvalidResourcePath(attr string, value ...any) ValidationError {
	return ValidationError{
		Field:  attr,
		Value:  value,
		ErrStr: "invalid resource path; must start with '/' and contain only alphanumeric characters, underscores, and hyphens",
	}
}

func ErrUnsupportedKind(attr string, value ...string) ValidationError {
	var errStr string
	if len(value) == 0 {
		errStr = "unsupported kind"
	} else {
		errStr = "unsupported kind " + InQuotes(value[0])
	}
	return ValidationError{
		Field:  attr,
		Value:  value,
		ErrStr: errStr,
	}
}

func ErrUnsupportedDataType(attr string, value ...string) ValidationError {
	var errStr string
	if len(value) == 0 {
		errStr = "invalid data type for version"
	} else {
		errStr = "invalid data type " + InQuotes(value[0]) + " for version"
	}
	return ValidationError{
		Field:  attr,
		Value:  value,
		ErrStr: errStr,
	}
}
