package util

func CatchError(errp *error) {
	if r := recover(); r != nil {
		if err, ok := r.(error); ok {
			*errp = err
			return
		}
		panic(r)
	}
}

// FirstError returns first no nil error from given errors.
func FirstError(err0, err1 error) error {
	if err0 != nil {
		return err0
	}
	return err1
}
