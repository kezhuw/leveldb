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
