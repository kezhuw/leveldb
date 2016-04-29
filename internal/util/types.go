package util

import (
	"reflect"
)

func SameLayoutStructs(t1 reflect.Type, t2 reflect.Type) bool {
	switch {
	case t1.NumField() != t2.NumField():
		return false
	case t1.Size() != t2.Size():
		return false
	case t1.Align() != t2.Align():
		return false
	}
	for i, n := 0, t1.NumField(); i < n; i++ {
		f1, f2 := t1.Field(i), t2.Field(i)
		if f1.Offset != f2.Offset {
			return false
		}
	}
	return true
}
