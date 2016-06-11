package utils

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"

	"errors"
)

func PrintErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

func PrintUsageErrorAndExit(message string) {
	fmt.Fprintln(os.Stderr, "ERROR:", message)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}

func StdinAvailable() bool {
	stat, _ := os.Stdin.Stat()
	return (stat.Mode() & os.ModeCharDevice) == 0
}

func Assert(condition bool, testName string) error {
	if !condition {
		return errors.New("Assertion failed: " + testName)
	}
	return nil
}

func InterfaceToString(v interface{}, toStrFunc func(reflect.Value) string) string {
	return toStrFunc(reflect.ValueOf(v))
}

func StructToString(val reflect.Value) string {
	if val.Kind() == reflect.Interface && !val.IsNil() {
		elm := val.Elem()
		if elm.Kind() == reflect.Ptr && !elm.IsNil() && elm.Elem().Kind() == reflect.Ptr {
			val = elm
		}
	}
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	maxLength := 0
	for i := 0; i < val.NumField(); i++ {
		fieldName := val.Type().Field(i).Name
		if len(fieldName) > maxLength {
			maxLength = len(fieldName)
		}
	}

	var output string
	for i := 0; i < val.NumField(); i++ {
		fieldName := val.Type().Field(i).Name
		formatLayout := strings.Repeat(" ", maxLength-len(fieldName))
		value := val.Field(i).Interface()
		output += fmt.Sprintf("\t%s%s : %v\n", fieldName, formatLayout, value)
	}
	return output
}
