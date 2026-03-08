package app

import (
	"errors"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"unsafe"
)

// Pascal Name pascal case.
func Pascal(str string) string {
	if str == "" {
		return ""
	}
	length := len(str)
	tmp := make([]byte, 0, length)
	next2upper := true
	for i := 0; i < length; i++ {
		if str[i] == '_' {
			next2upper = true
			continue
		}
		if next2upper && str[i] >= 'a' && str[i] <= 'z' {
			tmp = append(tmp, str[i]-32)
		} else {
			tmp = append(tmp, str[i])
		}
		next2upper = false
	}
	return string(tmp[:])
}

// Camel Name camel case.
func Camel(str string) string {
	if str == "" {
		return ""
	}
	str = Pascal(str)
	return strings.ToLower(str[0:1]) + str[1:]
}

// Underline Name underline case.
func Underline(str string) string {
	if str == "" {
		return ""
	}
	length := len(str)
	tmp := make([]byte, 0, length)
	for i := 0; i < length; i++ {
		if str[i] >= 'A' && str[i] <= 'Z' {
			if i > 0 {
				tmp = append(tmp, '_')
			}
			tmp = append(tmp, str[i]+32)
		} else {
			tmp = append(tmp, str[i])
		}
	}
	return *(*string)(unsafe.Pointer(&tmp))
}

func Upper(str string) string {
	return strings.ToUpper(str)
}

func Lower(str string) string {
	return strings.ToLower(str)
}

const (
	Number        = "0123456789"
	EnglishLetter = "AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz"
)

func EnglishLetterLower() []byte {
	letter := make([]byte, 0, 26)
	for i := byte('a'); i < 'z'; i++ {
		letter = append(letter, i)
	}
	return letter
}

func EnglishLetterUpper() []byte {
	letter := make([]byte, 0, 26)
	for i := byte('A'); i <= 'Z'; i++ {
		letter = append(letter, i)
	}
	return letter
}

func EnglishSymbol() []byte {
	letter := make([]byte, 0, 32)
	for i := byte('!'); i <= '/'; i++ {
		letter = append(letter, i)
	}
	for i := byte(':'); i <= '@'; i++ {
		letter = append(letter, i)
	}
	for i := byte('['); i <= '`'; i++ {
		letter = append(letter, i)
	}
	for i := byte('{'); i <= '~'; i++ {
		letter = append(letter, i)
	}
	return letter
}

// RandomString Generates a random string of specified length.
func RandomString(length int, chars ...byte) string {
	count := len(chars)
	if count == 0 {
		chars = append(chars, Number...)
		count = len(chars)
	}
	if length < 1 {
		length = 1
	}
	randoms := make([]byte, 0, length)
	for i := 0; i < length; i++ {
		randoms = append(randoms, chars[rand.IntN(count)])
	}
	return string(randoms)
}

// RemoveCreateFile If the file exists, delete it first and then create it.
func RemoveCreateFile(filename string) (*os.File, error) {
	dir := filepath.Dir(filename)
	if _, err := os.Stat(dir); err != nil {
		if err = os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	if _, err := os.Stat(filename); err == nil {
		if err = os.Remove(filename); err != nil {
			return nil, err
		}
	}
	return os.Create(filename)
}

// WriteFileIfNotExists Write file if not exists.
func WriteFileIfNotExists(filename string, content io.Reader) error {
	dir := filepath.Dir(filename)
	if _, err := os.Stat(dir); err != nil {
		if err = os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	if _, err := os.Stat(filename); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else {
		if IsDebug() {
			_ = os.Remove(filename)
		}
	}
	fil, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer fil.Close()
	_, err = io.Copy(fil, content)
	if err != nil {
		return err
	}
	return nil
}
