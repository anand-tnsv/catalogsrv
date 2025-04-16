//
//  Copyright 2006-2019 WebPKI.org (http://webpki.org).
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

// This program verifies the JSON canonicalizer using a test suite
// containing sample data and expected output

package schemastore

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

var testdata string

var failures = 0

func read(t *testing.T, fileName string, directory string) []byte {
	data, err := os.ReadFile(filepath.Join(filepath.Join(testdata, directory), fileName))
	check(t, err)
	return data
}

func canonicalizer_verify(t *testing.T, fileName string) {
	actual, err := Transform(read(t, fileName, "input"))
	check(t, err)
	recycled, err2 := Transform(actual)
	check(t, err2)
	expected := read(t, fileName, "output")
	var utf8InHex = "\nFile: " + fileName
	var byteCount = 0
	var next = false
	for _, b := range actual {
		if byteCount%32 == 0 {
			utf8InHex = utf8InHex + "\n"
			next = false
		}
		byteCount++
		if next {
			utf8InHex = utf8InHex + " "
		}
		next = true
		utf8InHex = utf8InHex + fmt.Sprintf("%02x", b)
	}
	fmt.Println(utf8InHex + "\n")
	if !bytes.Equal(actual, expected) || !bytes.Equal(actual, recycled) {
		failures++
		fmt.Println("THE TEST ABOVE FAILED!")
	}
}

func Test_Canonicalizer(t *testing.T) {
	_, executable, _, _ := runtime.Caller(0)
	t.Logf("Running JSON Canonicalizer tests from: %s", executable)
	testdata = filepath.Join(filepath.Dir(executable), "testdata")
	t.Logf(testdata)
	files, err := os.ReadDir(filepath.Join(testdata, "input"))
	check(t, err)
	for _, file := range files {
		canonicalizer_verify(t, file.Name())
	}
	if failures == 0 {
		t.Logf("All tests succeeded!")
	} else {
		t.Logf("\n****** ERRORS: %d *******\n", failures)
	}
}
