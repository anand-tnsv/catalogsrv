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

// This program tests the JSON number serializer using both a few discrete
// values as well as the 100 million value test suite

package schemastore

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
)

func check(t *testing.T, e error) {
	if e != nil {
		t.Fatalf(e.Error())
	}
}

// Change the file name to suit your environment
const testFile = "/Users/anandm/mugiliam/hatchcatalogsrv/pkg/api/schemastore/testdata/es6testfile100m.txt"

const invalidNumber = "null"

var conversionErrors int = 0

func verify(t *testing.T, ieeeHex string, expected string) {
	for len(ieeeHex) < 16 {
		ieeeHex = "0" + ieeeHex
	}
	ieeeU64, err := strconv.ParseUint(ieeeHex, 16, 64)
	check(t, err)
	ieeeF64 := math.Float64frombits(ieeeU64)
	es6Created, err := NumberToJSON(ieeeF64)
	if expected == invalidNumber {
		if err == nil {
			panic("Missing error")
		}
		return
	} else {
		check(t, err)
	}
	if es6Created != expected {
		conversionErrors++
		fmt.Println("\n" + ieeeHex)
		fmt.Println(es6Created)
		fmt.Println(expected)
	}
	esParsed, err := strconv.ParseFloat(expected, 64)
	check(t, err)
	if esParsed != ieeeF64 {
		panic("Parsing error ieeeHex: " + ieeeHex + " expected: " + expected)
	}
}

func Test_numbers(t *testing.T) {
	verify(t, "4340000000000001", "9007199254740994")
	verify(t, "4340000000000002", "9007199254740996")
	verify(t, "444b1ae4d6e2ef50", "1e+21")
	verify(t, "3eb0c6f7a0b5ed8d", "0.000001")
	verify(t, "3eb0c6f7a0b5ed8c", "9.999999999999997e-7")
	verify(t, "8000000000000000", "0")
	verify(t, "7fffffffffffffff", invalidNumber)
	verify(t, "7ff0000000000000", invalidNumber)
	verify(t, "fff0000000000000", invalidNumber)

	file, err := os.Open(testFile)
	if errors.Is(err, os.ErrNotExist) {
		// Skip the test if the file does not exist
		t.Skipf("Skipping the 100M test since the file %s does not exist", testFile)
	}
	check(t, err)
	defer file.Close()
	scanner := bufio.NewScanner(file)
	var lineCount int = 0
	for scanner.Scan() {
		lineCount++
		if lineCount%1000000 == 0 {
			t.Logf("line: %d\n", lineCount)
		}
		line := scanner.Text()
		comma := strings.IndexByte(line, ',')
		if comma <= 0 {
			t.Fatalf("Missing comma!")
		}
		verify(t, line[:comma], line[comma+1:])
	}
	check(t, scanner.Err())
	if conversionErrors == 0 {
		t.Logf("\nSuccessful Operation. Lines read: %d\n", lineCount)
	} else {
		t.Logf("\n****** ERRORS: %d *******\n", conversionErrors)
	}
}
