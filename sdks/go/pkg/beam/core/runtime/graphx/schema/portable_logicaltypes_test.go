// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schema

import (
	"reflect"
	"testing"

	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

type portableInt int64

type portableRow struct {
	Value portableInt
}

type testPortableLogicalType struct {
	argType *pipepb.FieldType
	arg     *pipepb.FieldValue
}

func (t testPortableLogicalType) URN() string {
	return "beam:test:logical_type:portable_int:v1"
}

func (t testPortableLogicalType) GoType() reflect.Type {
	return reflect.TypeOf(portableInt(0))
}

func (t testPortableLogicalType) Representation() *pipepb.FieldType {
	return &pipepb.FieldType{
		TypeInfo: &pipepb.FieldType_AtomicType{AtomicType: pipepb.AtomicType_INT64},
	}
}

func (t testPortableLogicalType) ArgumentType() *pipepb.FieldType {
	return t.argType
}

func (t testPortableLogicalType) Argument() *pipepb.FieldValue {
	return t.arg
}

func TestPortableLogicalTypeRegistry(t *testing.T) {
	r := NewRegistry()
	lt := testPortableLogicalType{
		argType: &pipepb.FieldType{
			TypeInfo: &pipepb.FieldType_AtomicType{AtomicType: pipepb.AtomicType_INT32},
		},
		arg: &pipepb.FieldValue{
			FieldValue: &pipepb.FieldValue_AtomicValue{
				AtomicValue: &pipepb.AtomicTypeValue{
					Value: &pipepb.AtomicTypeValue_Int32{Int32: 9},
				},
			},
		},
	}
	r.RegisterPortableLogicalType(lt)

	got, ok := r.PortableLogicalType(lt.URN())
	if !ok {
		t.Fatalf("portable logical type %q was not registered", lt.URN())
	}
	if got.GoType() != lt.GoType() {
		t.Fatalf("Go type mismatch: got %v, want %v", got.GoType(), lt.GoType())
	}
	if got.ArgumentType() == nil || got.Argument() == nil {
		t.Fatal("parameterized logical type metadata was not retained")
	}

	schema, err := r.FromType(reflect.TypeOf(portableRow{}))
	if err != nil {
		t.Fatalf("FromType() failed: %v", err)
	}
	field := schema.GetFields()[0]
	logical := field.GetType().GetLogicalType()
	if gotURN := logical.GetUrn(); gotURN != lt.URN() {
		t.Fatalf("logical type URN = %q, want %q", gotURN, lt.URN())
	}
	if !reflect.DeepEqual(logical.GetRepresentation(), lt.Representation()) {
		t.Fatalf("logical representation was not preserved: got %v, want %v", logical.GetRepresentation(), lt.Representation())
	}
	if !reflect.DeepEqual(logical.GetArgumentType(), lt.ArgumentType()) {
		t.Fatalf("logical argument type was not preserved: got %v, want %v", logical.GetArgumentType(), lt.ArgumentType())
	}
	if !reflect.DeepEqual(logical.GetArgument(), lt.Argument()) {
		t.Fatalf("logical argument was not preserved: got %v, want %v", logical.GetArgument(), lt.Argument())
	}

	gotType, err := r.ToType(schema)
	if err != nil {
		t.Fatalf("ToType() failed: %v", err)
	}
	if gotType.Field(0).Type != lt.GoType() {
		t.Fatalf("decoded field type = %v, want %v", gotType.Field(0).Type, lt.GoType())
	}
}

func TestLogicalTypeValueConverters(t *testing.T) {
	goType := reflect.TypeOf(portableInt(0))
	storageType := reflect.TypeOf(int64(0))
	lt := ToLogicalTypeWithConverters(
		"beam:test:logical_type:converter:v1",
		goType,
		storageType,
		func(v reflect.Value) (reflect.Value, error) {
			return reflect.ValueOf(v.Int()), nil
		},
		func(v reflect.Value) (reflect.Value, error) {
			return reflect.ValueOf(portableInt(v.Int())), nil
		},
	)

	storage, err := lt.ToStorageValue(reflect.ValueOf(portableInt(42)))
	if err != nil {
		t.Fatalf("ToStorageValue() failed: %v", err)
	}
	if storage.Type() != storageType || storage.Int() != 42 {
		t.Fatalf("storage value = %v (%v), want 42 (%v)", storage.Interface(), storage.Type(), storageType)
	}

	logical, err := lt.ToGoValue(storage)
	if err != nil {
		t.Fatalf("ToGoValue() failed: %v", err)
	}
	if logical.Type() != goType || logical.Int() != 42 {
		t.Fatalf("logical value = %v (%v), want 42 (%v)", logical.Interface(), logical.Type(), goType)
	}
}
