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

	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

// PortableLogicalType describes a Beam logical type using its portable URN.
//
// Unlike the legacy Go LogicalType, a portable logical type is identified by
// the Beam URN that appears on the wire. Representation and argument metadata
// are supplied as pipeline schema protos so they can be preserved exactly.
type PortableLogicalType interface {
	URN() string
	GoType() reflect.Type
	Representation() *pipepb.FieldType
	ArgumentType() *pipepb.FieldType
	Argument() *pipepb.FieldValue
}

// PortableLogicalTypeConverter is an optional extension implemented by logical
// types that need to translate values between their language and representation
// types. Keeping conversion separate from PortableLogicalType preserves a small
// metadata-only interface for logical types that are handled by an existing
// coder/provider while allowing new implementations to opt into automatic
// conversion.
type PortableLogicalTypeConverter interface {
	ToStorageValue(reflect.Value) (reflect.Value, error)
	ToGoValue(reflect.Value) (reflect.Value, error)
}
