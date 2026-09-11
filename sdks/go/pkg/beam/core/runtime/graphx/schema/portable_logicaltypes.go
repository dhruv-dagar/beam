// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
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

// PortableLogicalType describes a logical type that can be represented in a
// portable Beam schema. It is intentionally separate from LogicalType for now:
// LogicalType is the existing Go schema-registration abstraction, while this
// interface models the wire-level contract needed for cross-SDK schemas.
type PortableLogicalType interface {
	// URN returns the portable Beam logical type URN.
	URN() string

	// GoType returns the Go type represented by this logical type.
	GoType() reflect.Type

	// Representation returns the underlying Beam field type used on the wire.
	Representation() *pipepb.FieldType

	// ArgumentType returns the Beam field type used for the logical type
	// argument, or nil for non-parameterized logical types.
	ArgumentType() *pipepb.FieldType

	// Argument returns the argument value, or nil for non-parameterized logical
	// types.
	Argument() *pipepb.FieldValue
}
