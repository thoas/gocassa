// This package provides some punk-rock reflection which is not in the stdlib.
package reflect

import (
	"fmt"
	r "reflect"
	"strings"
)

// StructToMap converts a struct to map. The object's default key string
// is the struct field name but can be specified in the struct field's
// tag value. The "cql" key in the struct field's tag value is the key
// name. Examples:
//
//   // Field appears in the resulting map as key "myName".
//   Field int `cql:"myName"`
//
//   // Field appears in the resulting as key "Field"
//   Field int
//
//   // Field appears in the resulting map as key "myName"
//   Field int "myName"
// MapToStruct converts a map to a struct. It is the inverse of the StructToMap

// function. For details see StructToMap.
func MapToStruct(m map[string]interface{}, struc interface{}) error {
	sinfo, err := NewStructInfo(struc)

	if err != nil {
		return err
	}

	for k, v := range m {
		if info, ok := sinfo.FieldsMap[k]; ok {
			structField := sinfo.Value.Field(info.Num)
			if structField.Type().Name() == r.TypeOf(v).Name() {
				structField.Set(r.ValueOf(v))
			}
		}
	}
	return nil
}

// FieldsAndValues returns a list field names and a corresponing list of values
// for the given struct. For details on how the field names are determined please
// see StructToMap.
func FieldsAndValues(val interface{}) ([]string, []interface{}, bool) {
	sinfo, err := NewStructInfo(val)

	if err != nil {
		return nil, nil, false
	}

	fields := make([]string, len(sinfo.FieldsList))
	values := make([]interface{}, len(sinfo.FieldsList))
	for i, info := range sinfo.FieldsList {
		field := sinfo.Value.Field(info.Num)
		fields[i] = info.Key
		values[i] = field.Interface()
	}
	return fields, values, true
}

type fieldInfo struct {
	Key string
	Num int
}

type StructInfo struct {
	// FieldsMap is used to access fields by their key
	FieldsMap map[string]fieldInfo
	// FieldsList allows iteration over the fields in their struct order.
	FieldsList     []fieldInfo
	NullableFields []string
	Value          r.Value
}

func NewStructInfo(val interface{}) (*StructInfo, error) {
	structVal := r.Indirect(r.ValueOf(val))
	kind := structVal.Kind()
	if kind != r.Struct {
		return nil, fmt.Errorf("You must pass a valid struct")
	}

	st := r.Indirect(structVal).Type()

	n := st.NumField()
	fieldsMap := make(map[string]fieldInfo, n)
	fieldsList := make([]fieldInfo, 0, n)
	nullableFields := make([]string, 0, n)
	for i := 0; i != n; i++ {
		field := st.Field(i)
		info := fieldInfo{Num: i}
		tag := field.Tag.Get("cql")
		// If there is no cql specific tag and there are no other tags
		// set the cql tag to the whole field tag
		if tag == "" && strings.Index(string(field.Tag), ":") < 0 {
			tag = string(field.Tag)
		}

		opt := strings.Split(tag, ";")

		if tag != "" {
			info.Key = opt[0]
		} else {
			info.Key = field.Name
		}

		if len(opt) > 1 && opt[1] == "null" {
			nullableFields = append(nullableFields, info.Key)
		}

		if _, found := fieldsMap[info.Key]; found {
			msg := fmt.Sprintf("Duplicated key '%s' in struct %s", info.Key, st.String())
			panic(msg)
		}

		fieldsList = append(fieldsList, info)
		fieldsMap[info.Key] = info
	}

	return &StructInfo{
		fieldsMap,
		fieldsList,
		nullableFields,
		structVal,
	}, nil
}

func (s *StructInfo) ToMap() map[string]interface{} {
	mapVal := make(map[string]interface{}, len(s.FieldsList))

	for _, field := range s.FieldsList {
		mapVal[field.Key] = s.Value.Field(field.Num).Interface()
	}

	return mapVal
}
