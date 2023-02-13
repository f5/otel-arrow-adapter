/*
 * Copyright The OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package schema

import (
	"errors"
	"fmt"
	"math"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"

	carrow "github.com/f5/otel-arrow-adapter/pkg/arrow"
)

var (
	ErrSchemaNotUpToDate = errors.New("schema not up to date")
)

// A window on the last n capacities of each Arrow builder (present in a RecordBuilder) is maintained. The optimal
// capacity of each Arrow builder in a RecordBuilder is determined from the previous observations. The size of this
// window therefore represents the depth of the history used to optimize the construction of the RecordBuilder.
// In the current implementation is based on the max value of the last n observations (i.e. max(nth capacities)).
const builderCapacityWindowSize = 10

// Metadata constants used to mark fields as optional or dictionary.

type MetadataKey int

const (
	Optional MetadataKey = iota
	Dictionary

	OptionalKey   = "#optional"
	DictionaryKey = "#dictionary"
)

// Metadata returns a map of Arrow metadata for the given metadata keys.
func Metadata(keys ...MetadataKey) arrow.Metadata {
	m := make(map[string]string, len(keys))
	for _, key := range keys {
		switch key {
		case Optional:
			m[OptionalKey] = "true"
		case Dictionary:
			m[DictionaryKey] = "true"
		}
	}
	return arrow.MetadataFrom(m)
}

// AdaptiveSchema is a wrapper around [arrow.Schema] that can be used to detect
// dictionary overflow and update the schema accordingly. It also maintains the
// dictionary values for each dictionary field so that the dictionary builders
// can be initialized with the initial dictionary values.
type AdaptiveSchema struct {
	pool memory.Allocator
	cfg  config // configuration

	schema   *arrow.Schema // current schema
	schemaID string        // current schema ID

	// list of all dictionary fields
	dictionaries map[string]*dictionaryField
	// map of dictionary fields that have overflowed (used for test purpose)
	// map = path -> dictionary index type
	dictionariesWithOverflow map[string]string

	fieldCapacities map[string]*BuilderCapacityWindow

	recordBuilder *array.RecordBuilder

	// Statistics
	analyzeCount           int
	updateSchemaCount      int
	recBuilderCreatedCount int
	recBuilderCallCount    int
	dictOverflowCount      int
}

// BuilderCapacityWindow is a moving window on builder length observations collected
// after each batch (window size=len(values)). These n last observations are used
// to compute the maximum capacity of the corresponding builder for the new batch.
type BuilderCapacityWindow struct {
	index  int
	values []int
}

type dictionaryField struct {
	path       string                // string path to the dictionary field (mostly for debugging)
	ids        []int                 // numerical path to the dictionary field (fast access)
	upperLimit uint64                // upper limit of the dictionary index
	dictionary *arrow.DictionaryType // dictionary type
	init       arrow.Array           // initial dictionary values
}

// SchemaUpdate is a struct that contains the information needed to update a schema.
// It contains the index of the dictionary field that needs to be updated, the old
// dictionary type and the new dictionary
type SchemaUpdate struct {
	// path of the dictionary field in the adaptive schema
	DictPath string
	// old dictionary type
	oldDict *arrow.DictionaryType
	// new dictionary type (promoted to a larger index type or string/binary)
	// or nil if the dictionary field has to be replaced by a string or binary.
	newDict *arrow.DictionaryType
	// new upper limit of the dictionary index
	newUpperLimit uint64
}

type config struct {
	initIndexSize  uint64
	limitIndexSize uint64
}

// Option is a function that configures the AdaptiveSchema.
type Option func(*config)

// NewAdaptiveSchema creates a new AdaptiveSchema from an [arrow.Schema]
// and a list of options.
func NewAdaptiveSchema(pool memory.Allocator, schema *arrow.Schema, options ...Option) *AdaptiveSchema {
	cfg := config{
		initIndexSize:  math.MaxUint8,  // default to uint8
		limitIndexSize: math.MaxUint16, // default to uint16
	}
	dictionaries := make(map[string]*dictionaryField)

	for _, opt := range options {
		opt(&cfg)
	}

	schema = initSchema(schema, &cfg)

	fields := schema.Fields()
	for i := 0; i < len(fields); i++ {
		ids := []int{i}
		collectDictionaries(fields[i].Name, ids, &fields[i], &dictionaries)
	}
	return &AdaptiveSchema{
		pool:                     pool,
		cfg:                      cfg,
		schema:                   schema,
		schemaID:                 carrow.SchemaToID(schema),
		dictionaries:             dictionaries,
		dictionariesWithOverflow: make(map[string]string),
		fieldCapacities:          make(map[string]*BuilderCapacityWindow),
		recordBuilder:            array.NewRecordBuilder(pool, schema),
		recBuilderCreatedCount:   1,
	}
}

// Schema returns the current schema.
func (m *AdaptiveSchema) Schema() *arrow.Schema {
	return m.schema
}

// SchemaID returns the current schema ID.
func (m *AdaptiveSchema) SchemaID() string {
	return m.schemaID
}

// RecordBuilder returns a record builder that can be used to build a record corresponding to the current schema.
// The record builder is reused between calls to RecordBuilder if the schema has not been adapted in between.
// Note: the caller is responsible for releasing the record builder.
func (m *AdaptiveSchema) RecordBuilder() *array.RecordBuilder {
	m.recBuilderCallCount++
	m.recordBuilder.Retain()
	return m.recordBuilder
}

// Analyze detects if any of the dictionary fields in the schema have
// overflowed and returns a list of updates that need to be applied to
// the schema. The content of each dictionary array (unique values) is
// also stored in the AdaptiveSchema so that the dictionary builders
// can be initialized with the initial dictionary values.
//
// Returns true if any of the dictionaries have overflowed and false
// otherwise.
func (m *AdaptiveSchema) Analyze(record arrow.Record) (overflowDetected bool, updates []SchemaUpdate) {
	m.analyzeCount++
	arrays := record.Columns()
	overflowDetected = false

	for dictPath, d := range m.dictionaries {
		dict := getDictionaryArray(arrays[d.ids[0]], d.ids[1:])
		if d.init != nil {
			d.init.Release()
		}
		d.init = dict.Dictionary()
		d.init.Retain()
		observedSize := uint64(d.init.Len())
		if observedSize > d.upperLimit {
			m.dictOverflowCount++
			overflowDetected = true
			newDict, newUpperLimit := m.promoteDictionaryType(observedSize, d.dictionary)
			updates = append(updates, SchemaUpdate{
				DictPath:      dictPath,
				oldDict:       d.dictionary,
				newDict:       newDict,
				newUpperLimit: newUpperLimit,
			})
			if newDict == nil {
				m.dictionariesWithOverflow[d.path] = d.dictionary.ValueType.Name()
			} else {
				m.dictionariesWithOverflow[d.path] = newDict.IndexType.Name()
			}
		}
	}

	return overflowDetected, updates
}

// UpdateSchema updates the schema with the provided updates.
func (m *AdaptiveSchema) UpdateSchema(updates []SchemaUpdate) {
	m.updateSchemaCount++
	m.rebuildSchema(updates)

	// update dictionaries based on the updates
	for _, u := range updates {
		m.dictionaries[u.DictPath].upperLimit = u.newUpperLimit
		m.dictionaries[u.DictPath].dictionary = u.newDict
		if u.newDict == nil {
			prevDict := m.dictionaries[u.DictPath].init
			if prevDict != nil {
				prevDict.Release()
				m.dictionaries[u.DictPath].init = nil
			}
		}
	}

	// remove dictionary fields that have been replaced by string/binary
	for path, dict := range m.dictionaries {
		if dict.init == nil {
			delete(m.dictionaries, path)
		}
	}

	// Build a new record builder with the updated schema
	// and transfer the dictionaries from the old record builder
	// to the new one.
	newRecBuilder := array.NewRecordBuilder(m.pool, m.schema)
	if err := CopyDictValuesTo(m.recordBuilder.Fields(), newRecBuilder.Fields()); err != nil {
		panic(err)
	}
	m.recordBuilder.Release()
	m.recordBuilder = newRecBuilder

	m.recBuilderCreatedCount++
}

// CopyDictValuesTo recursively copy the dictionary values from the source array
// builders to the destination array builders.
func CopyDictValuesTo(srcFields []array.Builder, destFields []array.Builder) error {
	if len(srcFields) != len(destFields) {
		panic("The number of fields between the source and destination record builders must be the same")
	}

	for i := 0; i < len(srcFields); i++ {
		srcField := srcFields[i]
		destField := destFields[i]
		if err := copyFieldDictValuesTo(srcField, destField); err != nil {
			return err
		}
	}
	return nil
}

// Recursively copy the dictionary values from the source array builder to the
// destination array builder.
func copyFieldDictValuesTo(srcField array.Builder, destField array.Builder) (err error) {
	if srcField.Type().ID() == arrow.DICTIONARY && destField.Type().ID() != arrow.DICTIONARY {
		// The dictionary has been promoted to a string/binary field.
		return nil
	}

	if srcField.Type().ID() != destField.Type().ID() {
		panic("The source and destination record builders must have the same schema (except for dictionary indices)")
	}

	switch builder := srcField.(type) {
	case *array.StructBuilder:
		for i := 0; i < builder.NumField(); i++ {
			if err = copyFieldDictValuesTo(builder.FieldBuilder(i), destField.(*array.StructBuilder).FieldBuilder(i)); err != nil {
				return
			}
		}
	case *array.ListBuilder:
		if err = copyFieldDictValuesTo(builder.ValueBuilder(), destField.(*array.ListBuilder).ValueBuilder()); err != nil {
			return
		}
	case array.UnionBuilder:
		typeCodes := builder.Type().(arrow.UnionType).TypeCodes()
		for childID := 0; childID < len(typeCodes); childID++ {
			if err = copyFieldDictValuesTo(builder.Child(int(typeCodes[childID])), destField.(array.UnionBuilder).Child(int(typeCodes[childID]))); err != nil {
				return
			}
		}
	case *array.MapBuilder:
		if err = copyFieldDictValuesTo(builder.KeyBuilder(), destField.(*array.MapBuilder).KeyBuilder()); err != nil {
			return err
		}
		if err = copyFieldDictValuesTo(builder.ItemBuilder(), destField.(*array.MapBuilder).ItemBuilder()); err != nil {
			return err
		}
	case array.DictionaryBuilder:
		srcDictArr := builder.NewDictionaryArray()
		defer srcDictArr.Release()
		srcDict := srcDictArr.Dictionary()
		defer srcDict.Release()
		switch dict := srcDict.(type) {
		case *array.String:
			err = destField.(*array.BinaryDictionaryBuilder).InsertStringDictValues(dict)
		case *array.Binary:
			err = destField.(*array.BinaryDictionaryBuilder).InsertDictValues(dict)
		case *array.FixedSizeBinary:
			err = destField.(*array.FixedSizeBinaryDictionaryBuilder).InsertDictValues(dict)
		case *array.Int32:
			err = destField.(*array.Int32DictionaryBuilder).InsertDictValues(dict)
		case *array.Uint32:
			err = destField.(*array.Uint32DictionaryBuilder).InsertDictValues(dict)
		default:
			panic("copyFieldDictValuesTo: unsupported dictionary type " + dict.DataType().Name())
		}
	}
	return nil
}

// Release releases all the dictionary arrays that were stored in the AdaptiveSchema.
func (m *AdaptiveSchema) Release() {
	for _, d := range m.dictionaries {
		if d.init != nil {
			d.init.Release()
		}
	}
	m.recordBuilder.Release()
}

// DictionariesWithOverflow returns a map of dictionary fields that have overflowed and the
// corresponding last promoted type.
func (m *AdaptiveSchema) DictionariesWithOverflow() map[string]string {
	// TODO find a less "intrusive" way to test which dictionaries have overflowed, consider how to remove test-specific functionality from the code
	return m.dictionariesWithOverflow
}

func (m *AdaptiveSchema) ShowStats() {
	println("AdaptiveSchema stats:")
	println("  analyzeCount: ", m.analyzeCount)
	println("  updateSchemaCount: ", m.updateSchemaCount)
	println("  RecordBuilder call: ", m.recBuilderCallCount)
	println("  NewRecordBuilder: ", m.recBuilderCreatedCount)
	println("  dictOverflowCount: ", m.dictOverflowCount)
}

func WithDictInitIndexSize(size uint64) Option {
	return func(cfg *config) {
		cfg.initIndexSize = size
	}
}

func WithDictLimitIndexSize(size uint64) Option {
	return func(cfg *config) {
		cfg.limitIndexSize = size
	}
}

func (m *AdaptiveSchema) promoteDictionaryType(observedSize uint64, existingDT *arrow.DictionaryType) (dictType *arrow.DictionaryType, upperLimit uint64) {
	if observedSize <= math.MaxUint8 {
		dictType = &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Uint8,
			ValueType: existingDT.ValueType,
			Ordered:   false,
		}
		upperLimit = math.MaxUint8
	} else if observedSize <= math.MaxUint16 {
		dictType = &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Uint16,
			ValueType: existingDT.ValueType,
			Ordered:   false,
		}
		upperLimit = math.MaxUint16
	} else if observedSize <= math.MaxUint32 {
		dictType = &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Uint32,
			ValueType: existingDT.ValueType,
			Ordered:   false,
		}
		upperLimit = math.MaxUint32
	} else {
		dictType = &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Uint64,
			ValueType: existingDT.ValueType,
			Ordered:   false,
		}
		upperLimit = math.MaxUint64
	}

	if upperLimit > m.cfg.limitIndexSize {
		dictType = nil
	}
	return dictType, upperLimit
}

// Updates the schema in function of the configuration provided.
func initSchema(schema *arrow.Schema, cfg *config) *arrow.Schema {
	var indexType arrow.DataType
	switch {
	case cfg.initIndexSize == 0:
		indexType = nil
	case cfg.initIndexSize == math.MaxUint8:
		indexType = arrow.PrimitiveTypes.Uint8
	case cfg.initIndexSize == math.MaxUint16:
		indexType = arrow.PrimitiveTypes.Uint16
	case cfg.initIndexSize == math.MaxUint32:
		indexType = arrow.PrimitiveTypes.Uint32
	case cfg.initIndexSize == math.MaxUint64:
		indexType = arrow.PrimitiveTypes.Uint64
	default:
		panic("initSchema: unsupported initial index size")
	}

	oldFields := schema.Fields()
	newFields := make([]arrow.Field, len(oldFields))
	for i := 0; i < len(oldFields); i++ {
		newFields[i] = initField(&oldFields[i], indexType)
	}

	metadata := schema.Metadata()
	return arrow.NewSchema(newFields, &metadata)
}

func (m *AdaptiveSchema) rebuildSchema(updates []SchemaUpdate) {
	// Mapping old dictionary type to new dictionary type
	// Used to identify the dictionary builders that need to be updated
	oldToNewDicts := make(map[*arrow.DictionaryType]*arrow.DictionaryType)
	for _, u := range updates {
		oldToNewDicts[u.oldDict] = u.newDict
	}

	oldFields := m.schema.Fields()
	newFields := make([]arrow.Field, len(oldFields))
	for i := 0; i < len(oldFields); i++ {
		newFields[i] = updateField(&oldFields[i], oldToNewDicts)
	}

	metadata := m.schema.Metadata()
	m.schema = arrow.NewSchema(newFields, &metadata)
	m.schemaID = carrow.SchemaToID(m.schema)
}

// collectSizeBuildersFromRecord collects the size of each internal array present in the record
// passed in parameter. These values will be used to initialize the size builders in the next
// batch.
func (m *AdaptiveSchema) collectSizeBuildersFromRecord(record arrow.Record) {
	arrays := record.Columns()
	schema := record.Schema()
	fields := schema.Fields()
	for i, arr := range arrays {
		field := &fields[i]
		m.collectSizeBuildersFromArray(field.Name, field, arr)
	}
}

func (m *AdaptiveSchema) collectSizeBuildersFromArray(path string, field *arrow.Field, arr arrow.Array) {
	window, found := m.fieldCapacities[path]
	if !found {
		window = NewBuilderCapacityWindow(builderCapacityWindowSize)
		m.fieldCapacities[path] = window
	}
	window.Record(arr.Len())

	switch arr := arr.(type) {
	case *array.Struct:
		structField, ok := field.Type.(*arrow.StructType)
		if !ok {
			panic("collectSizeBuildersFromArray: expected struct field")
		}
		for i := 0; i < arr.NumField(); i++ {
			subField := structField.Field(i)
			m.collectSizeBuildersFromArray(path+"."+subField.Name, &subField, arr.Field(i))
		}
	case *array.List:
		elemField := field.Type.(*arrow.ListType).ElemField()
		m.collectSizeBuildersFromArray(path+"[]", &elemField, arr.ListValues())
	case array.Union:
		variantFields := field.Type.(arrow.UnionType).Fields()
		for i := 0; i < arr.NumFields(); i++ {
			m.collectSizeBuildersFromArray(path+"."+variantFields[i].Name, &variantFields[i], arr.Field(i))
		}
	case *array.Map:
		keyField := field.Type.(*arrow.MapType).KeyField()
		valueField := field.Type.(*arrow.MapType).ItemField()
		m.collectSizeBuildersFromArray(path+".key", &keyField, arr.Keys())
		m.collectSizeBuildersFromArray(path+".value", &valueField, arr.Items())
	}
}

// initSizeBuildersFromRecordBuilder initializes the size of each internal builder in the record builder
// passed in parameter. The previous size of the builders are used to determine the initial sizes.
// The goal is to avoid resizing the builders too often (the default size being 32).
func (m *AdaptiveSchema) initSizeBuildersFromRecordBuilder(recordBuilder *array.RecordBuilder) {
	builders := recordBuilder.Fields()
	schema := recordBuilder.Schema()

	for i, builder := range builders {
		field := schema.Field(i)
		m.initSizeBuildersFromBuilder(field.Name, &field, builder)
	}
}

func (m *AdaptiveSchema) initSizeBuildersFromBuilder(path string, field *arrow.Field, builder array.Builder) {
	window, found := m.fieldCapacities[path]
	capacity := 0
	if found {
		capacity = window.Max()
	}

	builder.Reserve(capacity)

	switch b := builder.(type) {
	case *array.StructBuilder:
		structField, ok := field.Type.(*arrow.StructType)
		if !ok {
			panic("initSizeBuildersFromBuilder: expected struct field")
		}
		for i := 0; i < b.NumField(); i++ {
			subField := structField.Field(i)
			m.initSizeBuildersFromBuilder(path+"."+subField.Name, &subField, b.FieldBuilder(i))
		}
	case *array.ListBuilder:
		elemField := field.Type.(*arrow.ListType).ElemField()
		m.initSizeBuildersFromBuilder(path+"[]", &elemField, b.ValueBuilder())
	case array.UnionBuilder:
		variantFields := field.Type.(arrow.UnionType).Fields()
		for i := 0; i < len(variantFields); i++ {
			m.initSizeBuildersFromBuilder(path+"."+variantFields[i].Name, &variantFields[i], b.Child(i))
		}
	case *array.MapBuilder:
		keyField := field.Type.(*arrow.MapType).KeyField()
		valueField := field.Type.(*arrow.MapType).ItemField()
		m.initSizeBuildersFromBuilder(path+".key", &keyField, b.KeyBuilder())
		m.initSizeBuildersFromBuilder(path+".value", &valueField, b.ItemBuilder())
	}
}

func initField(f *arrow.Field, indexType arrow.DataType) arrow.Field {
	switch t := f.Type.(type) {
	case *arrow.DictionaryType:
		if indexType == nil {
			return arrow.Field{Name: f.Name, Type: t.ValueType, Nullable: f.Nullable, Metadata: f.Metadata}
		} else {
			dictType := &arrow.DictionaryType{
				IndexType: indexType,
				ValueType: t.ValueType,
				Ordered:   t.Ordered,
			}
			return arrow.Field{Name: f.Name, Type: dictType, Nullable: f.Nullable, Metadata: f.Metadata}
		}
	case *arrow.StructType:
		oldFields := t.Fields()
		newFields := make([]arrow.Field, len(oldFields))
		for i := 0; i < len(oldFields); i++ {
			newFields[i] = initField(&oldFields[i], indexType)
		}
		return arrow.Field{Name: f.Name, Type: arrow.StructOf(newFields...), Nullable: f.Nullable, Metadata: f.Metadata}
	case *arrow.ListType:
		elemField := t.ElemField()
		newField := initField(&elemField, indexType)
		return arrow.Field{Name: f.Name, Type: arrow.ListOf(newField.Type), Nullable: f.Nullable, Metadata: f.Metadata}
	case *arrow.SparseUnionType:
		oldFields := t.Fields()
		newFields := make([]arrow.Field, len(oldFields))
		for i := 0; i < len(oldFields); i++ {
			newFields[i] = initField(&oldFields[i], indexType)
		}
		return arrow.Field{Name: f.Name, Type: arrow.SparseUnionOf(newFields, t.TypeCodes()), Nullable: f.Nullable, Metadata: f.Metadata}
	case *arrow.DenseUnionType:
		oldFields := t.Fields()
		newFields := make([]arrow.Field, len(oldFields))
		for i := 0; i < len(oldFields); i++ {
			newFields[i] = initField(&oldFields[i], indexType)
		}
		return arrow.Field{Name: f.Name, Type: arrow.DenseUnionOf(newFields, t.TypeCodes()), Nullable: f.Nullable, Metadata: f.Metadata}
	case *arrow.MapType:
		keyField := t.KeyField()
		newKeyField := initField(&keyField, indexType)
		valueField := t.ItemField()
		newValueField := initField(&valueField, indexType)
		return arrow.Field{Name: f.Name, Type: arrow.MapOf(newKeyField.Type, newValueField.Type), Nullable: f.Nullable, Metadata: f.Metadata}
	default:
		return *f
	}
}

func updateField(f *arrow.Field, dictMap map[*arrow.DictionaryType]*arrow.DictionaryType) arrow.Field {
	switch t := f.Type.(type) {
	case *arrow.DictionaryType:
		if newDict, ok := dictMap[t]; ok {
			if newDict != nil {
				return arrow.Field{Name: f.Name, Type: newDict, Nullable: f.Nullable, Metadata: f.Metadata}
			} else {
				return arrow.Field{Name: f.Name, Type: t.ValueType, Nullable: f.Nullable, Metadata: f.Metadata}
			}
		} else {
			return *f
		}
	case *arrow.StructType:
		oldFields := t.Fields()
		newFields := make([]arrow.Field, len(oldFields))
		for i := 0; i < len(oldFields); i++ {
			newFields[i] = updateField(&oldFields[i], dictMap)
		}
		return arrow.Field{Name: f.Name, Type: arrow.StructOf(newFields...), Nullable: f.Nullable, Metadata: f.Metadata}
	case *arrow.ListType:
		elemField := t.ElemField()
		newField := updateField(&elemField, dictMap)
		return arrow.Field{Name: f.Name, Type: arrow.ListOf(newField.Type), Nullable: f.Nullable, Metadata: f.Metadata}
	case *arrow.SparseUnionType:
		oldFields := t.Fields()
		newFields := make([]arrow.Field, len(oldFields))
		for i := 0; i < len(oldFields); i++ {
			newFields[i] = updateField(&oldFields[i], dictMap)
		}
		return arrow.Field{Name: f.Name, Type: arrow.SparseUnionOf(newFields, t.TypeCodes()), Nullable: f.Nullable, Metadata: f.Metadata}
	case *arrow.DenseUnionType:
		oldFields := t.Fields()
		newFields := make([]arrow.Field, len(oldFields))
		for i := 0; i < len(oldFields); i++ {
			newFields[i] = updateField(&oldFields[i], dictMap)
		}
		return arrow.Field{Name: f.Name, Type: arrow.DenseUnionOf(newFields, t.TypeCodes()), Nullable: f.Nullable, Metadata: f.Metadata}
	case *arrow.MapType:
		keyField := t.KeyField()
		newKeyField := updateField(&keyField, dictMap)
		valueField := t.ItemField()
		newValueField := updateField(&valueField, dictMap)
		return arrow.Field{Name: f.Name, Type: arrow.MapOf(newKeyField.Type, newValueField.Type), Nullable: f.Nullable, Metadata: f.Metadata}
	default:
		return *f
	}
}

func getDictionaryArray(arr arrow.Array, ids []int) *array.Dictionary {
	if len(ids) == 0 {
		return arr.(*array.Dictionary)
	}

	switch arr := arr.(type) {
	case *array.Struct:
		return getDictionaryArray(arr.Field(ids[0]), ids[1:])
	case *array.List:
		return getDictionaryArray(arr.ListValues(), ids)
	case *array.SparseUnion:
		return getDictionaryArray(arr.Field(ids[0]), ids[1:])
	case *array.DenseUnion:
		return getDictionaryArray(arr.Field(ids[0]), ids[1:])
	case *array.Map:
		switch ids[0] {
		case 0: // key
			return getDictionaryArray(arr.Keys(), ids[1:])
		case 1: // value
			return getDictionaryArray(arr.Items(), ids[1:])
		default:
			panic("getDictionaryArray: invalid map field id")
		}
	default:
		panic("getDictionaryArray: unsupported array type `" + arr.DataType().Name() + "`")
	}
}

// collectDictionaries collects recursively all dictionary fields in the schema and returns a list of them.
func collectDictionaries(prefix string, ids []int, field *arrow.Field, dictionaries *map[string]*dictionaryField) {
	switch t := field.Type.(type) {
	case *arrow.DictionaryType:
		(*dictionaries)[prefix] = &dictionaryField{path: prefix, ids: ids, upperLimit: indexUpperLimit(t.IndexType), dictionary: field.Type.(*arrow.DictionaryType)}
	case *arrow.StructType:
		fields := t.Fields()
		for i := 0; i < len(fields); i++ {
			childIds := make([]int, len(ids)+1)
			copy(childIds, ids)
			childIds[len(ids)] = i
			collectDictionaries(prefix+"."+fields[i].Name, childIds, &fields[i], dictionaries)
		}
	case *arrow.ListType:
		field := t.ElemField()
		collectDictionaries(prefix, ids, &field, dictionaries)
	case *arrow.SparseUnionType:
		fields := t.Fields()
		for i := 0; i < len(fields); i++ {
			childIds := make([]int, len(ids)+1)
			copy(childIds, ids)
			childIds[len(ids)] = i
			collectDictionaries(prefix+"."+fields[i].Name, childIds, &fields[i], dictionaries)
		}
	case *arrow.DenseUnionType:
		fields := t.Fields()
		for i := 0; i < len(fields); i++ {
			childIds := make([]int, len(ids)+1)
			copy(childIds, ids)
			childIds[len(ids)] = i
			collectDictionaries(prefix+"."+fields[i].Name, childIds, &fields[i], dictionaries)
		}
	case *arrow.MapType:
		childIds := make([]int, len(ids)+1)
		copy(childIds, ids)
		childIds[len(ids)] = 0
		keyField := t.KeyField()
		collectDictionaries(prefix+".key", childIds, &keyField, dictionaries)

		childIds = make([]int, len(ids)+1)
		copy(childIds, ids)
		childIds[len(ids)] = 1
		itemField := t.ItemField()
		collectDictionaries(prefix+".value", childIds, &itemField, dictionaries)
	}
}

func indexUpperLimit(dt arrow.DataType) uint64 {
	switch dt {
	case arrow.PrimitiveTypes.Uint8:
		return math.MaxUint8
	case arrow.PrimitiveTypes.Uint16:
		return math.MaxUint16
	case arrow.PrimitiveTypes.Uint32:
		return math.MaxUint32
	case arrow.PrimitiveTypes.Uint64:
		return math.MaxUint64
	case arrow.PrimitiveTypes.Int8:
		return math.MaxInt8
	case arrow.PrimitiveTypes.Int16:
		return math.MaxInt16
	case arrow.PrimitiveTypes.Int32:
		return math.MaxInt32
	case arrow.PrimitiveTypes.Int64:
		return math.MaxInt64
	default:
		panic("unsupported index type `" + dt.Name() + "`")
	}
}

// DictionaryOverflowError is returned when the cardinality of a dictionary (or several)
// exceeds the maximum allowed value.
//
// This error is returned by the TracesBuilder.Build method. This error is retryable.
type DictionaryOverflowError struct {
	FieldNames []string
}

func (e *DictionaryOverflowError) Error() string {
	return fmt.Sprintf("dictionary overflow for fields: %v", e.FieldNames)
}

func NewBuilderCapacityWindow(maxNumValues int) *BuilderCapacityWindow {
	return &BuilderCapacityWindow{
		index:  0,
		values: make([]int, maxNumValues),
	}
}

func (w *BuilderCapacityWindow) Record(value int) {
	w.values[w.index] = value
	w.index = (w.index + 1) % cap(w.values)
}

func (w *BuilderCapacityWindow) Max() int {
	max := w.values[0]
	for i := 1; i < len(w.values); i++ {
		if w.values[i] > max {
			max = w.values[i]
		}
	}
	return max
}

func StructFieldBuilder(dt *arrow.StructType, fieldName string, builder *array.StructBuilder) array.Builder {
	if fieldIdx, found := dt.FieldIdx(fieldName); found {
		return builder.FieldBuilder(fieldIdx)
	} else {
		return nil
	}
}

// NewSchemaFrom creates a new schema from a prototype schema and a transformation tree.
func NewSchemaFrom(prototype *arrow.Schema, transformTree *TransformNode) *arrow.Schema {
	protoFields := prototype.Fields()
	fields := make([]arrow.Field, 0, len(protoFields))

	for i := 0; i < len(protoFields); i++ {
		field := NewFieldFrom(&protoFields[i], transformTree.Children[i])
		if field != nil {
			fields = append(fields, *NewFieldFrom(&protoFields[i], transformTree.Children[i]))
		}
	}

	metadata := prototype.Metadata()
	return arrow.NewSchema(fields, &metadata)

}

// NewFieldFrom creates a new field from a prototype field and a transformation tree.
func NewFieldFrom(prototype *arrow.Field, transformNode *TransformNode) *arrow.Field {
	field := prototype

	// remove metadata keys that are only used to specify transformations.

	// apply transformations to the current prototype field.
	// if a transformation returns nil, the field is removed.
	for _, t := range transformNode.transforms {
		field = t.Transform(field)
		if field == nil {
			return nil
		}
	}
	metadata := cleanMetadata(field.Metadata)

	switch dt := field.Type.(type) {
	case *arrow.StructType:
		oldFields := dt.Fields()
		newFields := make([]arrow.Field, 0, len(oldFields))

		for i := 0; i < len(oldFields); i++ {
			newField := NewFieldFrom(&oldFields[i], transformNode.Children[i])
			if newField != nil {
				newFields = append(newFields, *newField)
			}
		}

		return &arrow.Field{Name: field.Name, Type: arrow.StructOf(newFields...), Nullable: field.Nullable, Metadata: metadata}
	case *arrow.ListType:
		elemField := dt.ElemField()
		newField := NewFieldFrom(&elemField, transformNode.Children[0])
		return &arrow.Field{Name: field.Name, Type: arrow.ListOf(newField.Type), Nullable: field.Nullable, Metadata: metadata}
	case arrow.UnionType:
		oldFields := dt.Fields()
		oldTypeCodes := dt.TypeCodes()
		newFields := make([]arrow.Field, 0, len(oldFields))
		newTypeCodes := make([]int8, 0, len(oldTypeCodes))

		for i := 0; i < len(oldFields); i++ {
			newField := NewFieldFrom(&oldFields[i], transformNode.Children[i])
			if newField != nil {
				newFields = append(newFields, *newField)
				newTypeCodes = append(newTypeCodes, oldTypeCodes[i])
			}
		}

		switch dt.(type) {
		case *arrow.SparseUnionType:
			return &arrow.Field{Name: field.Name, Type: arrow.SparseUnionOf(newFields, newTypeCodes), Nullable: field.Nullable, Metadata: metadata}
		case *arrow.DenseUnionType:
			return &arrow.Field{Name: field.Name, Type: arrow.DenseUnionOf(newFields, newTypeCodes), Nullable: field.Nullable, Metadata: metadata}
		default:
			panic("unknown union type")
		}
	case *arrow.MapType:
		keyField := dt.KeyField()
		newKeyField := NewFieldFrom(&keyField, transformNode.Children[0])
		valueField := dt.ItemField()
		newValueField := NewFieldFrom(&valueField, transformNode.Children[1])

		if newKeyField == nil || newValueField == nil {
			return nil
		}
		return &arrow.Field{Name: field.Name, Type: arrow.MapOf(newKeyField.Type, newValueField.Type), Nullable: field.Nullable, Metadata: metadata}
	default:
		return &arrow.Field{Name: field.Name, Type: field.Type, Nullable: field.Nullable, Metadata: metadata}
	}
}

func cleanMetadata(metadata arrow.Metadata) arrow.Metadata {
	keys := make([]string, 0, len(metadata.Keys()))
	values := make([]string, 0, len(metadata.Values()))

	for i, key := range metadata.Keys() {
		if key == OptionalKey || key == DictionaryKey {
			continue
		}
		keys = append(keys, key)
		values = append(values, metadata.Values()[i])
	}

	return arrow.NewMetadata(keys, values)
}
