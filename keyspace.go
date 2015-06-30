package gocassa

import (
	"fmt"
	"github.com/hailocab/gocassa/reflect"
	"strings"
	"time"
)

type tableFactory interface {
	NewTable(string, interface{}, Keys) (Table, error)
}

type k struct {
	qe           QueryExecutor
	name         string
	debugMode    bool
	tableFactory tableFactory
}

// Connect to a certain keyspace directly. Same as using Connect().KeySpace(keySpaceName)
func ConnectToKeySpace(keySpace string, nodeIps []string, username, password string) (KeySpace, error) {
	c, err := Connect(nodeIps, username, password)
	if err != nil {
		return nil, err
	}
	return c.KeySpace(keySpace), nil
}

func (k *k) DebugMode(b bool) {
	k.debugMode = b
}

func (k *k) Table(name string, entity interface{}, keys Keys) (Table, error) {
	return k.NewTable(name, entity, keys)
}

func (k *k) NewTable(name string, entity interface{}, keys Keys) (Table, error) {
	// Act both as a proxy to a tableFactory, and as the tableFactory itself (in most situations, a k will be its own
	// tableFactory, but not always [ie. mocking])

	info, err := reflect.NewStructInfo(entity)

	if err != nil {
		return nil, err
	}

	if k.tableFactory != k {
		return k.tableFactory.NewTable(name, entity, keys)
	}

	ti := newTableInfo(k.name, name, keys, entity, info)
	return &t{
		keySpace: k,
		info:     ti,
		options:  Options{},
	}, nil
}

func (k *k) MapTable(name, id string, row interface{}) (MapTable, error) {
	table, err := k.NewTable(name, row, Keys{
		PartitionKeys: []string{id},
	})

	if err != nil {
		return nil, err
	}

	return &mapT{
		Table:   table,
		idField: id,
	}, nil
}

func (k *k) SetKeysSpaceName(name string) {
	k.name = name
}

func (k *k) MultimapTable(name, fieldToIndexBy, id string, row interface{}) (MultimapTable, error) {
	table, err := k.NewTable(name, row, Keys{
		PartitionKeys:     []string{fieldToIndexBy},
		ClusteringColumns: []string{id},
	})

	if err != nil {
		return nil, err
	}

	return &multimapT{
		Table:          table,
		idField:        id,
		fieldToIndexBy: fieldToIndexBy,
	}, nil
}

func (k *k) TimeSeriesTable(name, timeField, idField string, bucketSize time.Duration, row interface{}) (TimeSeriesTable, error) {
	table, err := k.NewTable(name, row, Keys{
		PartitionKeys:     []string{bucketFieldName},
		ClusteringColumns: []string{timeField, idField},
	})

	if err != nil {
		return nil, err
	}

	return &timeSeriesT{
		Table:      table,
		timeField:  timeField,
		idField:    idField,
		bucketSize: bucketSize,
	}, nil
}

func (k *k) MultiTimeSeriesTable(name, indexField, timeField, idField string, bucketSize time.Duration, row interface{}) (MultiTimeSeriesTable, error) {
	table, err := k.NewTable(name, row, Keys{
		PartitionKeys:     []string{indexField, bucketFieldName},
		ClusteringColumns: []string{timeField, idField},
	})

	if err != nil {
		return nil, err
	}

	return &multiTimeSeriesT{
		Table:      table,
		indexField: indexField,
		timeField:  timeField,
		idField:    idField,
		bucketSize: bucketSize,
	}, nil
}

// Returns table names in a keyspace
func (k *k) Tables() ([]string, error) {
	const stmt = "SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name = ?"
	maps, err := k.qe.Query(stmt, k.name)
	if err != nil {
		return nil, err
	}
	ret := []string{}
	for _, m := range maps {
		ret = append(ret, m["columnfamily_name"].(string))
	}
	return ret, nil
}

func (k *k) Exists(cf string) (bool, error) {
	ts, err := k.Tables()
	if err != nil {
		return false, err
	}
	for _, v := range ts {
		if strings.ToLower(v) == strings.ToLower(cf) {
			return true, nil
		}
	}
	return false, nil
}

func (k *k) DropTable(cf string) error {
	stmt := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", k.name, cf)
	return k.qe.Execute(stmt)
}

func (k *k) Name() string {
	return k.name
}
