package app

import (
	_ "embed"
)

var (
	//go:embed template/pgsql/func_create.sql
	pgsqlFuncCreate string

	//go:embed template/pgsql/func_drop.sql
	pgsqlFuncDrop string
)

//go:embed template/template_data
var templateData []byte

var (
	//go:embed template/default_schema
	defaultSchemaTemplate []byte

	//go:embed template/default_table
	defaultTableTemplate []byte

	//go:embed template/default_replace
	defaultReplaceTemplate []byte
)

//go:embed example.yaml
var ExampleConfig []byte
