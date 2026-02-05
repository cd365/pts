package app

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"text/template"
	"time"
	"unsafe"

	"gopkg.in/yaml.v3"

	"github.com/cd365/hey/v7/cst"

	"github.com/cd365/hey/v7"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

const (
	CmdConfig  = "config"
	CmdCustom  = "custom"
	CmdReplace = "replace"
	CmdSchema  = "schema"
	CmdTable   = "table"
)

type Config struct {
	// Database driver name, database connection, database schema name, database table prefix
	Database struct {
		Driver             string `yaml:"driver"`               // postgres
		Username           string `yaml:"username"`             // postgres
		Password           string `yaml:"password"`             // postgres
		Host               string `yaml:"host"`                 // localhost
		Port               uint16 `yaml:"port"`                 // 5432
		Database           string `yaml:"database"`             // postgres
		DataSourceName     string `yaml:"data_source_name"`     // $HOME/example.db
		DatabaseSchemaName string `yaml:"database_schema_name"` // public
		TablePrefix        string `yaml:"table_prefix"`         // table prefix
	}

	// Use a set of regular expressions or specific table names to filter out table structures that do not need to be exported
	DisableTable       []string             `yaml:"disable_table"`
	DisableTableMap    map[string]*struct{} `yaml:"-"`
	DisableTableRegexp []*regexp.Regexp     `yaml:"-"`

	// Configuration comment: when a configuration comment exists and the corresponding (table or column) comment is empty, use the configuration comment to fill it
	Comments map[string]struct {
		Comment string            `yaml:"comment"`
		Columns map[string]string `yaml:"columns"`
	} `yaml:"comments"`

	// Custom template file, default template file will be used if not set
	TemplateFileCustom  string `yaml:"template_file_custom"`
	TemplateFileReplace string `yaml:"template_file_replace"`
	TemplateFileSchema  string `yaml:"template_file_schema"`
	TemplateFileTable   string `yaml:"template_file_table"`

	// Only export the following tables.
	OnlyTable []string `yaml:"only_table"`
}

// exampleConfig Config example
func exampleConfig() ([]byte, error) {
	c := &Config{}
	c.Database.Driver = "postgres"
	c.Database.Username = "postgres"
	c.Database.Password = "postgres"
	c.Database.Host = "localhost"
	c.Database.Port = 5432
	c.Database.Database = "db_name"
	c.Database.DatabaseSchemaName = "public"
	c.Database.TablePrefix = "pre_"
	c.DisableTable = []string{
		"^disable_.*$",
		"^example_.*$",
		"system_table_name",
	}
	c.Comments = map[string]struct {
		Comment string            `yaml:"comment"`
		Columns map[string]string `yaml:"columns"`
	}{
		"example_user": {
			Comment: "example user",
			Columns: map[string]string{
				"id":         "ID primary key",
				"name":       "Name",
				"email":      "Email",
				"age":        "Age",
				"created_at": "created timestamp",
				"updated_at": "updated timestamp",
				"deleted_at": "deleted timestamp",
			},
		},
		"example_test": {
			Comment: "example test table comment",
			Columns: map[string]string{
				"id": "ID primary key",
			},
		},
	}
	c.TemplateFileCustom = "replace this with a custom template path"
	c.TemplateFileReplace = "replace this with a custom-replace template path"
	c.TemplateFileSchema = "replace this with a custom-schema template path"
	c.TemplateFileTable = "replace this with a custom-table template path"
	out, err := yaml.Marshal(c)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ParseConfig Parse configuration file
func ParseConfig(configFile string) (*Config, error) {
	stat, err := os.Stat(configFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("config file %s does not exist", configFile)
		}
		return nil, err
	}
	if stat.IsDir() {
		return nil, fmt.Errorf("config file is a directory")
	}
	fil, err := os.OpenFile(configFile, os.O_RDONLY, 0o644)
	if err != nil {
		return nil, err
	}
	defer func() { _ = fil.Close() }()
	config := &Config{}
	if err = yaml.NewDecoder(fil).Decode(config); err != nil {
		return nil, err
	}
	return config, nil
}

// initConfigDisableTable Configuration Initialization
func initConfigDisableTable(cfg *Config) {
	for _, v := range cfg.DisableTable {
		v = strings.TrimSpace(v)
		if strings.HasPrefix(v, "^") && strings.HasSuffix(v, "$") {
			cfg.DisableTableRegexp = append(cfg.DisableTableRegexp, regexp.MustCompile(v))
			continue
		}
		if cfg.DisableTableMap == nil {
			cfg.DisableTableMap = make(map[string]*struct{})
		}
		cfg.DisableTableMap[v] = nil
	}
}

// isTableDisabled Determine whether a table is prohibited from being exported
func isTableDisabled(cfg *Config, table string) bool {
	if cfg.DisableTableMap != nil {
		_, ok := cfg.DisableTableMap[table]
		return ok
	}
	for _, disable := range cfg.DisableTableRegexp {
		if disable.MatchString(table) {
			return true
		}
	}
	return false
}

func NewWay(cfg *Config) (*hey.Way, error) {
	driver := cfg.Database.Driver
	dataSourceName := strings.TrimSpace(cfg.Database.DataSourceName)
	if dataSourceName == "" {
		db := cfg.Database
		switch driver {
		case "mysql":
			dataSourceName = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", db.Username, db.Password, db.Host, db.Port, db.Database)
		case "postgres":
			dataSourceName = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", db.Username, db.Password, db.Host, db.Port, db.Database)
		case "sqlite", "sqlite3":
			panic("SQLite must have the data_source_name value configured")
		default:
			panic(fmt.Errorf("unsupported database driver: %s", driver))
		}
	}
	db, err := sql.Open(driver, dataSourceName)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(time.Minute * 3)
	db.SetConnMaxLifetime(time.Minute * 3)
	opts := make([]hey.Option, 0)
	configDefault := hey.ConfigDefault()
	switch driver {
	case string(cst.Postgresql), "postgres":
		configDefault = hey.ConfigDefaultPostgresql()
	case string(cst.Mysql):
		configDefault = hey.ConfigDefaultMysql()
	case string(cst.Sqlite), "sqlite3":
		configDefault = hey.ConfigDefaultSqlite()
	}
	opts = append(opts, hey.WithConfig(configDefault))
	opts = append(opts, hey.WithDatabase(db))
	way := hey.NewWay(opts...)
	switch driver {
	case string(cst.Mysql):
		if cfg.Database.Database == "" {
			start := strings.Index(dataSourceName, "/")
			if start > -1 {
				end := strings.Index(dataSourceName, "?")
				if end > -1 {
					cfg.Database.Database = dataSourceName[start+1 : end]
				} else {
					cfg.Database.Database = dataSourceName[start+1:]
				}
			}
		}
	case string(cst.Postgresql), "postgres":
		if cfg.Database.DatabaseSchemaName == "" {
			cfg.Database.DatabaseSchemaName = "public"
		}
	case string(cst.Sqlite), "sqlite3":
	default:
		panic(fmt.Errorf("unsupported driver name: %s", driver))
	}
	return way, nil
}

func NewSchema(way *hey.Way) Schema {
	databaseType := way.Config().Manual.DatabaseType
	switch databaseType {
	case cst.Mysql:
		return NewSchemaMysql(way)
	case cst.Postgresql, "postgres":
		return NewSchemaPostgresql(way)
	case cst.Sqlite, "sqlite3":
		return NewSchemaSqlite(way)
	default:
		panic(fmt.Errorf("unsupported database type: %s", databaseType))
	}
}

func NewTemplate(name string, content []byte, funcMap map[string]any) *template.Template {
	return template.Must(template.New(name).Delims("{{", "}}").Funcs(funcMap).Parse(*(*string)(unsafe.Pointer(&content))))
}

type App struct {
	cfg    *Config
	way    *hey.Way
	schema Schema
}

func NewApp(config string) (app *App, err error) {
	cfg, err := ParseConfig(config)
	if err != nil {
		return
	}
	initConfigDisableTable(cfg)
	way, err := NewWay(cfg)
	if err != nil {
		return
	}
	schema := NewSchema(way)
	app = &App{
		cfg:    cfg,
		way:    way,
		schema: schema,
	}
	return
}

func (s *App) Cfg() *Config {
	return s.cfg
}

func (s *App) Run(ctx context.Context, output func(ctx context.Context, tmp *Template) (content []byte, err error)) (content []byte, err error) {
	if output == nil {
		return
	}

	if s.way.Config().Manual.DatabaseType == cst.Postgresql {
		if _, err = s.way.Database().Exec(pgsqlFuncCreate); err != nil {
			return
		}
		defer func() { _, _ = s.way.Database().Exec(pgsqlFuncDrop) }()
	}

	var tables []*Table
	tables, err = GetAllTables(ctx, s.cfg, s.schema, s.way)
	if err != nil {
		return
	}

	tmp := &Template{
		Tables: tables,
	}

	// Remove duplicate column names
	allColumns := make(map[string]*struct{})
	for _, table := range tables {
		// replace empty comment
		{
			va, ok := s.cfg.Comments[table.Table]
			if ok {
				if va.Comment != "" {
					if table.Comment == "" || table.Comment == table.Table {
						table.Comment = va.Comment
					}
				}
				if len(va.Columns) > 0 {
					for _, column := range table.Columns {
						vb, ol := va.Columns[column.Column]
						if ol && vb != "" {
							if column.Comment == "" || column.Comment == column.Column {
								column.Comment = vb
							}
						}
					}
				}
			}
		}
		// all table columns
		for _, column := range table.Columns {
			_, ok := allColumns[column.Column]
			if ok {
				continue
			}
			allColumns[column.Column] = nil
			tmp.AllTableColumns = append(tmp.AllTableColumns, column.Column)
		}
	}

	content, err = output(ctx, tmp)
	if err != nil {
		return
	}

	return
}

func (s *App) newTemplate(name string, content []byte) *template.Template {
	funcMap := template.FuncMap{
		// Addition
		"add": func(x, y int) int {
			return x + y
		},
		// Used to check if a string is not empty
		"isNotEmpty": func(s string) bool {
			return strings.TrimSpace(s) != ""
		},
		// user => "user" | `user`
		// prefix.user => "prefix"."user" | `prefix`.`user`
		"mark": func(c string, s string) string {
			c = strings.TrimSpace(c)
			if c == `"` {
				c = `\"`
			}
			sss := strings.Split(s, ".")
			return fmt.Sprintf("%s%s%s", c, strings.Join(sss, fmt.Sprintf("%s.%s", c, c)), c)
		},
	}
	return NewTemplate(name, content, funcMap)
}

func getContent(contentFile string, contentDefault []byte) (content []byte, err error) {
	if contentFile != "" {
		content, err = os.ReadFile(contentFile)
		if err != nil {
			return nil, err
		}
		return content, nil
	}
	return contentDefault, nil
}

func (s *App) NewOutput(cmd string) func(ctx context.Context, tmp *Template) (content []byte, err error) {
	return func(ctx context.Context, tmp *Template) (content []byte, err error) {
		switch cmd {
		case CmdCustom:
			content, err = getContent(s.cfg.TemplateFileCustom, make([]byte, 0))
			if err != nil {
				return
			}
		case CmdReplace:
			content, err = getContent(s.cfg.TemplateFileReplace, defaultReplaceTemplate)
			if err != nil {
				return
			}
		case CmdSchema:
			content, err = getContent(s.cfg.TemplateFileSchema, defaultSchemaTemplate)
			if err != nil {
				return
			}
		case CmdTable:
			content, err = getContent(s.cfg.TemplateFileTable, defaultTableTemplate)
			if err != nil {
				return
			}
		default:
			err = fmt.Errorf("invalid command: %s", cmd)
			return
		}
		tt := s.newTemplate(CmdTable, content)
		buf := bytes.NewBuffer(nil)
		err = tt.Execute(buf, tmp)
		if err != nil {
			return
		}
		content = buf.Bytes()
		return
	}
}

type Template struct {
	Tables          []*Table // All exported tables
	AllTableColumns []string // A list of all columns from all tables, with duplicates removed based on column names
}

type Table struct {
	Database string    `db:"table_schema"`  // database name
	Table    string    `db:"table_name"`    // table name (original table name)
	Comment  string    `db:"table_comment"` // table comment
	Columns  []*Column `db:"-"`             // table columns
	Defined  string    `db:"-"`             // table DDL

	AutoIncrementColumn string `db:"-"` // auto-increment column

	TableGoTypeName          string `db:"-"` // table go type name struct
	TableGoTypeNameTimestamp string `db:"-"` // table go type name struct + timestamp
}

type Column struct {
	table                  *Table  `db:"-"`
	Database               string  `db:"table_schema"`             // database name
	Table                  string  `db:"table_name"`               // table name
	Column                 string  `db:"column_name"`              // column name
	Comment                string  `db:"column_comment"`           // column comment
	Type                   *string `db:"column_type"`              // column type
	DataType               *string `db:"data_type"`                // column data type
	ColumnDefault          *string `db:"column_default"`           // column default value
	IsNullable             *string `db:"is_nullable"`              // whether to allow the column value to be null
	OrdinalPosition        *int    `db:"ordinal_position"`         // column serial number
	CharacterMaximumLength *int    `db:"character_maximum_length"` // maximum string length
	CharacterOctetLength   *int    `db:"character_octet_length"`   // maximum byte length of text string
	NumericPrecision       *int    `db:"numeric_precision"`        // maximum length of integer | total length of decimal (integer + decimal)
	NumericScale           *int    `db:"numeric_scale"`            // decimal precision length
	CharacterSetName       *string `db:"character_set_name"`       // character set name
	CollationName          *string `db:"collation_name"`           // collation name
	ColumnKey              *string `db:"column_key"`               // column index '', 'PRI', 'UNI', 'MUL'
	Extra                  *string `db:"extra"`                    // column extra auto_increment

	ColumnCamel     string `db:"-"` // column name camel case
	ColumnPascal    string `db:"-"` // column name pascal case
	ColumnUnderline string `db:"-"` // column name underline case
	GoType          string `db:"-"` // string, int64, int, *string ...
}

func (s *Column) goType() (result string) {
	nullable := true
	if s.IsNullable != nil && strings.ToLower(*s.IsNullable) == "no" {
		nullable = false
	}
	datatype := ""
	if s.DataType != nil {
		datatype = strings.ToLower(*s.DataType)
	}
	{
		// Consider SQLite
		if datatype == "" && s.Type != nil && *s.Type != "" {
			datatype = strings.ToLower(*s.Type)
		}
	}
	switch datatype {
	case "tinyint":
		result = "int8"
	case "smallint", "smallserial":
		result = "int16"
	case "integer", "serial", "int":
		result = "int"
	case "bigint", "bigserial":
		result = "int64"
	case "decimal", "numeric", "real", "double precision", "double", "float":
		result = "float64"
	case "char", "character", "character varying", "text", "varchar", "enum", "mediumtext", "longtext":
		result = "string"
	case "bool", "boolean":
		result = "bool"
	case "binary", "varbinary", "tinyblob", "mediumblob", "longblob", // mysql
		"blob",  // mysql && sqlite
		"bytea": // postgresql
		result = "[]byte"
	default:
		result = "string"
	}
	if nullable {
		if result != "[]byte" {
			result = "*" + result
		}
	}
	return result
}

func (s *Column) init(way *hey.Way) {
	if s.ColumnCamel != "" {
		return
	}
	if s.ColumnCamel == "" {
		s.ColumnCamel = Camel(s.Column)
	}
	if s.ColumnPascal == "" {
		s.ColumnPascal = Pascal(s.Column)
	}
	if s.ColumnUnderline == "" {
		s.ColumnUnderline = Underline(s.Column)
	}
	s.GoType = s.goType()
}

// Schema Parse the structure of tables and columns in the database
type Schema interface {
	// QueryTableDefineSql Get the DDL of a specific table in a database
	QueryTableDefineSql(ctx context.Context, cfg *Config, table *Table) (string, error)

	// QueryTables Get all tables in a database
	QueryTables(ctx context.Context, cfg *Config, schema string) ([]*Table, error)

	// QueryColumns Get all columns of a specific table in a database
	QueryColumns(ctx context.Context, cfg *Config, schema string, table string) ([]*Column, error)

	// QuerySchemas Call QueryColumns and QueryTableDefineSql.
	QuerySchemas(ctx context.Context, cfg *Config, tables []*Table) error
}

// autoIncrementRegexpReplace Auto-increment column.
var autoIncrementRegexpReplace = regexp.MustCompile(`(AUTO_INCREMENT|auto_increment)=\d+`)

/* MySQL */

type SchemaMysql struct {
	way *hey.Way
}

func (s *SchemaMysql) QueryTableDefineSql(ctx context.Context, cfg *Config, table *Table) (string, error) {
	for _, c := range table.Columns {
		if c.Extra != nil && strings.ToLower(*c.Extra) == "auto_increment" {
			table.AutoIncrementColumn = c.Column
		}
	}
	prepare := fmt.Sprintf("SHOW CREATE TABLE %s.%s", table.Database, table.Table)
	name, result := "", ""
	err := s.way.Query(ctx, hey.NewSQL(prepare), func(rows *sql.Rows) error {
		for rows.Next() {
			if err := rows.Scan(&name, &result); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	defined := strings.ReplaceAll(result, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
	defined = autoIncrementRegexpReplace.ReplaceAllString(defined, "${1}=1")
	table.Defined = defined
	return defined, nil
}

func (s *SchemaMysql) QueryTables(ctx context.Context, cfg *Config, schema string) ([]*Table, error) {
	tables := make([]*Table, 0)
	// "SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name, TABLE_COMMENT AS table_comment FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = ? ORDER BY TABLE_NAME ASC;"
	query := s.way.Table("information_schema.TABLES")
	query.Select("TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name, TABLE_COMMENT AS table_comment")
	query.WhereFunc(func(where hey.Filter) {
		where.Equal("TABLE_SCHEMA", schema)
		where.Equal("TABLE_TYPE", "BASE TABLE")
		if len(cfg.OnlyTable) > 0 {
			where.In("TABLE_NAME", cfg.OnlyTable)
		}
	})
	query.Asc("TABLE_NAME")
	if err := query.Scan(ctx, &tables); err != nil {
		return nil, err
	}
	return tables, nil
}

func (s *SchemaMysql) QueryColumns(ctx context.Context, cfg *Config, schema string, table string) ([]*Column, error) {
	columns := make([]*Column, 0)
	if schema == "" || table == "" {
		return columns, nil
	}
	prepare := "SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name, COLUMN_NAME AS column_name, ORDINAL_POSITION AS ordinal_position, COLUMN_DEFAULT AS column_default, IS_NULLABLE AS is_nullable, DATA_TYPE AS data_type, CHARACTER_MAXIMUM_LENGTH AS character_maximum_length, CHARACTER_OCTET_LENGTH AS character_octet_length, NUMERIC_PRECISION AS numeric_precision, NUMERIC_SCALE AS numeric_scale, CHARACTER_SET_NAME AS character_set_name, COLLATION_NAME AS collation_name, COALESCE(COLUMN_COMMENT,'') AS column_comment, COLUMN_TYPE AS column_type, COLUMN_KEY AS column_key, EXTRA AS extra FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ordinal_position ASC"
	err := s.way.Scan(ctx, hey.NewSQL(prepare, schema, table), &columns)
	if err != nil {
		return nil, err
	}
	return columns, nil
}

func (s *SchemaMysql) QuerySchemas(ctx context.Context, cfg *Config, tables []*Table) error {
	var errorQuery error
	once := &sync.Once{}
	waitGroup := &sync.WaitGroup{}
	for _, table := range tables {
		waitGroup.Add(1)
		go func(table *Table) {
			defer waitGroup.Done()
			columns, err := s.QueryColumns(ctx, cfg, table.Database, table.Table)
			if err != nil {
				once.Do(func() { errorQuery = err })
				return
			}
			table.Columns = columns
			defined, err := s.QueryTableDefineSql(ctx, cfg, table)
			if err != nil {
				once.Do(func() { errorQuery = err })
				return
			}
			table.Defined = defined
		}(table)
	}
	waitGroup.Wait()
	if errorQuery != nil {
		return errorQuery
	}
	return nil
}

func NewSchemaMysql(way *hey.Way) *SchemaMysql {
	schema := &SchemaMysql{}
	schema.way = way
	return schema
}

/* PostgreSQL */

// pgsqlSeq Postgresql Sequence.
var pgsqlSeq = regexp.MustCompile(`^nextval\('([A-Za-z0-9_]+)'::regclass\)$`)

type SchemaPostgresql struct {
	way *hey.Way
}

func (s *SchemaPostgresql) QueryTableDefineSql(ctx context.Context, cfg *Config, table *Table) (string, error) {
	var createSequence string
	for _, c := range table.Columns {
		if c.ColumnDefault == nil {
			continue
		}
		if strings.Contains(*c.ColumnDefault, "\"") {
			*c.ColumnDefault = strings.ReplaceAll(*c.ColumnDefault, "\"", "")
		}
		if pgsqlSeq.MatchString(*c.ColumnDefault) {
			result := pgsqlSeq.FindAllStringSubmatch(*c.ColumnDefault, -1)
			if len(result) == 1 && len(result[0]) == 2 && result[0][1] != "" {
				createSequence = fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s START 1;\n", result[0][1])
				table.AutoIncrementColumn = c.Column
			}
		}
	}
	prepare := fmt.Sprintf("SELECT show_create_table_schema('%s', '%s')", table.Database, table.Table)
	result := ""
	err := s.way.Query(ctx, hey.NewSQL(prepare), func(rows *sql.Rows) error {
		for rows.Next() {
			if err := rows.Scan(&result); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	result = strings.ReplaceAll(result, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
	result = strings.ReplaceAll(result, "CREATE INDEX", "CREATE INDEX IF NOT EXISTS")
	result = strings.ReplaceAll(result, "CREATE UNIQUE INDEX", "CREATE UNIQUE INDEX IF NOT EXISTS")
	result = createSequence + result
	table.Defined = result
	return result, nil
}

func (s *SchemaPostgresql) queryTableComment(ctx context.Context, cfg *Config, table *Table) (string, error) {
	prepare := "SELECT cast(obj_description(relfilenode, 'pg_class') AS VARCHAR) AS table_comment FROM pg_tables LEFT OUTER JOIN pg_class ON pg_tables.tablename = pg_class.relname WHERE ( pg_tables.schemaname = ? AND pg_tables.tablename = ? ) ORDER BY pg_tables.schemaname ASC LIMIT 1;"
	if err := s.way.Query(ctx, hey.NewSQL(prepare, table.Database, table.Table), func(rows *sql.Rows) error {
		if !rows.Next() {
			return nil
		}
		comment := sql.NullString{}
		if err := rows.Scan(&comment); err != nil {
			return err
		}
		if comment.Valid {
			table.Comment = comment.String
		}
		return nil
	}); err != nil {
		return "", err
	}
	return table.Comment, nil
}

func (s *SchemaPostgresql) QueryTables(ctx context.Context, cfg *Config, schema string) ([]*Table, error) {
	tables := make([]*Table, 0)
	// SELECT table_schema, table_name FROM information_schema.tables WHERE ( table_schema = ? AND table_type = 'BASE TABLE' ) ORDER BY table_name ASC
	query := s.way.Table("information_schema.tables")
	query.Select("table_schema, table_name")
	query.WhereFunc(func(where hey.Filter) {
		where.Equal("table_schema", schema)
		where.Equal("table_type", "BASE TABLE")
		if len(cfg.OnlyTable) > 0 {
			where.In("table_name", cfg.OnlyTable)
		}
	})
	query.Asc("table_name")
	if err := query.Scan(ctx, &tables); err != nil {
		return nil, err
	}
	return tables, nil
}

func (s *SchemaPostgresql) QueryColumns(ctx context.Context, cfg *Config, schema string, table string) ([]*Column, error) {
	columns := make([]*Column, 0)
	if schema == "" || table == "" {
		return columns, nil
	}
	prepare := "SELECT table_schema, table_name, column_name, ordinal_position, column_default, is_nullable, data_type, character_maximum_length, character_octet_length, numeric_precision, numeric_scale, character_set_name, collation_name FROM information_schema.columns WHERE ( table_schema = ? AND table_name = ? ) ORDER BY ordinal_position ASC"
	err := s.way.Query(ctx, hey.NewSQL(prepare, schema, table), func(rows *sql.Rows) (err error) {
		for rows.Next() {
			tmp := &Column{}
			if err = rows.Scan(
				&tmp.Database,
				&tmp.Table,
				&tmp.Column,
				&tmp.OrdinalPosition,
				&tmp.ColumnDefault,
				&tmp.IsNullable,
				&tmp.DataType,
				&tmp.CharacterMaximumLength,
				&tmp.CharacterOctetLength,
				&tmp.NumericPrecision,
				&tmp.NumericScale,
				&tmp.CharacterSetName,
				&tmp.CollationName,
			); err != nil {
				return err
			}
			columns = append(columns, tmp)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	for k, v := range columns {
		if v.Column == "" {
			continue
		}
		// query column comment
		// SELECT a.attnum AS id, a.attname AS column_name, t.typname AS type_basic, SUBSTRING(FORMAT_TYPE(a.atttypid, a.atttypmod) FROM '(.*)') AS type_sql, a.attnotnull AS not_null, d.description AS comment FROM pg_class c, pg_attribute a, pg_type t, pg_description d WHERE ( c.relname = 'TABLE_NAME' AND a.attnum > 0 AND a.attrelid = c.oid AND a.atttypid = t.oid AND d.objoid = a.attrelid AND d.objsubid = a.attnum ) ORDER BY id ASC;
		err = s.way.Query(ctx, hey.NewSQL("SELECT COALESCE(d.description,'') AS column_comment FROM pg_class c, pg_attribute a, pg_type t, pg_description d WHERE ( c.relname = ? AND a.attname = ? AND a.attnum > 0 AND a.attrelid = c.oid AND a.atttypid = t.oid AND d.objoid = a.attrelid AND d.objsubid = a.attnum ) ORDER BY a.attnum ASC LIMIT 1;", table, v.Column), func(rows *sql.Rows) (err error) {
			if !rows.Next() {
				return err
			}
			tmp := ""
			if err = rows.Scan(&tmp); err != nil {
				return err
			}
			columns[k].Comment = tmp
			return err
		})
		if err != nil {
			return nil, err
		}
	}
	return columns, nil
}

func (s *SchemaPostgresql) QuerySchemas(ctx context.Context, cfg *Config, tables []*Table) error {
	var errorQuery error
	once := &sync.Once{}
	wg := &sync.WaitGroup{}
	for _, table := range tables {
		wg.Add(1)
		go func(table *Table) {
			defer wg.Done()
			columns, err := s.QueryColumns(ctx, cfg, table.Database, table.Table)
			if err != nil {
				once.Do(func() { errorQuery = err })
				return
			}
			table.Columns = columns
			if table.Comment, err = s.queryTableComment(ctx, cfg, table); err != nil {
				once.Do(func() { errorQuery = err })
			}
			_, err = s.QueryTableDefineSql(ctx, cfg, table)
			if err != nil {
				once.Do(func() { errorQuery = err })
			}
		}(table)
	}
	wg.Wait()
	if errorQuery != nil {
		return errorQuery
	}
	return nil
}

func NewSchemaPostgresql(way *hey.Way) *SchemaPostgresql {
	schema := &SchemaPostgresql{}
	schema.way = way
	return schema
}

type SchemaSqlite struct {
	way *hey.Way
}

func (s *SchemaSqlite) QueryTableDefineSql(ctx context.Context, cfg *Config, table *Table) (string, error) {
	return table.Defined, nil
}

func (s *SchemaSqlite) QueryTables(ctx context.Context, cfg *Config, schema string) ([]*Table, error) {
	tables := make([]*Table, 0)
	// SELECT name AS table_name, sql AS table_defined FROM sqlite_master WHERE ( type = 'table' AND name <> 'sqlite_sequence' );
	query := s.way.Table("sqlite_master")
	query.Select("name AS table_name, sql AS table_defined")
	query.WhereFunc(func(where hey.Filter) {
		where.Equal("type", "table")
		where.NotEqual("name", "sqlite_sequence")
		if len(cfg.OnlyTable) > 0 {
			where.In("name", cfg.OnlyTable)
		}
	})
	query.Asc("table_name")
	if err := s.way.Query(ctx, query.ToSelect(), func(rows *sql.Rows) error {
		for rows.Next() {
			table := ""
			defined := ""
			if err := rows.Scan(&table, &defined); err != nil {
				return err
			}
			tables = append(tables, &Table{
				Table:   table,
				Defined: defined,
			})
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return tables, nil
}

func (s *SchemaSqlite) QueryColumns(ctx context.Context, cfg *Config, schema string, table string) ([]*Column, error) {
	columns := make([]*Column, 0)
	if table == "" {
		return columns, nil
	}
	prepare := fmt.Sprintf("PRAGMA table_info(%s);", table)
	err := s.way.Query(ctx, hey.NewSQL(prepare), func(rows *sql.Rows) error {
		for rows.Next() {
			cid := 0         // cid
			name := ""       // name
			columnType := "" // type
			notNull := 0     // notnull
			defaultValue := sql.NullString{}
			pk := 0
			err := rows.Scan(
				&cid,
				&name,
				&columnType,
				&notNull,
				&defaultValue,
				&pk,
			)
			if err != nil {
				return err
			}
			tmp := &Column{
				Table:           table,
				Column:          name,
				OrdinalPosition: &cid,
				Type:            &columnType,
			}
			isNullable := ""
			if notNull > 0 {
				isNullable = "no"
			} else {
				isNullable = "yes"
			}
			tmp.IsNullable = &isNullable
			if defaultValue.Valid {
				tmp.ColumnDefault = &defaultValue.String
			}
			if pk > 0 {
				autoIncrement := "auto_increment"
				tmp.Extra = &autoIncrement
			}
			columns = append(columns, tmp)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return columns, nil
}

func (s *SchemaSqlite) QuerySchemas(ctx context.Context, cfg *Config, tables []*Table) error {
	for _, table := range tables {
		columns, err := s.QueryColumns(ctx, cfg, table.Database, table.Table)
		if err != nil {
			return err
		}
		for _, column := range columns {
			if table.AutoIncrementColumn == "" && column.Extra != nil && *column.Extra == "auto_increment" {
				table.AutoIncrementColumn = column.Column
			}
		}
		table.Columns = columns
	}
	return nil
}

func NewSchemaSqlite(way *hey.Way) *SchemaSqlite {
	schema := &SchemaSqlite{}
	schema.way = way
	return schema
}

// GetAllTables Get all tables and their columns that meet the criteria
func GetAllTables(ctx context.Context, config *Config, schema Schema, way *hey.Way) ([]*Table, error) {
	databaseName := config.Database.Database
	switch way.Config().Manual.DatabaseType {
	case cst.Postgresql:
		databaseName = config.Database.DatabaseSchemaName
	case cst.Sqlite:
		databaseName = ""
	}
	lists, err := schema.QueryTables(ctx, config, databaseName)
	if err != nil {
		return nil, err
	}

	onlyTableMap := make(map[string]*struct{})
	for _, t := range config.OnlyTable {
		t = strings.TrimSpace(t)
		if t != "" {
			onlyTableMap[t] = nil
		}
	}
	onlyTable := len(onlyTableMap) > 0
	tables := make([]*Table, 0, len(lists))
	for _, t := range lists {
		if onlyTable {
			if _, ok := onlyTableMap[t.Table]; ok {
				tables = append(tables, t)
			}
			continue
		}
		if isTableDisabled(config, t.Table) {
			continue
		}
		tables = append(tables, t)
	}
	err = schema.QuerySchemas(ctx, config, tables)
	if err != nil {
		return nil, err
	}

	timestamp := time.Now().Unix()
	for _, t := range tables {
		if t.Comment == "" {
			t.Comment = t.Table
		}
		// Handle naming
		{
			if t.TableGoTypeName == "" {
				name := t.Table
				if config.Database.TablePrefix != "" {
					name = strings.TrimPrefix(name, config.Database.TablePrefix)
				}
				t.TableGoTypeName = Pascal(name)
				t.TableGoTypeNameTimestamp = fmt.Sprintf("%s%d", t.TableGoTypeName, timestamp)
			}
			for _, c := range t.Columns {
				c.init(way)
			}
		}
	}

	return tables, nil
}
