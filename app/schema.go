package app

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

var (
	Mode        = "PRODUCTION"
	GitCommitId = ""
)

func IsDebug() bool {
	return strings.ToUpper(Mode) == "DEBUG"
}

const (
	CmdConfig = "config"

	CmdCustom  = "custom"
	CmdModel   = "model"
	CmdSchema  = "schema"
	CmdReplace = "replace"

	CmdTable = "table"
)

type SchemaTableColumnMethods struct {
	Select []string `yaml:"select"` // columns for query
}

// TemplateTable Config command table
type TemplateTable struct {
	TemplateFile            string `yaml:"template_file"`
	OutputFileDirectory     string `yaml:"output_file_directory"`
	OutputFileNameSuffix    string `yaml:"output_file_name_suffix"`
	OutputGoFilePackageName string `yaml:"output_go_file_package_name"`
}

type Config struct {
	// GitCommitId Git commit id
	GitCommitId string `yaml:"-"`

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

	// Custom template configuration.
	TemplateFileCustom  string           `yaml:"template_file_custom"`
	TemplateFileModel   string           `yaml:"template_file_model"`
	TemplateFileSchema  string           `yaml:"template_file_schema"`
	TemplateFileReplace string           `yaml:"template_file_replace"`
	TemplateTable       []*TemplateTable `yaml:"template_table"`

	// Only export the following tables
	OnlyTable []string `yaml:"only_table"`

	// Sign template sign
	Sign string `yaml:"sign"`
	// ModelPackage Package name of the generated table struct
	ModelPackage string `yaml:"model_package"`

	/* schema module */

	// OutputSchemaMethods Single table default actions
	OutputSchemaMethods bool `yaml:"output_schema_methods"`
	// SchemaTableColumnMethods Method of table-column
	OutputSchemaTableColumnMethods map[string]*SchemaTableColumnMethods `yaml:"output_schema_table_column_methods"`
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
	c.Sign = "01"
	c.ModelPackage = "model"
	c.TemplateFileCustom = "replace this with a `custom` template path"
	c.TemplateFileReplace = "replace this with a `replace` template path"
	c.TemplateFileSchema = "replace this with a `schema` template path"
	c.TemplateFileModel = "replace this with a `model` template path"
	c.TemplateTable = []*TemplateTable{
		{
			TemplateFile:            "",
			OutputFileDirectory:     "",
			OutputFileNameSuffix:    "_admin",
			OutputGoFilePackageName: "schema",
		},
	}
	c.OutputSchemaMethods = true
	c.OutputSchemaTableColumnMethods = map[string]*SchemaTableColumnMethods{
		"employee": {
			Select: []string{"email", "username"},
		},
	}
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
	cfg.GitCommitId = GitCommitId
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

func DefaultTemplateContent(cmd string) (content []byte) {
	switch cmd {
	case CmdCustom:
		content = defaultCustomTemplate
	case CmdModel:
		content = defaultModelTemplate
	case CmdSchema:
		content = defaultSchemaTemplate
	case CmdReplace:
		content = defaultReplaceTemplate
	case CmdTable:
		content = defaultTableTemplate
	default:
		return
	}
	return content
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

func (s *App) Run(ctx context.Context, cmd string, args []string) (err error) {
	if cmd == "" {
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

	{
		// replace empty comment
		for _, table := range tables {
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
	}

	// 1. Write directly to the file.
	if cmd == CmdTable {
		configs := s.cfg.TemplateTable
		length := len(configs)
		if length == 0 {
			return
		}
		var content []byte
		for index, config := range configs {
			content, err = getTemplateFileContent(config.TemplateFile, defaultTableTemplate)
			if err != nil {
				return
			}
			suffix := config.OutputFileNameSuffix
			if suffix == "" {
				suffix = "_admin"
			}
			goPackageName := config.OutputGoFilePackageName
			if goPackageName == "" {
				goPackageName = "schema"
			}
			for _, table := range tables {
				buf := bytes.NewBuffer(nil)
				templateName := fmt.Sprintf("%s_%d", cmd, index)
				err = s.newTemplateWithFuncMap(templateName, content).Execute(buf, table)
				if err != nil {
					return
				}
				b := bytes.NewBuffer(nil)
				b.WriteString("package ")
				b.WriteString(goPackageName)
				b.WriteString("\n\n")
				b.Write(buf.Bytes())
				filename := filepath.Join(config.OutputFileDirectory, fmt.Sprintf("%s%s.go", table.Table, suffix))
				err = WriteFileIfNotExists(filename, b)
				if err != nil {
					return
				}
			}
		}
		return nil
	}

	// 2. Directly output to the standard output stream.
	commonData := &Template{
		Config: s.cfg,
		Tables: tables,
	}

	{
		// Remove duplicate column names
		allColumns := make(map[string]*struct{})
		for _, table := range tables {
			// all table columns
			for _, column := range table.Columns {
				_, ok := allColumns[column.Column]
				if ok {
					continue
				}
				allColumns[column.Column] = nil
				commonData.AllTableColumns = append(commonData.AllTableColumns, column.Column)
			}
		}
	}

	var contentDefault []byte
	contentFile := ""
	switch cmd {
	case CmdCustom:
		contentDefault = defaultCustomTemplate
		contentFile = s.cfg.TemplateFileCustom
	case CmdModel:
		contentDefault = defaultModelTemplate
		contentFile = s.cfg.TemplateFileModel
	case CmdSchema:
		contentDefault = defaultSchemaTemplate
		contentFile = s.cfg.TemplateFileSchema
	case CmdReplace:
		contentDefault = defaultReplaceTemplate
		contentFile = s.cfg.TemplateFileReplace
	default:
		err = fmt.Errorf("invalid command: %s", cmd)
		return
	}
	var content []byte
	content, err = getTemplateFileContent(contentFile, contentDefault)
	if err != nil {
		return
	}
	buf := bytes.NewBuffer(nil)
	err = s.newTemplateWithFuncMap(cmd, content).Execute(buf, commonData)
	if err != nil {
		return
	}
	_, err = os.Stdout.Write(buf.Bytes())
	if err != nil {
		return
	}
	return
}

func (s *App) newTemplateWithFuncMap(name string, content []byte) *template.Template {
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
		// table index
		"tableIndexMapKey": tableIndexMapKey,
		"tableIndexMapVal": tableIndexMapVal,
	}
	return NewTemplate(name, content, funcMap)
}

func getTemplateFileContent(contentFile string, contentDefault []byte) (content []byte, err error) {
	if contentFile != "" {
		content, err = os.ReadFile(contentFile)
		if err != nil {
			return nil, err
		}
		return content, nil
	}
	return contentDefault, nil
}

type Template struct {
	Config          *Config
	Tables          []*Table // All exported tables
	AllTableColumns []string // A list of all columns from all tables, with duplicates removed based on column names
}

type Table struct {
	Config     *Config            `db:"-"`             // config
	Database   string             `db:"table_schema"`  // database name
	Table      string             `db:"table_name"`    // table name (original table name)
	Comment    string             `db:"table_comment"` // table comment
	Columns    []*Column          `db:"-"`             // table columns
	Defined    string             `db:"-"`             // table DDL
	Indexes    []*Index           `db:"-"`             // table indexes
	ColumnsMap map[string]*Column `db:"-"`             // table column map

	AutoIncrementColumn string `db:"-"` // auto-increment column

	TableGoTypeName     string `db:"-"` // table go type name struct
	TableGoTypeSignName string `db:"-"` // table go type name struct with sign
}

func (s *Table) setColumns(columns []*Column) {
	s.Columns = columns
	s.ColumnsMap = make(map[string]*Column, len(s.Columns))
	for _, v := range s.Columns {
		s.ColumnsMap[v.Column] = v
	}
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
	GoTypeBase      string `db:"-"` // the base type pointed to by the pointer, such as string, int64, int ...
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
	_ = way
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
	s.GoTypeBase = strings.ReplaceAll(s.GoType, "*", "")
}

type Index struct {
	Name          string   `db:"index_name"`      // index name
	Column        string   `db:"index_column"`    // Multiple columns use, concatenation.
	IsPrimaryKey  int      `db:"is_primary_key"`  // Is it a primary key index?
	IsUniqueKey   int      `db:"is_unique_key"`   // Is it a unique key index?
	IsOrdinaryKey int      `db:"is_ordinary_key"` // Is it a index key index?
	IndexType     string   `db:"index_type"`      // btree or others
	Columns       []string `db:"-"`               // all columns
	Category      int      `db:"-"`               // 1:primary-key 2:unique-key 3:index key
	PrimaryKey    bool     `db:"-"`               // primary key bool value
	UniqueKey     bool     `db:"-"`               // unique key bool value
	OrdinaryKey   bool     `db:"-"`               // ordinary key bool value
}

func (s *Index) setColumns() *Index {
	s.Columns = strings.Split(s.Column, ",")
	if s.IsPrimaryKey == 1 {
		s.PrimaryKey = true
		s.UniqueKey = false
		s.OrdinaryKey = false
		s.Category = 1
	} else {
		if s.IsUniqueKey == 1 {
			s.PrimaryKey = false
			s.UniqueKey = true
			s.OrdinaryKey = false
			s.Category = 2
		} else {
			s.PrimaryKey = false
			s.UniqueKey = false
			s.OrdinaryKey = true
			s.Category = 3
		}
	}
	return s
}

func tableIndexMapKey(indexColumn []string, table *Table) string {
	length := len(indexColumn)
	key := make([]string, length)
	for i := 0; i < length; i++ {
		column := table.ColumnsMap[indexColumn[i]]
		if column.GoTypeBase == "string" {
			key[i] = "%s"
		} else {
			if strings.Contains(column.GoTypeBase, "int") {
				key[i] = "%d"
			} else {
				key[i] = "%v"
			}
		}
	}
	return strings.Join(key, "_")
}

func tableIndexMapVal(indexColumn []string, table *Table, paramName string, asterisk bool) string {
	length := len(indexColumn)
	if paramName == "" {
		paramName = "s"
	}
	value := make([]string, length)
	for i := 0; i < length; i++ {
		column := table.ColumnsMap[indexColumn[i]]
		if asterisk && column.GoType != column.GoTypeBase {
			value[i] = fmt.Sprintf("*%s.%s", paramName, column.ColumnPascal)
		} else {
			value[i] = fmt.Sprintf("%s.%s", paramName, column.ColumnPascal)
		}
	}
	return strings.Join(value, ", ")
}

func tablesQueryIndexes(ctx context.Context, way *hey.Way, tables []*Table, prepare string, args func(table *Table) []any) error {
	var errorQuery error
	once := &sync.Once{}
	waitGroup := &sync.WaitGroup{}
	query := func(table *Table) error {
		table.Indexes = make([]*Index, 0)
		err := way.Query(ctx, hey.NewSQL(prepare, args(table)...), way.RowsScan(&table.Indexes))
		if err != nil {
			return err
		}
		for _, index := range table.Indexes {
			index.setColumns()
		}
		return nil
	}
	for _, table := range tables {
		waitGroup.Add(1)
		go func(table *Table) {
			defer waitGroup.Done()
			err := query(table)
			if err != nil {
				once.Do(func() { errorQuery = err })
				return
			}
		}(table)
	}
	waitGroup.Wait()
	if errorQuery != nil {
		return errorQuery
	}
	return nil
}

// Schema Parse the structure of tables and columns in the database
type Schema interface {
	// QueryTables Get all tables in a database
	QueryTables(ctx context.Context, cfg *Config, schema string) ([]*Table, error)

	// QueryTableDefineSql Get the DDL of a specific table in a database
	QueryTableDefineSql(ctx context.Context, cfg *Config, table *Table) (string, error)

	// QueryColumns Get all columns of a specific table in a database
	QueryColumns(ctx context.Context, cfg *Config, schema string, table string) ([]*Column, error)

	// QueryIndexes Get all index of tables
	QueryIndexes(ctx context.Context, cfg *Config, tables []*Table) error

	// QuerySchemas Call QueryTableDefineSql, QueryColumns and QueryIndexes.
	QuerySchemas(ctx context.Context, cfg *Config, tables []*Table) error
}

// autoIncrementRegexpReplace Auto-increment column.
var autoIncrementRegexpReplace = regexp.MustCompile(`(AUTO_INCREMENT|auto_increment)=\d+`)

/* MySQL */

type SchemaMysql struct {
	way *hey.Way
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

func (s *SchemaMysql) QueryIndexes(ctx context.Context, cfg *Config, tables []*Table) error {
	prepare := `
SELECT
    i.relname AS index_name,
    STRING_AGG(a.attname, ',' ORDER BY 
        array_position(ix.indkey, a.attnum::smallint)
    ) AS index_column,
    CASE WHEN ix.indisprimary THEN 1 ELSE 0 END AS is_primary_key,
    CASE WHEN ix.indisunique THEN 1 ELSE 0 END AS is_unique_key,
    CASE WHEN NOT ix.indisprimary AND NOT ix.indisunique THEN 1 ELSE 0 END AS is_ordinary_key,
    am.amname AS index_type
FROM pg_class t
JOIN pg_index ix ON t.oid = ix.indrelid
JOIN pg_class i ON i.oid = ix.indexrelid
JOIN pg_am am ON i.relam = am.oid
JOIN pg_attribute a ON a.attrelid = t.oid 
    AND a.attnum = ANY(ix.indkey)
WHERE t.relname = ?
AND t.relkind = 'r'
GROUP BY i.relname, ix.indisprimary, ix.indisunique, am.amname
ORDER BY is_primary_key DESC, is_unique_key DESC, i.relname;
`
	return tablesQueryIndexes(ctx, s.way, tables, prepare, func(t *Table) []any {
		return []any{t.Table}
	})
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
			table.setColumns(columns)
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
	initAllTablesAllColumns(cfg, s.way, tables)
	err := s.QueryIndexes(ctx, cfg, tables)
	if err != nil {
		return err
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

func (s *SchemaPostgresql) QueryIndexes(ctx context.Context, cfg *Config, tables []*Table) error {
	prepare := `
SELECT
    i.relname AS index_name,
    STRING_AGG(a.attname, ',' ORDER BY 
        array_position(ix.indkey, a.attnum::smallint)
    ) AS index_column,
    CASE WHEN ix.indisprimary THEN 1 ELSE 0 END AS is_primary_key,
    CASE WHEN ix.indisunique THEN 1 ELSE 0 END AS is_unique_key,
    CASE WHEN NOT ix.indisprimary AND NOT ix.indisunique THEN 1 ELSE 0 END AS is_ordinary_key,
    am.amname AS index_type
FROM pg_class t
JOIN pg_index ix ON t.oid = ix.indrelid
JOIN pg_class i ON i.oid = ix.indexrelid
JOIN pg_am am ON i.relam = am.oid
JOIN pg_attribute a ON a.attrelid = t.oid 
    AND a.attnum = ANY(ix.indkey)
WHERE t.relname = ?
AND t.relkind = 'r'
GROUP BY i.relname, ix.indisprimary, ix.indisunique, am.amname
ORDER BY is_primary_key DESC, is_unique_key DESC, i.relname;
`
	return tablesQueryIndexes(ctx, s.way, tables, prepare, func(t *Table) []any {
		return []any{t.Table}
	})
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
			table.setColumns(columns)
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
	initAllTablesAllColumns(cfg, s.way, tables)
	err := s.QueryIndexes(ctx, cfg, tables)
	if err != nil {
		return err
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

func (s *SchemaSqlite) QueryTableDefineSql(ctx context.Context, cfg *Config, table *Table) (string, error) {
	return table.Defined, nil
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

func (s *SchemaSqlite) QueryIndexes(ctx context.Context, cfg *Config, tables []*Table) error {
	prepare := `
SELECT 
    idx_name AS index_name,
    group_concat(col_name, ',') AS index_column,
	is_primary_key,
    is_unique_key,
    CASE 
        WHEN is_primary_key = 0 AND is_unique_key = 0 THEN 1 
        ELSE 0 
    END AS is_ordinary_key,
    idx_type AS index_type
FROM (
    -- ordinary index
    SELECT 
        m.name AS idx_name,
        ii.name AS col_name,
        ii.seqno AS col_order,
        0 AS is_primary_key,
        CASE WHEN m.sql LIKE '%UNIQUE%' THEN 1 ELSE 0 END AS is_unique_key,
        CASE 
            WHEN m.sql LIKE '%UNIQUE%' THEN 'UNIQUE INDEX'
            ELSE 'INDEX'
        END AS idx_type
    FROM sqlite_master m
    CROSS JOIN pragma_index_info(m.name) ii
    WHERE m.type = 'index' 
    AND m.tbl_name = ?
    
    UNION ALL
    
    -- primary key index
    SELECT 
        'PRIMARY' AS idx_name,
        ti.name AS col_name,
        ti.pk AS col_order,
        1 AS is_primary_key,
        0 AS is_unique_key,
        'PRIMARY KEY' AS idx_type
    FROM pragma_table_info(?) ti
    WHERE ti.pk > 0
) t
GROUP BY idx_name, is_primary_key, is_unique_key, idx_type
ORDER BY is_primary_key DESC, is_unique_key DESC, idx_name;
`
	return tablesQueryIndexes(ctx, s.way, tables, prepare, func(t *Table) []any {
		return []any{t.Table, t.Table}
	})
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
		table.setColumns(columns)
	}
	initAllTablesAllColumns(cfg, s.way, tables)
	err := s.QueryIndexes(ctx, cfg, tables)
	if err != nil {
		return err
	}
	return nil
}

func NewSchemaSqlite(way *hey.Way) *SchemaSqlite {
	schema := &SchemaSqlite{}
	schema.way = way
	return schema
}

func removeNewlineCharacter(s string) string {
	substr := "\r\n"
	replace := ""
	if strings.Contains(s, substr) {
		s = strings.ReplaceAll(s, substr, replace)
	}
	substr = "\r"
	if strings.Contains(s, substr) {
		s = strings.ReplaceAll(s, substr, replace)
	}
	substr = "\n"
	if strings.Contains(s, substr) {
		s = strings.ReplaceAll(s, substr, replace)
	}
	return s
}

func initAllTablesAllColumns(config *Config, way *hey.Way, tables []*Table) {
	timestamp := time.Now().Unix()
	for _, t := range tables {
		if t.Comment == "" {
			t.Comment = t.Table
		} else {
			t.Comment = removeNewlineCharacter(t.Comment)
		}
		// Handle naming
		{
			if t.TableGoTypeName == "" {
				name := t.Table
				if config.Database.TablePrefix != "" {
					name = strings.TrimPrefix(name, config.Database.TablePrefix)
				}
				t.TableGoTypeName = Pascal(name)
				sign := strings.ReplaceAll(config.Sign, " ", "")
				if sign == "" {
					sign = fmt.Sprintf("%d", timestamp)
				}
				t.TableGoTypeSignName = fmt.Sprintf("T%s%s", sign, t.TableGoTypeName)
			}
			for _, c := range t.Columns {
				c.init(way)
				c.Comment = removeNewlineCharacter(c.Comment)
			}
		}
		// Method table column
		{
			if config.OutputSchemaTableColumnMethods == nil {
				config.OutputSchemaTableColumnMethods = make(map[string]*SchemaTableColumnMethods)
			}
			value, ok := config.OutputSchemaTableColumnMethods[t.Table]
			if !ok {
				value = &SchemaTableColumnMethods{}
				config.OutputSchemaTableColumnMethods[t.Table] = value
			}
			if t.AutoIncrementColumn != "" {
				assoc := make(map[string]*struct{})
				for _, column := range value.Select {
					assoc[column] = nil
				}
				if _, ok = assoc[t.AutoIncrementColumn]; !ok {
					columns := make([]string, 0, len(value.Select)+1)
					columns = append(columns, t.AutoIncrementColumn)
					columns = append(columns, value.Select...)
					value.Select = columns
				}
			}
		}
	}
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
		onlyTableMap[t] = nil
	}
	onlyTable := len(onlyTableMap) > 0

	tables := make([]*Table, 0, len(lists))
	for _, t := range lists {
		t.Config = config
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
	return tables, nil
}
