### INSTALL
```bash
go install github.com/cd365/pts/cmd/pts@latest
```

### TEMPLATE CODE CREATED BY PARSING TABLE STRUCTURE
```bash
pts custom -c config.yaml > create.sql
echo -e "package replace\n" > db1/replace/replace.go;pts replace -c config.yaml >> db1/replace/replace.go;go fmt db1/replace/replace.go
echo -e "package schema\n" > db1/schema/schema.go;pts schema -c config.yaml >> db1/schema/schema.go;go fmt db1/schema/schema.go
echo -e "package table\n" > db1/table/table.go;pts table -c config.yaml >> db1/table/table.go;go fmt db1/table/table.go
```
### KIND TIPS:
> Please do not use data keywords and reserved keywords as table names and column names in the database.