### INSTALL
```bash
go install github.com/cd365/pts/cmd/pts@latest
```

### TEMPLATE CODE CREATED BY PARSING TABLE STRUCTURE
```bash
pts custom -c config.yaml > create.sql
echo -e "package model\n" > db1/model/model.go;pts model -c config.yaml >> db1/model/model.go;go fmt db1/model/model.go
echo -e "package schema\n" > db1/schema/schema.go;pts schema -c config.yaml >> db1/schema/schema.go;go fmt db1/schema/schema.go
echo -e "package replace\n" > db1/replace/replace.go;pts replace -c config.yaml >> db1/replace/replace.go;go fmt db1/replace/replace.go
pts table -c config.yaml
```
### KIND TIPS:
> Please do not use data keywords and reserved keywords as table names and column names in the database.