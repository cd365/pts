package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/cd365/hey/v7"
	"github.com/cd365/pts/app"
	"github.com/spf13/cobra"
)

const (
	flagConfigure = "config"
	flagTable     = "table"
)

var rootCmd = &cobra.Command{
	Use:  "pts",
	Long: "Parsing database table structure, supports PostgreSQL, MySQL, SQLite",
}

func main() {
	{
		cmd := &cobra.Command{
			Use:   app.CmdConfig,
			Short: "Configuration example",
			RunE: func(cmd *cobra.Command, args []string) error {
				_, err := os.Stdout.Write(app.ExampleConfig)
				if err != nil {
					return err
				}
				return nil
			},
		}
		rootCmd.AddCommand(cmd)
	}
	{
		cmd := &cobra.Command{
			Use:   app.CmdCustom,
			Short: "Custom export",
			RunE: func(cmd *cobra.Command, args []string) error {
				return start(cmd, args, app.CmdCustom)
			},
		}
		cmd.Flags().StringP(flagConfigure, "c", "pts-custom.yaml", "Custom configure file path. PTS_CUSTOM_CONFIG")
		cmd.Flags().StringP(flagTable, "t", "", "Only table lists, multiple uses ',' concatenation. Example: table1,table2,table3...")
		rootCmd.AddCommand(cmd)
	}
	{
		cmd := &cobra.Command{
			Use:   app.CmdReplace,
			Short: "Database identifier mapping",
			Long:  "Commonly used to replace identifiers in a database",
			RunE: func(cmd *cobra.Command, args []string) error {
				return start(cmd, args, app.CmdReplace)
			},
		}
		cmd.Flags().StringP(flagConfigure, "c", "pts-replace.yaml", "Replace configure file path. PTS_REPLACE_CONFIG")
		cmd.Flags().StringP(flagTable, "t", "", "Only table lists, multiple uses ',' concatenation. Example: table1,table2,table3...")
		rootCmd.AddCommand(cmd)
	}

	{
		cmd := &cobra.Command{
			Use:   app.CmdSchema,
			Short: "Database table structure",
			Long:  "Parse database table structures into non-hard-coded structures in Go, preventing the use of hard-coded data in the code",
			RunE: func(cmd *cobra.Command, args []string) error {
				return start(cmd, args, app.CmdSchema)
			},
		}
		cmd.Flags().StringP(flagConfigure, "c", "pts-schema.yaml", "Schema configure file path. PTS_SCHEMA_CONFIG")
		cmd.Flags().StringP(flagTable, "t", "", "Only table lists, multiple uses ',' concatenation. Example: table1,table2,table3...")
		rootCmd.AddCommand(cmd)
	}

	{
		cmd := &cobra.Command{
			Use:   app.CmdTable,
			Short: "Database table data",
			Long:  "Parse the database table structure and define the corresponding Go structs",
			RunE: func(cmd *cobra.Command, args []string) error {
				return start(cmd, args, app.CmdTable)
			},
		}
		cmd.Flags().StringP(flagConfigure, "c", "pts-table.yaml", "Table configure file path. PTS_TABLE_CONFIG")
		cmd.Flags().StringP(flagTable, "t", "", "Only table lists, multiple uses ',' concatenation. Example: table1,table2,table3...")
		rootCmd.AddCommand(cmd)
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Println("Error:", err.Error())
	}
}

func start(cmd *cobra.Command, args []string, command string) error {
	configFile, err := cmd.Flags().GetString(flagConfigure)
	if err != nil {
		return err
	}
	// Try to get the configuration file path from the environment variables
	if _, err = os.Stat(configFile); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			key := fmt.Sprintf("PTS_%s_CONFIG", strings.ToUpper(command))
			if value := os.Getenv(key); value != "" {
				if _, err = os.Stat(value); err == nil {
					configFile = value
				}
			}
		}
	}
	cli, err := app.NewApp(configFile)
	if err != nil {
		return err
	}

	{
		values := ""
		values, err = cmd.Flags().GetString(flagTable)
		if err != nil {
			return err
		}
		tables := strings.Split(strings.TrimSpace(values), ",")
		tables = hey.DiscardDuplicate(func(tmp string) bool {
			if strings.TrimSpace(tmp) == "" {
				return true
			}
			return false
		}, tables...)
		if len(tables) > 0 {
			cli.Cfg().OnlyTable = tables
		}
	}

	output, err := cli.Run(context.Background(), cli.NewOutput(command))
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(output)
	if err != nil {
		return err
	}
	return err
}
