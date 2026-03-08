package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cd365/hey/v7"
	"github.com/cd365/pts/app"
	"github.com/spf13/cobra"
)

const (
	flagConfigure       = "config"
	flagTable           = "table"
	flagTemplateFile    = "template_file"
	flagTemplateDefault = "template_default"

	flagTableUsage           = "Only table lists, multiple uses ',' concatenation. Example: table1,table2,table3..."
	flagTemplateFileUsage    = "Use a custom template file"
	flagTemplateDefaultUsage = "Output default template content"
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
		cmd.Flags().StringP(flagConfigure, "c", "example.yaml", "Configuration file, ENV: PTS_CUSTOM_CONFIG")
		cmd.Flags().StringP(flagTable, "t", "", flagTableUsage)
		cmd.Flags().StringP(flagTemplateFile, "u", "", flagTemplateFileUsage)
		cmd.Flags().BoolP(flagTemplateDefault, "a", false, flagTemplateDefaultUsage)
		rootCmd.AddCommand(cmd)
	}

	{
		cmd := &cobra.Command{
			Use:   app.CmdModel,
			Short: "Table structure mapped to Go struct",
			Long:  "Parse the database table structure and define the corresponding Go structs",
			RunE: func(cmd *cobra.Command, args []string) error {
				return start(cmd, args, app.CmdModel)
			},
		}
		cmd.Flags().StringP(flagConfigure, "c", "example.yaml", "Configuration file, ENV: PTS_MODEL_CONFIG")
		cmd.Flags().StringP(flagTable, "t", "", flagTableUsage)
		cmd.Flags().StringP(flagTemplateFile, "u", "", flagTemplateFileUsage)
		cmd.Flags().BoolP(flagTemplateDefault, "a", false, flagTemplateDefaultUsage)
		rootCmd.AddCommand(cmd)
	}

	{
		cmd := &cobra.Command{
			Use:   app.CmdSchema,
			Short: "Table structure and common methods",
			Long:  "Parse database table structures into non-hard-coded structures in Go, preventing the use of hard-coded data in the code",
			RunE: func(cmd *cobra.Command, args []string) error {
				return start(cmd, args, app.CmdSchema)
			},
		}
		cmd.Flags().StringP(flagConfigure, "c", "example.yaml", "Configuration file, ENV: PTS_SCHEMA_CONFIG")
		cmd.Flags().StringP(flagTable, "t", "", flagTableUsage)
		cmd.Flags().StringP(flagTemplateFile, "u", "", flagTemplateFileUsage)
		cmd.Flags().BoolP(flagTemplateDefault, "a", false, flagTemplateDefaultUsage)
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
		cmd.Flags().StringP(flagConfigure, "c", "example.yaml", "Configuration file, ENV: PTS_REPLACE_CONFIG")
		cmd.Flags().StringP(flagTable, "t", "", flagTableUsage)
		cmd.Flags().StringP(flagTemplateFile, "u", "", flagTemplateFileUsage)
		cmd.Flags().BoolP(flagTemplateDefault, "a", false, flagTemplateDefaultUsage)
		rootCmd.AddCommand(cmd)
	}

	{
		cmd := &cobra.Command{
			Use:   app.CmdTable,
			Short: "Output template code to a file for a single table structure",
			Long:  "Output template code to a file for a single table structure, usually the file has a specific suffix.",
			RunE: func(cmd *cobra.Command, args []string) error {
				return start(cmd, args, app.CmdTable)
			},
		}
		cmd.Flags().StringP(flagConfigure, "c", "example.yaml", "Configuration file, ENV: PTS_TABLE_CONFIG")
		cmd.Flags().StringP(flagTable, "t", "", flagTableUsage)
		cmd.Flags().BoolP(flagTemplateDefault, "a", false, flagTemplateDefaultUsage)
		rootCmd.AddCommand(cmd)
	}

	{
		cmd := &cobra.Command{
			Use:   "git",
			Short: "View git commit id",
			RunE: func(cmd *cobra.Command, args []string) error {
				_, err := os.Stdout.WriteString(app.GitCommitId)
				if err != nil {
					return err
				}
				_, err = os.Stdout.WriteString("\n")
				if err != nil {
					return err
				}
				return nil
			},
		}
		rootCmd.AddCommand(cmd)
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Println("Error:", err.Error())
	}
}

func start(cmd *cobra.Command, args []string, command string) error {
	{
		// Whether to output the default template content.
		if value, err := cmd.Flags().GetBool(flagTemplateDefault); err == nil && value {
			_, err = os.Stdout.Write(app.DefaultTemplateContent(command))
			return err
		}
	}

	configFile := ""
	// Get the configuration file path from the env.
	{
		key := fmt.Sprintf("PTS_%s_CONFIG", strings.ToUpper(command))
		if value := os.Getenv(key); value != "" {
			if _, err := os.Stat(value); err == nil {
				configFile = value
			}
		}
	}

	// Get the configuration file path from the command line.
	{
		value, err := cmd.Flags().GetString(flagConfigure)
		if err != nil {
			return err
		}
		if _, err := os.Stat(value); err == nil {
			configFile = value
		}
	}

	if configFile == "" {
		return errors.New("please set up the configuration file first")
	}

	cli, err := app.NewApp(configFile)
	if err != nil {
		return err
	}

	{
		// Use the given database table.
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

	{
		// Replace the default template file in the configuration file.
		value := ""
		value, err = cmd.Flags().GetString(flagTemplateFile)
		if err == nil {
			if _, err = os.Stat(value); err == nil {
				switch command {
				case app.CmdCustom:
					cli.Cfg().TemplateFileCustom = value
				case app.CmdModel:
					cli.Cfg().TemplateFileModel = value
				case app.CmdSchema:
					cli.Cfg().TemplateFileSchema = value
				case app.CmdReplace:
					cli.Cfg().TemplateFileReplace = value
				}
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*30)
	defer cancel()

	err = cli.Run(ctx, command, args)
	if err != nil {
		return err
	}

	return nil
}
