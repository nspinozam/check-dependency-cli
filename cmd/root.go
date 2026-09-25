package cmd

import (
	"github.com/spf13/cobra"

	"github.com/nspinozam/check-dependency-cli/internal/checker"
)

const version = "0.0.1"

var rootCmd = &cobra.Command{
	Use:     "check-dependency",
	Short:   "Check project dependencies",
	Version: version,
}

var checkCmd = &cobra.Command{
	Use:   "check",
	Short: "Validate dependencies in a Kubernetes cluster",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return checker.Run(checkFile, kubeconfig, contextName)
	},
}

var (
	checkFile   string
	kubeconfig  string
	contextName string
)

func init() {
	checkCmd.Flags().StringVarP(&checkFile, "file", "f", "dependencies.yaml", "Dependency definition file")
	checkCmd.Flags().StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig")
	checkCmd.Flags().StringVar(&contextName, "context", "", "Kubeconfig context")
	rootCmd.AddCommand(checkCmd)
}

func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}
