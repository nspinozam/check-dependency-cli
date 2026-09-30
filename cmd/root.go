package cmd

import (
	"github.com/spf13/cobra"

	"github.com/nspinozam/check-dependency-cli/internal/checker"
)

const version = "0.0.3"

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
		return checker.Run(checkFile, kubeconfig, contextName, daprHTTPAddress)
	},
}

var (
	checkFile       string
	kubeconfig      string
	contextName     string
	daprHTTPAddress string
)

func init() {
	checkCmd.Flags().StringVarP(&checkFile, "file", "f", "dependencies.yaml", "Dependency definition file")
	checkCmd.Flags().StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig")
	checkCmd.Flags().StringVar(&contextName, "context", "", "Kubeconfig context")
	checkCmd.Flags().StringVar(&daprHTTPAddress, "dapr-http-address", "http://127.0.0.1:3500", "Dapr sidecar HTTP address")
	rootCmd.AddCommand(checkCmd)
}

func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}
