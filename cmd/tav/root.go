package main

import "github.com/spf13/cobra"

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:          "tav",
		Short:        "A jj-inspired VCS wrapper over git",
		SilenceUsage: true,
	}

	root.AddCommand(newInitCmd())
	root.AddCommand(newStatusCmd())
	root.AddCommand(newLogCmd())
	root.AddCommand(newNewCmd())
	root.AddCommand(newDescribeCmd())
	root.AddCommand(newCommitCmd())
	root.AddCommand(newSquashCmd())
	root.AddCommand(newRebaseCmd())
	root.AddCommand(newPushCmd())
	root.AddCommand(newAdvanceCmd())
	root.AddCommand(newOpCmd())
	return root
}
