package main

import (
	"github.com/spf13/cobra"
	"github.com/theoparis/tav/internal/tav"
)

func newOpCmd() *cobra.Command {
	op := &cobra.Command{Use: "op", Short: "Operation log commands"}
	op.AddCommand(&cobra.Command{
		Use:   "log",
		Short: "Show tav operation log",
		RunE: func(cmd *cobra.Command, args []string) error {
			return tav.OpLog(".")
		},
	})
	return op
}
