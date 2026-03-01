package main

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/theoparis/tav/internal/tav"
)

func newInitCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Initialize tav metadata in the current git repository",
		RunE: func(cmd *cobra.Command, args []string) error {
			return tav.Init(".")
		},
	}
}

func newStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show current tav change and worktree summary",
		RunE: func(cmd *cobra.Command, args []string) error {
			return tav.Status(".")
		},
	}
}

func newLogCmd() *cobra.Command {
	var revspec string
	c := &cobra.Command{
		Use:   "log",
		Short: "Show tav change log",
		RunE: func(cmd *cobra.Command, args []string) error {
			return tav.LogWithRev(".", strings.TrimSpace(revspec))
		},
	}
	c.Flags().StringVarP(&revspec, "revision", "r", "", "change-id or range (A..B, ..B, A..)")
	return c
}
