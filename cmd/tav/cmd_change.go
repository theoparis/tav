package main

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/theoparis/tav/internal/tav"
)

func newNewCmd() *cobra.Command {
	var opts tav.NewOptions
	c := &cobra.Command{
		Use:   "new [REVSETS]...",
		Short: "Create a new change",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.Parents = append(opts.Parents, args...)
			return tav.NewWithOptions(".", opts)
		},
	}
	c.Flags().StringVarP(&opts.Message, "message", "m", "", "description for the new change")
	c.Flags().BoolVar(&opts.NoEdit, "no-edit", false, "do not switch to editing the new change")
	c.Flags().StringSliceVarP(&opts.InsertAfter, "insert-after", "A", nil, "insert the new change after revsets")
	c.Flags().StringSliceVarP(&opts.InsertBefore, "insert-before", "B", nil, "insert the new change before revsets")
	c.Flags().StringSliceVarP(&opts.Parents, "onto", "o", nil, "parents of the new change")
	c.Flags().StringSliceVarP(&opts.Parents, "revision", "r", nil, "parents of the new change")
	return c
}

func newDescribeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "describe <description>",
		Short: "Set description for the current change",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return tav.Describe(".", strings.Join(args, " "))
		},
	}
}

func newCommitCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "commit [message]",
		Short: "Commit current change to git",
		RunE: func(cmd *cobra.Command, args []string) error {
			return tav.Commit(".", strings.Join(args, " "))
		},
	}
}
