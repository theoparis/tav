package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/theoparis/tav/internal/tav"
)

func newSquashCmd() *cobra.Command {
	var opts tav.SquashOptions
	c := &cobra.Command{
		Use:   "squash",
		Short: "Move changes from a revision into another revision",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				return fmt.Errorf("fileset arguments are not implemented yet: %s", strings.Join(args, " "))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return tav.SquashWithOptions(".", opts)
		},
	}
	c.Flags().StringVarP(&opts.Revision, "revision", "r", "", "revision to squash into its parent")
	c.Flags().StringSliceVarP(&opts.From, "from", "f", nil, "revision(s) to squash from")
	c.Flags().StringVarP(&opts.Into, "into", "t", "", "destination revision")
	c.Flags().StringVar(&opts.Into, "to", "", "destination revision")
	c.Flags().StringSliceVarP(&opts.Onto, "onto", "o", nil, "(experimental) place squashed revision onto")
	c.Flags().StringSliceVarP(&opts.InsertAfter, "insert-after", "A", nil, "(experimental) place squashed revision after")
	c.Flags().StringSliceVarP(&opts.InsertBefore, "insert-before", "B", nil, "(experimental) place squashed revision before")
	c.Flags().StringVarP(&opts.Message, "message", "m", "", "description for squashed revision")
	c.Flags().BoolVarP(&opts.UseDestinationMessage, "use-destination-message", "u", false, "keep destination description")
	c.Flags().BoolVarP(&opts.KeepEmptied, "keep-emptied", "k", false, "keep emptied source revision")
	return c
}

func newRebaseCmd() *cobra.Command {
	var opts tav.RebaseOptions
	c := &cobra.Command{
		Use:   "rebase",
		Short: "Rebase revisions",
		RunE: func(cmd *cobra.Command, args []string) error {
			return tav.RebaseWithOptions(".", opts)
		},
	}
	c.Flags().StringVarP(&opts.Source, "source", "s", "", "rebase source and descendants")
	c.Flags().StringVarP(&opts.Branch, "branch", "b", "", "rebase branch")
	c.Flags().StringSliceVarP(&opts.Revisions, "revisions", "r", nil, "rebase specified revisions only")
	c.Flags().StringVarP(&opts.Onto, "onto", "o", "", "destination revision")
	c.Flags().StringVarP(&opts.InsertAfter, "insert-after", "A", "", "insert after revision")
	c.Flags().StringVarP(&opts.InsertBefore, "insert-before", "B", "", "insert before revision")
	return c
}
