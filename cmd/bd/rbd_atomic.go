package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/steveyegge/beads/internal/storage/dolt"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/internal/validation"
)

var rbdOperationRequest string

func init() {
	rootCmd.PersistentFlags().StringVar(&rbdOperationRequest, "rbd-operation", "", "rbd atomic command request")
	_ = rootCmd.PersistentFlags().MarkHidden("rbd-operation")
	rootCmd.AddCommand(&cobra.Command{Use: "rbd-capabilities", Hidden: true, Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(`{"atomic_command":1,"operation_marker":1}`)
	}})
	rootCmd.AddCommand(&cobra.Command{Use: "rbd-close-guard", Hidden: true, Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(`{"close_if_assignee":1}`)
	}})
}

func rbdCloseRelease(ctx context.Context, cmd *cobra.Command, results []*RoutedResult) error {
	if !cmd.Flags().Changed("if-assignee") {
		return nil
	}
	expected, _ := cmd.Flags().GetString("if-assignee")
	if expected == "" || len(results) != 1 {
		return errors.New("close --if-assignee requires one issue and a non-empty assignee")
	}
	if !dolt.RBDOperationActive() {
		return errors.New("close --if-assignee requires one rbd atomic operation")
	}
	target := results[0]
	if target.Issue == nil || target.Issue.Status == types.StatusClosed {
		return nil
	}
	if target.Issue.Assignee == "" || !validation.ActorMatches(target.Issue.Assignee, expected) {
		return fmt.Errorf("cannot close %s: assignee is %q, expected %q", target.ResolvedID, target.Issue.Assignee, expected)
	}
	if validation.ActorMatches(target.Issue.Assignee, actor) {
		return nil
	}
	if err := target.Store.UnclaimIssueIfAssignee(ctx, target.ResolvedID, actor, expected); err != nil {
		return fmt.Errorf("cannot close %s: assignee is %q, expected %q: %v", target.ResolvedID, target.Issue.Assignee, expected, err)
	}
	released, err := target.Store.GetIssue(ctx, target.ResolvedID)
	if err != nil {
		return err
	}
	target.Issue = released
	return nil
}
