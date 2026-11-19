package main

import (
	"fmt"
	"github.com/spf13/cobra"
)

var rbdOperationRequest string

func init() {
	rootCmd.PersistentFlags().StringVar(&rbdOperationRequest, "rbd-operation", "", "rbd atomic command request")
	_ = rootCmd.PersistentFlags().MarkHidden("rbd-operation")
	rootCmd.AddCommand(&cobra.Command{Use: "rbd-capabilities", Hidden: true, Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(`{"atomic_command":1,"operation_marker":1}`)
	}})
}
