package main

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/lame13/pgvector-index-manager/cmd"
)

//go:embed VERSION
var version string

func main() {
	cmd.Version = strings.TrimSpace(version)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err := cmd.Execute(ctx)
	if err != nil && !errors.Is(err, cmd.ErrNothingToApply) {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(cmd.ExitCode(err))
}
