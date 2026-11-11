package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"mirrorcore/internal/database"
	"mirrorcore/internal/repository/postgres"
)

const confirmationPhrase = "I_VERIFIED_PROVIDER_TERMINAL"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("asr-unknown-resolve", flag.ContinueOnError)
	submissionIDValue := flags.String("submission-id", "", "UNKNOWN submission UUID without provider task ID")
	operatorID := flags.String("operator", "", "operator identity recorded in the audit table")
	terminalStatus := flags.String("terminal-status", "", "verified provider terminal status or NOT_ACCEPTED")
	evidenceRef := flags.String("evidence-ref", "", "provider case/ticket or internal evidence reference; do not use signed URLs")
	reason := flags.String("reason", "", "short explanation of the provider verification")
	confirmation := flags.String("confirm", "", "must equal I_VERIFIED_PROVIDER_TERMINAL")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *confirmation != confirmationPhrase {
		return errors.New("refusing to resolve without --confirm I_VERIFIED_PROVIDER_TERMINAL")
	}
	if strings.TrimSpace(os.Getenv("DATABASE_URL")) == "" {
		return errors.New("DATABASE_URL is required")
	}
	submissionID, err := uuid.Parse(strings.TrimSpace(*submissionIDValue))
	if err != nil {
		return fmt.Errorf("--submission-id must be a UUID: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := database.Open(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer pool.Close()
	repository := postgres.NewASRReconciliationRepository(pool)
	err = repository.ResolveUnknownWithoutTaskID(ctx, postgres.ASRManualResolution{
		SubmissionID: submissionID, OperatorID: *operatorID, TerminalStatus: *terminalStatus,
		EvidenceRef: *evidenceRef, Reason: *reason,
	})
	if err != nil {
		return err
	}
	fmt.Printf("submission=%s state=REMOTE_TERMINAL provider_status=OPERATOR_CONFIRMED_%s\n",
		submissionID, strings.ToUpper(strings.TrimSpace(*terminalStatus)))
	return nil
}
