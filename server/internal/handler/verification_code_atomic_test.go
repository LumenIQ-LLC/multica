package handler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestVerificationCodeConcurrentConsumption(t *testing.T) {
	t.Setenv(devVerificationCodeEnv, "")
	email := fmt.Sprintf("atomic-code-%d@example.test", time.Now().UnixNano())
	createVerificationCodeForTest(t, email, "123456")
	results := make(chan error, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			ready.Done()
			<-start
			results <- testHandler.consumeVerificationCode(context.Background(), email, "123456")
		}()
	}
	ready.Wait()
	close(start)
	successes, rejections := 0, 0
	for range 2 {
		err := <-results
		if err == nil {
			successes++
		} else if errors.Is(err, errInvalidVerificationCode) {
			rejections++
		} else {
			t.Fatal(err)
		}
	}
	if successes != 1 || rejections != 1 {
		t.Fatalf("successes=%d rejections=%d", successes, rejections)
	}
}

func TestVerificationCodeConcurrentAttemptBudget(t *testing.T) {
	t.Setenv(devVerificationCodeEnv, "")
	email := fmt.Sprintf("atomic-attempts-%d@example.test", time.Now().UnixNano())
	createVerificationCodeForTest(t, email, "123456")
	results := make(chan error, 12)
	start := make(chan struct{})
	for range 12 {
		go func() { <-start; results <- testHandler.consumeVerificationCode(context.Background(), email, "wrong") }()
	}
	close(start)
	for range 12 {
		if err := <-results; !errors.Is(err, errInvalidVerificationCode) {
			t.Fatalf("unexpected result: %v", err)
		}
	}
	row, err := testHandler.Queries.GetLatestCodeByEmail(context.Background(), email)
	if err != nil {
		t.Fatal(err)
	}
	if row.Attempts != 5 {
		t.Fatalf("attempts=%d, want5", row.Attempts)
	}
	if err := testHandler.consumeVerificationCode(context.Background(), email, "123456"); !errors.Is(err, errInvalidVerificationCode) {
		t.Fatalf("exhausted code accepted: %v", err)
	}
}
