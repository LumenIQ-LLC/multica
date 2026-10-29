package handler

import "testing"

func TestIsDevVerificationCodeRequiresDevelopment(t *testing.T) {
	for _, tt := range []struct {
		name        string
		environment string
		want        bool
	}{
		{name: "development", environment: "development", want: true},
		{name: "normalized development", environment: " Development ", want: true},
		{name: "production", environment: "production"},
		{name: "empty", environment: ""},
		{name: "unknown", environment: "staging"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("APP_ENV", tt.environment)
			t.Setenv(devVerificationCodeEnv, "888888")
			if got := isDevVerificationCode("888888"); got != tt.want {
				t.Fatalf("isDevVerificationCode() = %v, want %v", got, tt.want)
			}
			if isDevVerificationCode("123456") {
				t.Fatal("isDevVerificationCode() accepted a mismatched code")
			}
		})
	}
}
