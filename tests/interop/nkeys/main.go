// Copyright (c) 2023-2026 Sam Orlando
// SPDX-License-Identifier: MIT (see LICENSE)
// nkeys / JWT interop helper for nats-lua tests.
//   gen <nonce>                  -> SEED/PUB/SIG lines from a fresh user key (nkeys)
//   verify <pub> <nonce> <sig>   -> VERIFY_OK if sig (base64url raw) verifies
//   jwt <jwt>                    -> JWT_OK <sub> <iss> when jwt/v2 decodes+validates it
//   operator <dir>               -> writes an operator-mode server config (<dir>/server.conf)
//                                   and the account seed (<dir>/account.seed) for user JWT tests
package main

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

func main() {
	switch os.Args[1] {
	case "gen":
		kp, _ := nkeys.CreateUser()
		seed, _ := kp.Seed()
		pub, _ := kp.PublicKey()
		sig, _ := kp.Sign([]byte(os.Args[2]))
		fmt.Printf("SEED %s\nPUB %s\nSIG %s\n", seed, pub, base64.RawURLEncoding.EncodeToString(sig))
	case "verify":
		kp, err := nkeys.FromPublicKey(os.Args[2])
		if err != nil { fmt.Println("VERIFY_ERR", err); os.Exit(1) }
		sig, err := base64.RawURLEncoding.DecodeString(os.Args[4])
		if err != nil { fmt.Println("VERIFY_ERR", err); os.Exit(1) }
		if err := kp.Verify([]byte(os.Args[3]), sig); err != nil { fmt.Println("VERIFY_ERR", err); os.Exit(1) }
		fmt.Println("VERIFY_OK")
	case "operator":
		dir := os.Args[2]
		okp, _ := nkeys.CreateOperator()
		opub, _ := okp.PublicKey()
		akp, _ := nkeys.CreateAccount()
		apub, _ := akp.PublicKey()
		aseed, _ := akp.Seed()
		oc := jwt.NewOperatorClaims(opub)
		oc.Name = "nats-lua-test-op"
		ojwt, err := oc.Encode(okp)
		if err != nil { panic(err) }
		ac := jwt.NewAccountClaims(apub)
		ac.Name = "nats-lua-test-acc"
		ajwt, err := ac.Encode(okp)
		if err != nil { panic(err) }
		os.WriteFile(filepath.Join(dir, "account.seed"), aseed, 0600)
		conf := fmt.Sprintf("operator: %q\nresolver: MEMORY\nresolver_preload: {\n  %s: %q\n}\n", ojwt, apub, ajwt)
		os.WriteFile(filepath.Join(dir, "operator.conf"), []byte(conf), 0600)
		fmt.Println("OPERATOR_OK", apub)
	case "jwt":
		uc, err := jwt.DecodeUserClaims(os.Args[2])
		if err != nil { fmt.Println("JWT_ERR", err); os.Exit(1) }
		vr := jwt.CreateValidationResults()
		uc.Validate(vr)
		if len(vr.Errors()) > 0 { fmt.Println("JWT_ERR", vr.Errors()); os.Exit(1) }
		fmt.Println("JWT_OK", uc.Subject, uc.Issuer, uc.Name)
	}
}
