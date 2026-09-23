package dataflow

import (
	"sort"
	"strings"
)

// piiCategory groups field-name keywords into a privacy label. Keywords are
// matched against the normalized field name: multiword keywords ("first_
// name") by substring, single tokens ("card", "ip") by exact word match so
// "description" never lights up the "ip" category.
type piiCategory struct {
	category string
	keywords []string
}

var piiCategories = []piiCategory{
	{"password", []string{"password", "passwd", "pwd"}},
	{"secret", []string{"token", "secret", "apikey", "api_key", "credential", "session", "jwt", "auth"}},
	{"payment", []string{"card", "pan", "cvv", "cvc", "iban", "expiry"}},
	{"email", []string{"email", "e_mail", "mail"}},
	{"phone", []string{"phone", "mobile", "tel", "msisdn"}},
	{"government_id", []string{"ssn", "passport", "tax_id", "national_id"}},
	{"birth", []string{"birth", "dob", "age"}},
	{"name", []string{"first_name", "last_name", "full_name", "surname", "customer_name", "display_name"}},
	{"address", []string{"street", "zip", "postal", "street_address", "postal_address", "home_address", "billing_address", "shipping_address", "mailing_address"}},
	{"geo", []string{"city", "country", "region", "location", "lat", "lon", "lng"}},
	{"ip", []string{"ip", "ip_address", "client_ip", "remote_addr"}},
	{"device", []string{"device", "user_agent", "imei", "fingerprint"}},
}

// ClassifyPII maps field names to deduplicated, comma-joined privacy
// categories ("email,phone"). It runs in the SDK: only labels travel in
// span metadata, field values stay in the E2E-encrypted payload.
func ClassifyPII(fields []string) string {
	norm := strings.NewReplacer("-", "_", " ", "_", ".", "_").Replace
	seen := map[string]bool{}
	var out []string
	for _, f := range fields {
		n := norm(strings.ToLower(f))
		tokens := map[string]bool{}
		for _, t := range strings.Split(n, "_") {
			tokens[t] = true
		}
		for _, cat := range piiCategories {
			if seen[cat.category] {
				continue
			}
			for _, kw := range cat.keywords {
				hit := false
				if strings.Contains(kw, "_") {
					hit = strings.Contains(n, kw)
				} else {
					hit = tokens[kw]
				}
				if hit {
					seen[cat.category] = true
					out = append(out, cat.category)
					break
				}
			}
		}
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// classifyPII is the internal alias used by Span.End.
func classifyPII(fields []string) string { return ClassifyPII(fields) }
