package dataflow

import (
	"reflect"
	"strings"
	"testing"
)

func TestClassifyPII(t *testing.T) {
	cases := []struct {
		fields []string
		want   []string
	}{
		{[]string{"email", "user_id"}, []string{"email"}},
		{[]string{"card_number", "cvv", "order_id"}, []string{"payment"}},
		{[]string{"password"}, []string{"password"}},
		{[]string{"api_key"}, []string{"secret"}},
		{[]string{"client_ip", "user_agent"}, []string{"device", "ip"}},
		{[]string{"first_name", "last_name", "city", "zip"}, []string{"address", "geo", "name"}},
		{[]string{"description", "service_name", "count"}, nil}, // no false positives
		{[]string{"email", "phone", "mailing_address"}, []string{"address", "email", "phone"}},
	}
	for _, c := range cases {
		got := ClassifyPII(c.fields)
		var want string
		if len(c.want) > 0 {
			want = strings.Join(c.want, ",")
		}
		if got != want {
			t.Errorf("ClassifyPII(%v) = %q, want %q", c.fields, got, want)
		}
	}
}

func TestClassifyPIIWordBoundaries(t *testing.T) {
	// "description" contains the letters i+p but is not an IP field;
	// "image" contains "age" but is not a birth field.
	if got := ClassifyPII([]string{"description", "image"}); got != "" {
		t.Errorf("unexpected categories %q for benign fields", got)
	}
	if !reflect.DeepEqual(strings.Split(ClassifyPII([]string{"ip_address"}), ","), []string{"ip"}) {
		t.Errorf("ip_address should classify as ip")
	}
}
