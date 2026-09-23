// Package sdk is a compatibility entry point matching the documented
// zero-config import path:
//
//	import _ "github.com/huginnlabs-dev/dataflow-go/sdk"
//
// It pulls in the root dataflow package, whose init() performs auto-setup
// from DATAFLOW_* environment variables. Configuration can be overridden
// beforehand via dataflow.Configure.
package sdk

import _ "github.com/huginnlabs-dev/dataflow-go"
