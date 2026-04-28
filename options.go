package smartdb

import (
	"database/sql"
)

// WithGenerator returns an Option that sets a custom generator for SmartDB.
// If the provided generator is nil, the option does nothing (the default generator is retained).
func WithGenerator(gen Generator) Option {
	return func(s *SmartDB) {
		if gen != nil {
			s.gen = gen
		}
	}
}

// WithTxOptions returns an Option that sets transaction options for top-level transactions
// created by SmartDB. These options are passed to BeginTx when creating new transactions.
func WithTxOptions(opts *sql.TxOptions) Option {
	return func(s *SmartDB) {
		s.txOpts = opts
	}
}

// WithLogger returns an Option that sets a custom logger for SmartDB.
// If the provided logger is nil, the option does nothing (the default logger is retained).
func WithLogger(log Logger) Option {
	return func(s *SmartDB) {
		if log != nil {
			s.log = log
		}
	}
}
