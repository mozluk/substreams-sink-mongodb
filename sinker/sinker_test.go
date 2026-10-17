package sinker

import (
	"context"
	"testing"

	"github.com/streamingfast/bstream"
	pbdatabase "github.com/streamingfast/substreams-sink-mongodb/pb/substreams/sink/database/v1"
	"github.com/streamingfast/substreams-sink-mongodb/mongo"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"github.com/stretchr/testify/require"
)

func TestHandleBlockScopedData_NilGuards(t *testing.T) {
	s := &MongoSinker{}
	ctx := context.Background()

	// Test nil data
	err := s.HandleBlockScopedData(ctx, nil, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "received nil block scoped data")

	// Test nil output
	err = s.HandleBlockScopedData(ctx, &pbsubstreamsrpc.BlockScopedData{
		Output: nil,
	}, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "received block scoped data without an output module")

	// Test nil clock
	err = s.HandleBlockScopedData(ctx, &pbsubstreamsrpc.BlockScopedData{
		Output: &pbsubstreamsrpc.MapModuleOutput{
			Name: "test_module",
		},
		Clock: nil,
	}, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "received block scoped data without a clock")
}

func TestApplyDatabaseChanges_NullFieldValidation(t *testing.T) {
	s := &MongoSinker{
		tables: mongo.Tables{
			"test_table": {
				"null_field": mongo.NULL,
			},
		},
	}

	t.Run("null field mismatch error", func(t *testing.T) {
		changes := &pbdatabase.DatabaseChanges{
			TableChanges: []*pbdatabase.TableChange{
				{
					Table:     "test_table",
					Pk:        "1",
					Operation: pbdatabase.TableChange_CREATE,
					Fields: []*pbdatabase.Field{
						{
							Name:     "null_field",
							NewValue: "non-empty-value",
						},
					},
				},
			},
		}

		err := s.applyDatabaseChanges(context.Background(), bstream.BlockRefEmpty, changes)
		require.Error(t, err)
		require.Contains(t, err.Error(), "is typed as null but carries value")
	})

	t.Run("null field happy path success parsing", func(t *testing.T) {
		changes := &pbdatabase.DatabaseChanges{
			TableChanges: []*pbdatabase.TableChange{
				{
					Table:     "test_table",
					Pk:        "2",
					Operation: pbdatabase.TableChange_CREATE,
					Fields: []*pbdatabase.Field{
						{
							Name:     "null_field",
							NewValue: "",
						},
					},
				},
			},
		}

		// Validates that empty value for NULL typed field passes validation phase
		// and reaches loader step (panics due to nil loader, confirming parsing passed).
		require.Panics(t, func() {
			_ = s.applyDatabaseChanges(context.Background(), bstream.BlockRefEmpty, changes)
		})
	})
}

func TestApplyDatabaseChanges_FieldTypeParsingErrors(t *testing.T) {
	t.Run("invalid integer parsing", func(t *testing.T) {
		s := &MongoSinker{
			tables: mongo.Tables{
				"test_table": {
					"int_field": mongo.INTEGER,
				},
			},
		}
		changes := &pbdatabase.DatabaseChanges{
			TableChanges: []*pbdatabase.TableChange{
				{
					Table:     "test_table",
					Pk:        "1",
					Operation: pbdatabase.TableChange_CREATE,
					Fields: []*pbdatabase.Field{
						{
							Name:     "int_field",
							NewValue: "not-an-integer",
						},
					},
				},
			},
		}
		err := s.applyDatabaseChanges(context.Background(), bstream.BlockRefEmpty, changes)
		require.Error(t, err)
		require.Contains(t, err.Error(), "strconv.ParseInt")
	})

	t.Run("invalid double parsing", func(t *testing.T) {
		s := &MongoSinker{
			tables: mongo.Tables{
				"test_table": {
					"double_field": mongo.DOUBLE,
				},
			},
		}
		changes := &pbdatabase.DatabaseChanges{
			TableChanges: []*pbdatabase.TableChange{
				{
					Table:     "test_table",
					Pk:        "1",
					Operation: pbdatabase.TableChange_CREATE,
					Fields: []*pbdatabase.Field{
						{
							Name:     "double_field",
							NewValue: "not-a-double",
						},
					},
				},
			},
		}
		err := s.applyDatabaseChanges(context.Background(), bstream.BlockRefEmpty, changes)
		require.Error(t, err)
		require.Contains(t, err.Error(), "strconv.ParseFloat")
	})

	t.Run("invalid boolean parsing", func(t *testing.T) {
		s := &MongoSinker{
			tables: mongo.Tables{
				"test_table": {
					"bool_field": mongo.BOOLEAN,
				},
			},
		}
		changes := &pbdatabase.DatabaseChanges{
			TableChanges: []*pbdatabase.TableChange{
				{
					Table:     "test_table",
					Pk:        "1",
					Operation: pbdatabase.TableChange_CREATE,
					Fields: []*pbdatabase.Field{
						{
							Name:     "bool_field",
							NewValue: "not-a-boolean",
						},
					},
				},
			},
		}
		err := s.applyDatabaseChanges(context.Background(), bstream.BlockRefEmpty, changes)
		require.Error(t, err)
		require.Contains(t, err.Error(), "strconv.ParseBool")
	})
}

func TestApplyDatabaseChanges_DateParsing(t *testing.T) {
	s := &MongoSinker{
		tables: mongo.Tables{
			"test_table": {
				"date_field": mongo.DATE,
			},
		},
	}

	t.Run("malformed date string", func(t *testing.T) {
		changes := &pbdatabase.DatabaseChanges{
			TableChanges: []*pbdatabase.TableChange{
				{
					Table:     "test_table",
					Pk:        "1",
					Operation: pbdatabase.TableChange_CREATE,
					Fields: []*pbdatabase.Field{
						{
							Name:     "date_field",
							NewValue: "not-a-valid-date",
						},
					},
				},
			},
		}

		err := s.applyDatabaseChanges(context.Background(), bstream.BlockRefEmpty, changes)
		require.Error(t, err)
		require.Contains(t, err.Error(), "parsing time")
	})

	t.Run("valid RFC3339 date string", func(t *testing.T) {
		changes := &pbdatabase.DatabaseChanges{
			TableChanges: []*pbdatabase.TableChange{
				{
					Table:     "test_table",
					Pk:        "2",
					Operation: pbdatabase.TableChange_CREATE,
					Fields: []*pbdatabase.Field{
						{
							Name:     "date_field",
							NewValue: "2023-01-01T15:04:05Z",
						},
					},
				},
			},
		}

		// Validates that valid RFC3339 date parsing succeeds without returning an error
		// prior to reaching s.loader.Save().
		require.Panics(t, func() {
			_ = s.applyDatabaseChanges(context.Background(), bstream.BlockRefEmpty, changes)
		})
	})
}
