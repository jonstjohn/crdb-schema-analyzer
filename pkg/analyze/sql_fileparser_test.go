package analyze

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestParseSQLFile(t *testing.T) {
	sql := `
-- some comment
CREATE TABLE foo (
  id INT PRIMARY KEY
);

-- BEGIN BLOCK
INSERT INTO foo VALUES (1);
INSERT INTO foo VALUES (2);
-- END BLOCK

DROP TABLE foo;
`

	expected := [][]string{
		{fmt.Sprintf("CREATE TABLE foo (\nid INT PRIMARY KEY\n);")},
		{`INSERT INTO foo VALUES (1);`, `INSERT INTO foo VALUES (2);`},
		{`DROP TABLE foo;`}}

	parser := NewSqlFileParser("")
	reader := strings.NewReader(sql)
	batches, err := parser.ParseReader(reader)
	require.NoError(t, err)
	require.Len(t, batches, 3)
	require.Len(t, batches[0], 1)
	require.Len(t, batches[1], 2)
	require.Len(t, batches[2], 1)

	assert.Equal(t, expected[0][0], batches[0][0])
	assert.Equal(t, expected[1][0], batches[1][0])
	assert.Equal(t, expected[1][1], batches[1][1])
	assert.Equal(t, expected[2][0], batches[2][0])
}

func TestParseSingleLine(t *testing.T) {
	parser := NewSqlFileParser("testdata/singleline.sql")
	batches, err := parser.Parse()
	require.NoError(t, err)
	/*
	   SELECT * FROM test;
	   INSERT INTO test(v) VALUES ('foo'), ('bar');
	   SET CLUSTER SETTING foo = 'bar';
	*/
	require.Len(t, batches, 3)
	assert.Equal(t, "SELECT * FROM test;", batches[0][0])
	assert.Equal(t, "INSERT INTO test(v) VALUES ('foo'), ('bar');", batches[1][0])
	assert.Equal(t, "SET CLUSTER SETTING foo = 'bar';", batches[2][0])
}

func TestParseBlocks(t *testing.T) {
	parser := NewSqlFileParser("testdata/blocks.sql")
	batches, err := parser.Parse()
	require.NoError(t, err)
	/*
	   SELECT * FROM test;
	   INSERT INTO test(v) VALUES ('foo'), ('bar');
	   SET CLUSTER SETTING foo = 'bar';
	*/
	require.Len(t, batches, 2)
	require.Len(t, batches[0], 2)
	require.Len(t, batches[1], 1)
	assert.Equal(t, "SELECT * FROM test;", batches[0][0])
	assert.Equal(t, "INSERT INTO test(v) VALUES ('foo'), ('bar');", batches[0][1])
	assert.Equal(t, "SET CLUSTER SETTING foo = 'bar';", batches[1][0])
}

func TestParseFunctions(t *testing.T) {
	parser := NewSqlFileParser("testdata/functions.sql")
	batches, err := parser.Parse()
	require.NoError(t, err)
	require.Len(t, batches, 2)
	require.Len(t, batches[0], 1)
	require.Len(t, batches[1], 1)

}
