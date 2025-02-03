using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleSqlRunner
{
    public class SqlRunner
    {
        private readonly string ConnectionString;

        public SqlRunner(string connectionString) => ConnectionString = connectionString;

        public async Task<ResultSets> RunSqlAsync(string sql, SqlParameter[] parameters = null, bool isSproc = false, CancellationToken? cancellationToken = null)
        {
            using (var connection = new SqlConnection(ConnectionString))
            using (var command = connection.CreateCommand())
            {
                command.CommandText = sql;

                if (isSproc) 
                    command.CommandType = System.Data.CommandType.StoredProcedure;

                if (parameters != null)
                    command.Parameters.AddRange(parameters);

                await connection.OpenAsync(cancellationToken ?? CancellationToken.None);

                var reader = await command.ExecuteReaderAsync(cancellationToken ?? CancellationToken.None);

                var resultSets = new ResultSets();

                do
                {
                    var resultSet = new ResultSet();
                    resultSets.Add(resultSet);
                    var fieldNames = new string[reader.FieldCount];

                    if (!reader.HasRows) continue;

                    for (int i = 0; i < fieldNames.Length; i++)
                    {
                        fieldNames[i] = reader.GetName(i);
                    }

                    while (await reader.ReadAsync(cancellationToken ?? CancellationToken.None))
                    {
                        var row = new Row();

                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            row[fieldNames[i]] = reader[i];
                        }

                        resultSet.Add(row);
                    }

                } while (reader.NextResult());

                return resultSets;
            }
        }
    }

    public class ResultSets : List<ResultSet> 
    {
        public object GetScalar() => this.FirstOrDefault().GetScalar();
    }

    public class ResultSet : List<Row> 
    {
        public object GetScalar() => this.FirstOrDefault()?.GetScalar();
    }

    public class Row : Dictionary<string, object> 
    {
        public object GetScalar() => this.FirstOrDefault().Value;
    }
}
