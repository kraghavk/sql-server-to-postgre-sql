using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

using Microsoft.Data.SqlClient;

namespace CopyDb.MetaData
{
    public class Index
    {
        public string Name { get; set; }
        public string Table { get; set; }
        public bool IsUnique { get; set; }
        public List<string> Columns { get; set; }

        private readonly string _columnName;
        private readonly int _columnId;

        private Index() { }

        public Index(IDataReader reader)
        {
            Name = (string)reader["IndexName"];
            Table = (string)reader["TableName"];
            IsUnique = (bool)reader["IsUnique"];

            _columnId = (int)reader["ColumnId"];
            _columnName = (string)reader["ColumnName"];
        }

        public string Render()
        {
            var columns = String.Join(",", Columns.Select(c => $"\"{c}\""));

            return IsUnique
                ? $"CREATE UNIQUE INDEX \"{Name}\" ON \"{Table}\" USING btree ({columns});"
                : $"CREATE INDEX \"{Name}\" ON \"{Table}\" USING btree ({columns});";
        }

        public static List<Index> GetIndeIndices(string conStr)
        {
            using (var con = new SqlConnection(conStr))
            using (var cmd = new SqlCommand(QueryText, con))
            {
                con.Open();
                var ixs = new List<Index>();
                var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    ixs.Add(new Index(reader));
                }

                return ixs
                    .GroupBy(x => x.Name)
                    .Select(x => new Index
                    {
                        Name = x.FirstOrDefault()?.Name,
                        Table = x.FirstOrDefault()?.Table,
                        IsUnique = x.FirstOrDefault()?.IsUnique ?? false,
                        Columns = x
                            .OrderBy(c => c._columnId)
                            .Select(c => c._columnName)
                            .ToList()
                    })
                    .ToList();
            }
        }

        public override string ToString() => $"{Name} ({String.Join(",", Columns)})";



        private const string QueryText = @"
SELECT
     TableName = t.name,
     IndexName = ind.name,
     IndexId = ind.index_id,
     ColumnId = ic.index_column_id,
     ColumnName = col.name,
     IsUnique = ind.is_Unique
FROM
     sys.indexes ind
INNER JOIN
     sys.index_columns ic ON  ind.object_id = ic.object_id and ind.index_id = ic.index_id
INNER JOIN
     sys.columns col ON ic.object_id = col.object_id and ic.column_id = col.column_id
INNER JOIN
     sys.tables t ON ind.object_id = t.object_id
WHERE
     ind.is_primary_key = 0  AND t.is_ms_shipped = 0 and ind.is_disabled = 0
     and ic.is_included_column = 0 --TODO!
ORDER BY
     t.name, ind.name, ind.index_id, ic.index_column_id;
";
    }
}
