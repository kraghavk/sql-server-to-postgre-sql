using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

using Microsoft.Data.SqlClient;

namespace CopyDb.MetaData
{
    public class PrimaryKey
    {
        public string Name { get; set; }
        public string Table { get; set; }
        public List<string> Columns { get; set; }

        private readonly string _column;

        public PrimaryKey(IDataReader reader)
        {
            Name = (string)reader["CONSTRAINT_NAME"];
            Table = (string)reader["TABLE_NAME"];
            _column = (string)reader["COLUMN_NAME"];
        }

        private PrimaryKey() { }

        public override string ToString() => $"{Name} ({_column})";

        public string Render()
        {
            var columns = String.Join(",", Columns.Select(c => $"\"{c}\""));

            return
$@"ALTER TABLE ONLY ""{Table}""
    ADD CONSTRAINT ""{Name}"" PRIMARY KEY ({columns});";
        }

        public static List<PrimaryKey> GetPrimaryKeys(string catalog, string schema, string conStr)
        {
            using (var con = new SqlConnection(conStr))
            using (var cmd = new SqlCommand(PkCmdText, con))
            {
                cmd.Parameters.Add(new SqlParameter("@catalog", SqlDbType.VarChar, 255) { Value = catalog });
                cmd.Parameters.Add(new SqlParameter("@schema", SqlDbType.VarChar, 255) { Value = schema });

                con.Open();
                var pks = new List<PrimaryKey>();
                var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    pks.Add(new PrimaryKey(reader));
                }

                return pks
                    .GroupBy(g => g.Name)
                    .Select(x => new PrimaryKey
                    {
                        Name = x.FirstOrDefault()?.Name,
                        Table = x.FirstOrDefault()?.Table,
                        Columns = x
                            .Select(c => c._column)
                            .ToList()
                    })
                    .ToList();
            }
        }

        private const string PkCmdText = @"
SELECT
    Tab.CONSTRAINT_NAME,
    Tab.TABLE_NAME,
    Col.COLUMN_NAME
from
    INFORMATION_SCHEMA.TABLE_CONSTRAINTS Tab
INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Col
    ON Col.Constraint_Name = Tab.Constraint_Name
WHERE    
    Col.Table_Name = Tab.Table_Name
    AND Constraint_Type = 'PRIMARY KEY'
    AND Tab.Constraint_CATALOG = @catalog
    AND Tab.CONSTRAINT_SCHEMA = @schema
";
    }
}