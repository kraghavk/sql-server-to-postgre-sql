using System.Collections.Generic;
using System.Data;

using Microsoft.Data.SqlClient;

namespace CopyDb.MetaData
{
    public class ForeignKey
    {
        public string Name { get; set; }
        public string Table { get; set; }
        public string Column { get; set; }
        public string ReferencedTable { get; set; }
        public string ReferencedColumn { get; set; }
        public string UpdateRule { get; set; }
        public string DeleteRule { get; set; }

        public override string ToString() => $"{Name} : {Column} -> {ReferencedTable}.{ReferencedColumn}";

        public ForeignKey(IDataReader reader)
        {
            Name = (string)reader["FK_NAME"];
            Table = (string)reader["table"];
            Column = (string)reader["column"];
            ReferencedTable = (string)reader["referenced_table"];
            ReferencedColumn = (string)reader["referenced_column"];
            UpdateRule = (string)reader["on_update"];
            DeleteRule = (string)reader["on_delete"];
        }

        public string Render()
        {
            return
$@"ALTER TABLE ONLY ""{Table}""
    ADD CONSTRAINT ""{Name}""
	FOREIGN KEY (""{Column}"") REFERENCES ""{ReferencedTable}""(""{ReferencedColumn}"")
	ON UPDATE {UpdateRule}
	ON DELETE {DeleteRule};";
        }

        public static List<ForeignKey> GetForeignKeys(string schema, string conStr)
        {
            using (var con = new SqlConnection(conStr))
            using (var cmd = new SqlCommand(FkCmdText, con))
            {
                cmd.Parameters.Add(new SqlParameter("@schema", SqlDbType.VarChar, 255) { Value = schema });

                con.Open();
                var fks = new List<ForeignKey>();
                var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    fks.Add(new ForeignKey(reader));
                }
                return fks;
            }
        }

        private const string FkCmdText = @"
SELECT  obj.name AS FK_NAME,    
    tab1.name AS [table],
    col1.name AS [column],
    tab2.name AS [referenced_table],
    col2.name AS [referenced_column],
  i.UPDATE_RULE as [on_update],
  i.DELETE_RULE as [on_delete]
FROM sys.foreign_key_columns fkc
INNER JOIN sys.objects obj
    ON obj.object_id = fkc.constraint_object_id
INNER JOIN sys.tables tab1
    ON tab1.object_id = fkc.parent_object_id
INNER JOIN sys.schemas sch
    ON tab1.schema_id = sch.schema_id
INNER JOIN sys.columns col1
    ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id
INNER JOIN sys.tables tab2
    ON tab2.object_id = fkc.referenced_object_id
INNER JOIN sys.columns col2
    ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id
INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS i ON obj.name = i.CONSTRAINT_NAME
where sch.name = @schema
";
    }
}