using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace CopyDb.MetaData
{
    public class Identity
    {
        public string Table { get; set; }
        public string Column { get; set; }
        public long? Seed { get; set; }
        public long? Increment { get; set; }
        public long? LastValue { get; set; }

        public Identity(IDataReader reader)
        {
            Table = (string) reader["table"];
            Column = (string) reader["column"];
            Seed = reader["seed_value"] == DBNull.Value ? null : (long?)Int64.Parse( reader["seed_value"].ToString()); //the data type is variant
            Increment = reader["increment_value"] == DBNull.Value ? null : (long?)Int64.Parse( reader["increment_value"].ToString());
            LastValue = reader["last_value"] == DBNull.Value ? null : (long?)Int64.Parse(reader["last_value"].ToString());
        }

        public override string ToString() => $"{Column} ({Seed??0},{Increment??1})";

        public static List<Identity> GetIds(string conStr)
        {
            using (var con = new SqlConnection(conStr))
            using (var cmd = new SqlCommand(IdentityCmdText, con))
            {
                con.Open();
                var ids = new List<Identity>();
                var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    ids.Add(new Identity(reader));
                }
                return ids;
            }
        }

        private const string IdentityCmdText = @"
select
	o.name [table],
	id.name [column],
	seed_value,
	increment_value,
	last_value
from sys.identity_columns id
inner join sys.objects o
on id.object_id = o.object_id
";
    }
}
