using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace CopyDb.MetaData
{
    public class Table
    {
        public string Name { get; set; }
        public List<Column> Columns { get; set; }
        public List<ForeignKey> ForeignKeys { get; set; }
        public PrimaryKey PrimaryKey { get; set; }
        public List<Identity> Identities { get; set; }
        public List<Index> Indices { get; set; }

        public override string ToString() => Name;

        public string Render()
        {
            var cols = String.Join(",\r\n", Columns.Select(c => c.Render()));
            return $"CREATE TABLE \"{Name}\" (\r\n{cols}\r\n);";
        }

        public static List<Table> GetTables(string catalog, string schema, string conStr)
        {
            var fks = ForeignKey.GetForeignKeys(schema, conStr);
            var pks = PrimaryKey.GetPrimaryKeys(catalog, schema, conStr);
            var ids = Identity.GetIds(conStr);
            var ixs = Index.GetIndeIndices(conStr);

            //don't include indices that equals to PK
            ixs = ixs.Where(x => x.Columns.Count > 1 
                              || !pks.Any(p => p.Table == x.Table 
                                            && p.Column == x.Columns.First()))
                     .ToList();

            using (var con = new SqlConnection(conStr))
            using (var cmd = new SqlCommand(ColumnsCmdText, con))
            {
                cmd.Parameters.Add(new SqlParameter("@catalog", SqlDbType.VarChar, 255) { Value = catalog });
                cmd.Parameters.Add(new SqlParameter("@schema", SqlDbType.VarChar, 255) { Value = schema });

                con.Open();
                var columns = new List<Column>();
                var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    columns.Add(new Column(reader));
                }
                return columns
                    .GroupBy(c => c.TableName)
                    .Select(s => new Table
                    {
                        Name = s.Key,
                        Columns = s.OrderBy(o => o.Position).ToList(),
                        ForeignKeys = fks.Where(x => x.Table == s.Key).ToList(),
                        PrimaryKey = pks.FirstOrDefault(p => p.Table == s.Key),
                        Identities = ids.Where(i => i.Table == s.Key).ToList(),
                        Indices = ixs.Where(i => i.Table == s.Key).ToList()
                    })
                    .ToList();
            }
        }

        private const string ColumnsCmdText = @"
select * from INFORMATION_SCHEMA.COLUMNS c
INNER JOIN INFORMATION_SCHEMA.TABLES t
ON c.TABLE_NAME = t.TABLE_NAME
AND c.TABLE_CATALOG = t.TABLE_CATALOG
AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
WHERE t.TABLE_TYPE = 'BASE TABLE'
AND t.TABLE_CATALOG = @catalog
AND t.TABLE_SCHEMA = @schema
ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION
";
    }
}