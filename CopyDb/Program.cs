using System;
using System.IO;
using System.Linq;
using CopyDb.Data;
using CopyDb.MetaData;
using Npgsql;

namespace CopyDb
{
    public class Program
    {
        private const int Size = 100_000;

        private const string MsConStr = "Server=.;Database=ISCommerce;Trusted_Connection=True;";
        private const string PgConStr = "User ID=postgres;Password=1364791835Q!;Host=localhost;Port=5432;Database=test;Pooling=false;";
        
        static void Main(string[] args)
        {            
            var tables = Table.GetTables("ISCommerce", "dbo", MsConStr);

            /*
            var text = String.Join("\r\n\r\n", tables.Select(s => s.Render()));

            //TODO: here we will insert the data

            text += "\r\n\r\n" + String.Join("\r\n\r\n", tables.SelectMany(x => x.Identities).Select(x => x.Render()));
            text += "\r\n\r\n" + String.Join("\r\n\r\n", tables.Select(x => x.PrimaryKey.Render()));
            text += "\r\n\r\n" + String.Join("\r\n\r\n", tables.SelectMany(x => x.Indices).Select(x => x.Render()));
            text += "\r\n\r\n" + String.Join("\r\n\r\n", tables.SelectMany(x => x.ForeignKeys).Select(x => x.Render()));

            File.WriteAllText("d:\\test.sql",text);
            ExecuteCommand(text);
            */
            ProcessSingleTable(tables.First(x => x.Name == "ChatMessage"));
        }

        private static void ProcessSingleTable(Table table)
        {
            ExecuteCommand(table.Render());
            var copier = new DataCopier(table, Size, MsConStr, PgConStr);
            copier.CopyTable();

            var text = String.Join("\r\n\r\n", table.Identities.Select(x => x.Render()));
            text += "\r\n\r\n" + table.PrimaryKey.Render();
            text += "\r\n\r\n" + String.Join("\r\n\r\n", table.Indices.Select(x => x.Render()));
            ExecuteCommand(text);
        }

        private static void ExecuteCommand(string cmdText)
        {
            using (var con = new NpgsqlConnection(PgConStr))
            using (var cmd = new NpgsqlCommand(cmdText,con))
            {
                con.Open();
                cmd.ExecuteNonQuery();
                con.Close();
            }
        }
    }
}
