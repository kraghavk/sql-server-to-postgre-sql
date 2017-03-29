using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using CopyDb.Data;
using CopyDb.MetaData;
using Npgsql;

namespace CopyDb
{
    public class Program
    {
        private const int Size = 100_000;

        private const string MsConStr = "Server=.;Database=ISCommerce2;Trusted_Connection=True;MultipleActiveResultSets=True";
        private const string PgConStr = "User ID=postgres;Password=1364791835Q!;Host=localhost;Port=5432;Database=test;Pooling=false;";
        
        static void Main(string[] args)
        {
            //todo DB drop/ creation logic

            var sw = new Stopwatch();
            sw.Start();

            //get the source schema
            var tables = Table.GetTables("ISCommerce2", "dbo", MsConStr);

            //create tables
            var text = String.Join("\r\n\r\n", tables.Select(s => s.Render()));
            ExecuteCommand(text);

            //copy data
            Parallel.ForEach(tables, table =>
            {
                var copier = new DataCopier(table, Size, MsConStr, PgConStr);
                copier.CopyTable();
            });

            //create: seq, pk, ix, fk
            text =  String.Join("\r\n\r\n", tables.SelectMany(x => x.Identities).Select(x => x.Render()));
            text += "\r\n\r\n" + String.Join("\r\n\r\n", tables.Select(x => x.PrimaryKey.Render()));
            text += "\r\n\r\n" + String.Join("\r\n\r\n", tables.SelectMany(x => x.Indices).Select(x => x.Render()));
            text += "\r\n\r\n" + String.Join("\r\n\r\n", tables.SelectMany(x => x.ForeignKeys).Select(x => x.Render()));
            ExecuteCommand(text);

            sw.Stop();
            Console.WriteLine(sw.Elapsed);
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
