using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CopyDb.Data;
using CopyDb.MetaData;
using Npgsql;

namespace CopyDb
{
    public class Program
    {
        private static readonly string MsConStr = ConfigurationManager.AppSettings["MsConStr"];
        private static readonly string PgConStr = ConfigurationManager.AppSettings["PgConStr"];
        private static readonly string MsDbName = ConfigurationManager.AppSettings["MsDbName"];
        private static readonly string MsSchema = ConfigurationManager.AppSettings["MsSchema"];
        private static readonly int ChunkSize = Int32.Parse(ConfigurationManager.AppSettings["ChunkSize"]);
        private static readonly int Fps = Int32.Parse(ConfigurationManager.AppSettings["Fps"]);
        
        private static void Main()
        {
            var sw = new Stopwatch();
            sw.Start();

            Console.WriteLine("Getting the source schema");
            var tables = Table.GetTables(MsDbName, MsSchema, MsConStr);

            Console.WriteLine("Creating the tables");
            var text = String.Join("\r\n\r\n", tables.Select(s => s.Render()));
            ExecuteCommand(text);

            //progress visualization
            var progress = 0;
            var max = GetMaxProgress();
            var token = new CancellationTokenSource();
            Task.Factory.StartNew(() =>
            {
                while (!token.IsCancellationRequested)
                {
                    var percent = progress / (double)max;
                    Write($"Copying the data, progress: {percent:P}", percent, ConsoleColor.DarkGreen);
                    Task.Delay(1000 / Fps, token.Token).Wait(token.Token);
                }
            }, token.Token);

            //copy data
            var empty = Console.BackgroundColor;
            Parallel.ForEach(tables, table =>
            {
                var copier = new DataCopier(table, ChunkSize, MsConStr, PgConStr);
                copier.CopyTable(ref progress);
            });
            token.Cancel();
            Console.BackgroundColor = empty;
            Console.Write("\n");

            //create: seq, pk, ix, fk
            Console.WriteLine("Creating the Seq, PK, IX, FK");
            text = String.Join("\r\n\r\n", tables.SelectMany(x => x.Identities).Select(x => x.Render()));
            text += "\r\n\r\n" + String.Join("\r\n\r\n", tables.Select(x => x.PrimaryKey.Render()));
            text += "\r\n\r\n" + String.Join("\r\n\r\n", tables.SelectMany(x => x.Indices).Select(x => x.Render()));
            text += "\r\n\r\n" + String.Join("\r\n\r\n", tables.SelectMany(x => x.ForeignKeys).Select(x => x.Render()));
            ExecuteCommand(text);

            sw.Stop();
            Console.WriteLine("The conversion is complete. The total elapsed time is: {0:g}", sw.Elapsed);
        }

        private static void ExecuteCommand(string cmdText)
        {
            using (var con = new NpgsqlConnection(PgConStr))
            using (var cmd = new NpgsqlCommand(cmdText, con))
            {
                con.Open();
                cmd.ExecuteNonQuery();
                con.Close();
            }
        }
        private static int GetMaxProgress()
        {
            const string query = @"
                SELECT Sum(I.rows)
                FROM   sys.tables t
                INNER JOIN sys.sysindexes i
                ON t.object_id = i.id AND I.indid < 2";
            using (var con = new SqlConnection(MsConStr))
            using (var cmd = new SqlCommand(query, con))
            {
                con.Open();
                return (int)cmd.ExecuteScalar();
            }
        }

        private static void Write(string text, double progress, ConsoleColor color)
        {
            var empty = Console.BackgroundColor;
            Console.SetCursorPosition(0, 2);
            var max = Console.WindowWidth;
            var textPosition = (int)Math.Round((max - text.Length) / (double)2);
            var fill = (int)Math.Round(progress * max);

            var margin = String.Join("",Enumerable.Range(0, textPosition).Select(_ => " "));
            var buffer = margin + text + margin;

            var filled = buffer.Substring(0, fill);
            Console.BackgroundColor = color;
            Console.Write(filled);
            var unfilled = buffer.Substring(fill + 1);
            Console.BackgroundColor = empty;
            Console.Write(unfilled);
        }
    }
}
