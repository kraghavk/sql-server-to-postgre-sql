using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CopyDb.Data;
using CopyDb.MetaData;
using Npgsql;

namespace CopyDb
{
    public class Worker
    {
        private  readonly string _msConStr = ConfigurationManager.AppSettings["MsConStr"];
        private  readonly string _pgConStr = ConfigurationManager.AppSettings["PgConStr"];
        private  readonly string _msDbName = ConfigurationManager.AppSettings["MsDbName"];
        private  readonly string _msSchema = ConfigurationManager.AppSettings["MsSchema"];
        private  readonly int _chunkSize = Int32.Parse(ConfigurationManager.AppSettings["ChunkSize"]);
        private  readonly int _fps = Int32.Parse(ConfigurationManager.AppSettings["Fps"]);

        private List<Table> _tables;
        private long _max;
        private int _progress;

        public void GetSourceMetaData()
        {
            WrapMethod("Getting the source schema", () =>
            {
                _tables = Table.GetTables(_msDbName, _msSchema, _msConStr);
            });
        }

        public void CreateTables()
        {
            WrapMethod("Creating the tables", () =>
            {
                var text = String.Join("\r\n\r\n", _tables.Select(s => s.Render()));
                ExecuteCommand(text);
            });
        }

        public void DetermineMaximumRows()
        {
            _max = GetMaxProgress();
        }

        public void CopyData()
        {
            var sw = new Stopwatch();
            sw.Start();
            var token = new CancellationTokenSource();
            RenderProgressBar(token.Token, "Copying the data", 2);

            //copy data
            var empty = Console.BackgroundColor;
            Parallel.ForEach(_tables, table =>
            {
                var copier = new DataCopier(table, _chunkSize, _msConStr, _pgConStr);
                copier.CopyTable(ref _progress);
            });
            token.Cancel();
            sw.Stop();
            Console.BackgroundColor = empty;
            Console.Write($"({sw.Elapsed:g})\n");
        }

        public void CreateSeqPkIxFk()
        {
            WrapMethod("Creating Seq, PK, IX, FK", () =>
            {
                var text = String.Join("\r\n\r\n", _tables.SelectMany(x => x.Identities).Select(x => x.Render()));
                text += "\r\n\r\n" + String.Join("\r\n\r\n", _tables.Select(x => x.PrimaryKey.Render()));
                text += "\r\n\r\n" + String.Join("\r\n\r\n", _tables.SelectMany(x => x.Indices).Select(x => x.Render()));
                text += "\r\n\r\n" + String.Join("\r\n\r\n", _tables.SelectMany(x => x.ForeignKeys).Select(x => x.Render()));
                ExecuteCommand(text);
            });
        }

        public void VerifyData()
        {
            _progress = 0;
            var sw = new Stopwatch();
            sw.Start();
            var token = new CancellationTokenSource();
            RenderProgressBar(token.Token, "Verifying the data", 5);
            
            Exception ex = null;
            var empty = Console.BackgroundColor;
            Parallel.ForEach(_tables, (table, state) =>
            {
                try
                {
                    var verifier = new DataVerifier(table, _chunkSize, _msConStr, _pgConStr);
                    verifier.VerifyTable(ref _progress);
                }
                catch (Exception e)
                {
                    ex = e;
                    state.Break();
                }
            });
            token.Cancel();
            sw.Stop();
            Console.BackgroundColor = empty;
            Console.Write($"({sw.Elapsed:g})\n");

            if (ex != null)
            {
                var color = Console.ForegroundColor;
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Verification failed.\n" + ex.Message);
                Console.ForegroundColor = color;
            }
        }

        //Private methods

        private void RenderProgressBar(CancellationToken token, string phaseName, int lineNo)
        {
            Task.Factory.StartNew(() =>
            {
                while (!token.IsCancellationRequested)
                {
                    var percent = _progress / (double)_max;
                    Write($"{phaseName}, progress: {percent:P}", percent, ConsoleColor.DarkGreen, lineNo);
                    Task.Delay(1000 / _fps, token).Wait(token);
                }
            }, token);
        }


        private void WrapMethod(string message, Action action)
        {
            var sw = new Stopwatch();
            sw.Start();
            Console.Write(message);
            action();
            sw.Stop();
            Console.WriteLine($" ({sw.Elapsed:g})");
        }

        private void ExecuteCommand(string cmdText)
        {
            using (var con = new NpgsqlConnection(_pgConStr))
            using (var cmd = new NpgsqlCommand(cmdText, con))
            {
                con.Open();
                cmd.CommandTimeout = 0; //PK, FK, IX creation might take a while
                cmd.ExecuteNonQuery();
                con.Close();
            }
        }
        private int GetMaxProgress()
        {
            const string query = @"
                SELECT Sum(I.rows)
                FROM   sys.tables t
                INNER JOIN sys.sysindexes i
                ON t.object_id = i.id AND I.indid < 2";
            using (var con = new SqlConnection(_msConStr))
            using (var cmd = new SqlCommand(query, con))
            {
                con.Open();
                return (int)cmd.ExecuteScalar();
            }
        }

        private static void Write(string text, double progress, ConsoleColor color, int row)
        {
            var empty = Console.BackgroundColor;
            Console.SetCursorPosition(0, row);
            var max = Console.WindowWidth;
            var textPosition = (int)Math.Ceiling((max - text.Length) / (double)2);
            var fill = (int)Math.Ceiling(progress * max);

            var margin = String.Join("", Enumerable.Range(0, textPosition).Select(_ => " "));
            var buffer = margin + text + margin;

            var filled = buffer.Substring(0, fill);
            Console.BackgroundColor = color;
            Console.Write(filled);
            var unfilled = buffer.Substring(fill);
            Console.BackgroundColor = empty;
            Console.Write(unfilled);
        }
    }
}
