using System;
using System.Collections.Generic;
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
        private const int Size = 100_000;

        private const string MsConStr = "Server=.;Database=ISCommerce2;Trusted_Connection=True;MultipleActiveResultSets=True";
        private const string PgConStr = "User ID=postgres;Password=1364791835Q!;Host=localhost;Port=5432;Database=test;Pooling=false;";

        static void Main(string[] args)
        {
            //todo: 1 create concurrent dictionary (table -> progress)
            //todo: 2 update the dictionary during export
            //todo: 3. show progress in parallel task during export

            //>>>> emulation
            var ttt = Table.GetTables("ISCommerce2", "dbo", MsConStr);
            var data = Enumerable.Range(1, 10).Select(p => ttt.ToDictionary(k => k.Name, v => p/(double)10)).ToList();

            Console.WriteLine("Press any key to start");
            Console.ReadKey();
            Console.Clear();

            foreach (var step in data)
            {
                Print(step, ConsoleColor.DarkGreen);
                Thread.Sleep(200);
            }
            return;
            //<< emulation

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
            text = String.Join("\r\n\r\n", tables.SelectMany(x => x.Identities).Select(x => x.Render()));
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
            using (var cmd = new NpgsqlCommand(cmdText, con))
            {
                con.Open();
                cmd.ExecuteNonQuery();
                con.Close();
            }
        }

        private static void Print(Dictionary<string, double> progress, ConsoleColor color)
        {
            Console.SetCursorPosition(0,0);
            int max = Console.WindowWidth;
            int position = 0;
            foreach (var table in progress)
            {
                if (position + table.Key.Length + 2 >= max)
                {
                    Console.WriteLine();
                    Write(table.Key, table.Value, color);
                    position = table.Key.Length + 2;
                }
                else
                {
                    Write(table.Key, table.Value, color);
                    position += table.Key.Length + 2;
                }
            }
        }

        private static void Write(string text, double progress, ConsoleColor color)
        {
            text = $"[{text}]";
            var cur = (int)Math.Round(progress * text.Length);
            var empty = Console.BackgroundColor;
            for (int i = 0; i < text.Length; i++)
            {
                Console.BackgroundColor = cur > i ? color : empty;
                Console.Write(text[i]);
            }
            Console.BackgroundColor = empty;
        }
    }
}
