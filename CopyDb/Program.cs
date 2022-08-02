using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Configuration;

namespace CopyDb
{
    public class Program
    {
        private static readonly Stopwatch Sw = new Stopwatch();

        public static void Main()
        {
            Sw.Start();
            var token = new CancellationTokenSource();
            UpdateTitle(token.Token);

            var worker = new Worker();
            worker.GetSourceMetaData();
            worker.CreateTables();
            worker.DetermineMaximumRows();
            worker.CopyData();
            worker.CreateSeqPkIxFk();
            worker.VerifyData();

            Sw.Stop();
            token.Cancel();
            Console.WriteLine("\nThe conversion is complete. The total elapsed time is: {0:g}", Sw.Elapsed);
        }

        private static void UpdateTitle(CancellationToken token)
        {
            Task.Factory.StartNew(() =>
            {
                while (!token.IsCancellationRequested)
                {
                    Console.Title = $"Elapsed: {Sw.Elapsed:g}";
                    Thread.Sleep(100);
                }
            }, token);
        }
    }
}
