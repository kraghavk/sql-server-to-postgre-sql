using System;
using System.IO;
using System.Linq;
using CopyDb.MetaData;

namespace CopyDb
{
    public class Program
    {
        private const int Size = 1000000;

        private const string MsConStr = "Server=.;Database=ISCommerce;Trusted_Connection=True;";
        private const string PgConStr = "User ID=postgres;Password=1364791835Q!;Host=localhost;Port=5432;Database=ISCommerce;Pooling=true;";
        

        static void Main(string[] args)
        {            
            var tables = Table.GetTables("ISCommerce", "dbo", MsConStr);

            var text = String.Join("\r\n\r\n", tables.Select(s => s.Render()));

            //TODO: here we will insert the data and then create keys, indices etc

            text += "\r\n\r\n" + String.Join("\r\n\r\n", tables.SelectMany(x => x.Identities).Select(x => x.Render()));
            text += "\r\n\r\n" + String.Join("\r\n\r\n", tables.Select(x => x.PrimaryKey.Render()));
            text += "\r\n\r\n" + String.Join("\r\n\r\n", tables.SelectMany(x => x.Indices).Select(x => x.Render()));
            text += "\r\n\r\n" + String.Join("\r\n\r\n", tables.SelectMany(x => x.ForeignKeys).Select(x => x.Render()));

            File.WriteAllText("d:\\test.sql",text);
        }
    }
}
