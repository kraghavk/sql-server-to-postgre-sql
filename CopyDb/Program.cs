using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
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
            var colTypes = tables.SelectMany(x => x.Columns).Select(x => x.DataType).Distinct().ToList();
            
        }
    }
}
