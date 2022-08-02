using System;
using System.Data;
using System.Linq;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using System.Threading;

using CopyDb.MetaData;

using Microsoft.Data.SqlClient;

using Npgsql;

namespace CopyDb.Data
{
    public class DataVerifier
    {
        private readonly Table _table;
        private readonly string _msConStr;
        private readonly string _pgConStr;
        private readonly int _chunkSize;

        public DataVerifier(Table table, int chunkSize, string msConStr, string pgConStr)
        {
            _table = table;
            _msConStr = msConStr;
            _pgConStr = pgConStr;
            _chunkSize = chunkSize;
        }

        public void VerifyTable(ref int progress)
        {
            var i = 0;
            while (true)
            {
                var msData = GetMsChunk(i);
                var pgData = GetPgChunk(i);

                if (CompareDataTables(msData, pgData, ref progress) > 0)
                {
                    i++;
                }
                else
                {
                    break;
                }
            }
        }

        private IDataReader GetMsChunk(int page)
        {
            var columns = String.Join(",", _table.Columns.Select(x => $"[{x.Name}]"));
            var pkColumns = String.Join(",", _table.PrimaryKey.Columns.Select(x => $"[{x}]"));

            var query = $@"
                SELECT {columns}
                FROM [{_table.Name}]
                ORDER BY {pkColumns}
                OFFSET {page * _chunkSize} ROWS
                FETCH NEXT {_chunkSize} ROWS ONLY";

            var con = new SqlConnection(_msConStr);
            var cmd = new SqlCommand(query, con);

            con.Open();
            return cmd.ExecuteReader();

        }

        private IDataReader GetPgChunk(int page)
        {
            var columns = String.Join(",", _table.Columns.Select(x => $"\"{x.Name}\""));
            var pkColumns = String.Join(",", _table.PrimaryKey.Columns.Select(x => $"\"{x}\""));

            var query = $@"
                SELECT {columns}
                FROM ""{_table.Name}""
                ORDER BY {pkColumns}
                OFFSET {page * _chunkSize} ROWS
                FETCH NEXT {_chunkSize} ROWS ONLY";

            var con = new NpgsqlConnection(_pgConStr);
            var cmd = new NpgsqlCommand(query, con);

            con.Open();
            return cmd.ExecuteReader();

        }

        private int CompareDataTables(IDataReader ms, IDataReader pg, ref int progress)
        {
            int i = 0;
            while (ms.Read() && pg.Read())
            {
                foreach (var column in _table.Columns)
                {
                    var str = ms[column.Name] as string;
                    if (str == null)
                    {
                        var byteArray = ms[column.Name] as byte[];
                        if (byteArray != null)
                        {
                            var byteArrayPg = pg[column.Name] as byte[];
                            if (byteArrayPg == null)
                                Throw(column, ms, pg);

                            using (var md5 = MD5.Create())
                            {
                                var msHash = new Guid(md5.ComputeHash(byteArray));
                                var pgHash = new Guid(md5.ComputeHash(byteArrayPg));
                                if (!msHash.Equals(pgHash))
                                    Throw(column, ms, pg);
                            }
                        }
                        else //not byte array
                        {
                            if (!ms[column.Name].Equals(pg[column.Name]))
                                Throw(column, ms, pg);
                        }
                    }
                    else //string
                    {
                        var validStr = Regex.Replace(str, "\0", "");
                        if (!validStr.Equals(pg[column.Name]))
                            Throw(column, ms, pg);
                    }
                }
                Interlocked.Increment(ref progress);
                i++;
            }
            ms.Close();
            pg.Close();
            return i;
        }

        private void Throw(Column c, IDataReader ms, IDataReader pg)
        {
            throw new Exception($"The column data is different.\ntable:{_table.Name}\ncolumn:{c.Name}({c.DataType})\nMS:{ms[c.Name]}\nPG:{pg[c.Name]}");
        }
    }
}
