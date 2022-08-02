using System;
using System.Data;
using System.Linq;
using System.Threading;

using CopyDb.MetaData;

using Microsoft.Data.SqlClient;

using Npgsql;

namespace CopyDb.Data
{
    public class DataCopier
    {
        private readonly Table _table;
        private readonly string _msConStr;
        private readonly string _pgConStr;
        private readonly int _chunkSize;


        public DataCopier(Table table, int chunkSize, string msConStr, string pgConStr)
        {
            _table = table;
            _msConStr = msConStr;
            _pgConStr = pgConStr;
            _chunkSize = chunkSize;
        }

        public void CopyTable(ref int progress)
        {
            var i = 0;
            while (true)
            {
                var data = GetDataChunk(i);
                if (data.Rows.Count > 0)
                {
                    ExportData(data, ref progress);
                    i++;
                }
                else
                {
                    break;
                }
            }
        }

        private DataTable GetDataChunk(int page)
        {
            var columns = String.Join(",", _table.Columns.Select(x =>
            {
                return x.Type != MsType.GEOMETRY ? $"[{x.Name}]" : $"[{x.Name}].STAsText() AS [{x.Name}]";
            }));

            var pkColumns = _table.PrimaryKey?.Columns != null ? String.Join(",", _table.PrimaryKey?.Columns?.Select(x => $"[{x}]")) : columns;

            var query = $@"
                SELECT {columns}
                FROM [{_table.Name}]
                ORDER BY {pkColumns}
                OFFSET {page * _chunkSize} ROWS
                FETCH NEXT {_chunkSize} ROWS ONLY";

            using (var con = new SqlConnection(_msConStr))
            using (var cmd = new SqlCommand(query, con))
            {
                con.Open();
                var reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                var dataTable = new DataTable(_table.Name);
                dataTable.Load(reader);
                for (var i = 0; i < _table.Columns.Count; i++)
                    dataTable.Columns[i].ColumnName = _table.Columns[i].Name;

                return dataTable;
            }
        }

        private void ExportData(DataTable data, ref int progress)
        {
            var columns = String.Join(",", _table.Columns.Select(c => $"\"{c.Name}\""));
            var cmdText = $"COPY \"{_table.Name}\"({columns}) FROM STDIN (FORMAT BINARY);";
            using (var con = new NpgsqlConnection(_pgConStr))
            {
                con.Open();
                using (var writer = con.BeginBinaryImport(cmdText))
                {
                    foreach (DataRow row in data.Rows)
                    {
                        writer.StartRow();
                        foreach (var column in _table.Columns)
                        {
                            PgSerializer.SerializeColumn(column, row[column.Name], writer);
                        }
                        Interlocked.Increment(ref progress);
                    }
                }
            }
        }
    }
}

