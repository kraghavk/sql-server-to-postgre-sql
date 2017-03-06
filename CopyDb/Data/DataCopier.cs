using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using CopyDb.MetaData;
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

        public void CopyTable()
        {
            var i = 0;
            while (true)
            {
                var data = GetDataChunk(i);
                if (data.Rows.Count > 0)
                {
                    ExportData(data);
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
            var columns = String.Join(",", _table.Columns);
            var pkColumns = String.Join(",", _table.PrimaryKey.Columns);

            var query = $@"
                SELECT {columns}
                FROM {_table.Name}
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

        private void ExportData(DataTable data)
        {
            using (var con = new NpgsqlConnection(_pgConStr))
            using (var cmd = con.CreateCommand())
            {
                con.Open();
                var columns = String.Join(",", _table.Columns.Select(c => $"\"{c.Name}\""));
                cmd.CommandText = $"COPY \"{_table.Name}\"({columns}) FROM STDIN;";

                var serializer = new NpgsqlCopySerializer(con);
                var copyIn = new NpgsqlCopyIn(cmd, con, serializer.ToStream);

                copyIn.Start();
                foreach (DataRow row in data.Rows)
                {
                    foreach (var column in _table.Columns)
                    {
                        PgSerializer.SerializeColumn(column, row[column.Name], serializer);
                    }
                    serializer.EndRow();
                    serializer.Flush();
                }

                copyIn.End();
                serializer.Close();
                // copyIn.Cancel("Undo copy on exception."); //TODO: cancell the insertion on exception!
            }
        }
    }
}
