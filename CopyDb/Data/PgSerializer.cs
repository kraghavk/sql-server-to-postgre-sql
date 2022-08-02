using System;
using System.Text.RegularExpressions;

using CopyDb.MetaData;

using Npgsql;

using NpgsqlTypes;

namespace CopyDb.Data
{
    public class PgSerializer
    {
        public static void SerializeColumn(Column c, object value, NpgsqlBinaryImporter importer)
        {
            if (value == DBNull.Value)
            {
                importer.WriteNull();
            }
            else
            {
                var type = GetPgType(c.Type, c.IsMax);
                var s = value as string;
                if (s == null)
                {
                    importer.Write(value, type);
                }
                else
                {
                    s = Regex.Replace(s, "\0", ""); //null characters are not allowed in Postgres
                    importer.Write(s, type);
                }
            }
        }

        private static NpgsqlDbType GetPgType(MsType src, bool isMax)
        {
            switch (src)
            {
                case MsType.BIGINT: return NpgsqlDbType.Bigint;
                case MsType.BINARY: return NpgsqlDbType.Bytea;
                case MsType.BIT: return NpgsqlDbType.Boolean;
                case MsType.CHAR: return NpgsqlDbType.Char;
                case MsType.CHARACTER: return NpgsqlDbType.Char;
                case MsType.DATE: return NpgsqlDbType.Date;
                case MsType.DATETIME: return NpgsqlDbType.Timestamp;
                case MsType.DATETIME2: return NpgsqlDbType.Timestamp;
                case MsType.DATETIMEOFFSET: return NpgsqlDbType.TimestampTz;
                case MsType.DECIMAL: return NpgsqlDbType.Numeric;
                case MsType.DEC: return NpgsqlDbType.Numeric;
                case MsType.DOUBLE_PRECISION: return NpgsqlDbType.Double;
                case MsType.FLOAT: return NpgsqlDbType.Double;
                case MsType.GEOMETRY: return NpgsqlDbType.Geometry;
                case MsType.IMAGE: return NpgsqlDbType.Bytea;
                case MsType.INT: return NpgsqlDbType.Integer;
                case MsType.INTEGER: return NpgsqlDbType.Integer;
                case MsType.MONEY: return NpgsqlDbType.Numeric;
                case MsType.NCHAR: return NpgsqlDbType.Char;
                case MsType.NTEXT: return NpgsqlDbType.Text;
                case MsType.NUMERIC: return NpgsqlDbType.Numeric;
                case MsType.NVARCHAR: return isMax ? NpgsqlDbType.Text : NpgsqlDbType.Varchar;
                case MsType.REAL: return NpgsqlDbType.Real;
                case MsType.ROWVERSION: return NpgsqlDbType.Bytea;
                case MsType.SMALLDATETIME: return NpgsqlDbType.Timestamp;
                case MsType.SMALLINT: return NpgsqlDbType.Smallint;
                case MsType.SMALLMONEY: return NpgsqlDbType.Money;
                case MsType.TEXT: return NpgsqlDbType.Text;
                case MsType.TIME: return NpgsqlDbType.Time;
                case MsType.TIMESTAMP: return NpgsqlDbType.Bytea;
                case MsType.TINYINT: return NpgsqlDbType.Smallint;
                case MsType.UNIQUEIDENTIFIER: return NpgsqlDbType.Uuid;
                case MsType.VARBINARY: return NpgsqlDbType.Bytea;
                case MsType.VARCHAR: return isMax ? NpgsqlDbType.Text : NpgsqlDbType.Varchar;
                case MsType.XML: return NpgsqlDbType.Xml;
                default: throw new ArgumentOutOfRangeException(nameof(src), src, null);
            }
        }
    }
}
