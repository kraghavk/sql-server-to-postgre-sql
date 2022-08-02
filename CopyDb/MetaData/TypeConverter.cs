using System;

namespace CopyDb.MetaData
{
    public class TypeConverter
    {
        public static string GetPostgresType(Column c)
        {
            switch (c.Type)
            {
                case MsType.BIGINT:
                    return PgType.BIGINT.ToString();

                case MsType.BINARY:
                    return PgType.BYTEA.ToString();

                case MsType.BIT:
                    return PgType.BOOLEAN.ToString();

                case MsType.CHAR:
                    return $"{PgType.CHAR}({c.MaxLenth})";

                case MsType.CHARACTER:
                    return $"{PgType.CHARACTER}({c.MaxLenth})";

                case MsType.DATE:
                    return PgType.DATE.ToString();

                case MsType.DATETIME:
                    return PgType.TIMESTAMP.ToString();

                case MsType.DATETIME2:
                    return PgType.TIMESTAMP.ToString();

                case MsType.DATETIMEOFFSET:
                    return $"TIMESTAMP({c.DateTymePrecision}) WITH TIME ZONE";

                case MsType.DECIMAL:
                    return $"{PgType.DECIMAL}({c.NumericPrecision},{c.NumericScale})";

                case MsType.DEC:
                    return $"{PgType.DEC}({c.NumericPrecision},{c.NumericScale})";

                case MsType.DOUBLE_PRECISION:
                    return "DOUBLE PRECISION";

                case MsType.FLOAT:
                    return "DOUBLE PRECISION";

                case MsType.GEOMETRY:
                    return "geometry(geometry,4326)";

                case MsType.IMAGE:
                    return PgType.BYTEA.ToString();

                case MsType.INT:
                    return PgType.INT.ToString();

                case MsType.INTEGER:
                    return PgType.INTEGER.ToString();

                case MsType.MONEY:
                    return $"{PgType.NUMERIC}(19,4)"; //return PgType.MONEY.ToString();

                case MsType.NCHAR:
                    return $"{PgType.CHAR}({c.MaxLenth})";

                case MsType.NTEXT:
                    return PgType.TEXT.ToString();

                case MsType.NUMERIC:
                    return $"{PgType.NUMERIC}({c.NumericPrecision},{c.NumericScale})";

                case MsType.NVARCHAR:
                    return c.IsMax
                        ? PgType.TEXT.ToString()
                        : $"{PgType.VARCHAR} ({c.MaxLenth})";

                case MsType.REAL:
                    return PgType.REAL.ToString();

                case MsType.ROWVERSION:
                    return PgType.BYTEA.ToString();

                case MsType.SMALLDATETIME:
                    return $"{PgType.TIMESTAMP}(0)";

                case MsType.SMALLINT:
                    return PgType.SMALLINT.ToString();

                case MsType.SMALLMONEY:
                    return PgType.MONEY.ToString();

                case MsType.TEXT:
                    return PgType.TEXT.ToString();

                case MsType.TIME:
                    return $"{PgType.TIME}({c.DateTymePrecision})";

                case MsType.TIMESTAMP:
                    return PgType.BYTEA.ToString();

                case MsType.TINYINT:
                    return PgType.SMALLINT.ToString();

                case MsType.UNIQUEIDENTIFIER:
                    return PgType.UUID.ToString();

                case MsType.VARBINARY:
                    return PgType.BYTEA.ToString();

                case MsType.VARCHAR:
                    return c.IsMax
                        ? PgType.TEXT.ToString()
                        : $"{PgType.VARCHAR} ({c.MaxLenth})";

                case MsType.XML:
                    return PgType.XML.ToString();

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}