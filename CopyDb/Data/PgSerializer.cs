using System;
using System.Collections.Generic;
using CopyDb.MetaData;
using Npgsql;

namespace CopyDb.Data
{
    public class PgSerializer
    {
        public static void SerializeColumn(Column c, object value, NpgsqlCopySerializer serializer)
        {
            if (value == DBNull.Value)
            {
                serializer.AddNull();
            }
            else
            {
                Handlers[c.Type](serializer, value);
            }
        }

        private static readonly IDictionary<MsType, Action<NpgsqlCopySerializer, object>> Handlers =
            new Dictionary<MsType, Action<NpgsqlCopySerializer, object>>
        {
            { MsType.BIT,(s,o) => s.AddBool((bool)o) },  //bool
            { MsType.BIGINT,(s,o) => s.AddInt64((long)o) }, //long
            { MsType.CHAR,(s,o) => s.AddString((string)o) }, //string
            { MsType.CHARACTER,(s,o) => s.AddString((string)o) }, //string
            { MsType.DATE,(s,o) => s.AddDateTime((DateTime)o) }, //datetime
            { MsType.DATETIME,(s,o) => s.AddDateTime((DateTime)o) },  //datetime
            { MsType.DATETIME2,(s,o) => s.AddDateTime((DateTime)o) },  //datetime
            { MsType.DECIMAL,(s,o) => s.AddNumber(Convert.ToDouble((decimal)o)) },  //decimal
            { MsType.DEC,(s,o) => s.AddNumber(Convert.ToDouble((decimal)o)) },  //decimal
            { MsType.DOUBLE_PRECISION,(s,o) => s.AddNumber((double)o) }, //double
            { MsType.FLOAT,(s,o) => s.AddNumber((double)o) },  //double
            { MsType.INT,(s,o) => s.AddInt32((int)o) },  //int
            { MsType.INTEGER,(s,o) => s.AddInt32((int)o) },  //int
            { MsType.MONEY,(s,o) => s.AddNumber(Convert.ToDouble((decimal)o)) },  //decimal
            { MsType.NCHAR,(s,o) => s.AddString((string)o) },  //string
            { MsType.NTEXT,(s,o) => s.AddString((string)o) },  //string
            { MsType.NUMERIC,(s,o) => s.AddNumber(Convert.ToDouble((decimal)o)) }, //decimal
            { MsType.NVARCHAR,(s,o) => s.AddString((string)o) }, //string
            { MsType.REAL,(s,o) => s.AddNumber(Convert.ToDouble((float)o)) }, //float
            { MsType.VARCHAR,(s,o) => s.AddString((string)o) }, //string
            { MsType.TINYINT,(s,o) => s.AddInt32((byte)o) }, //byte
            { MsType.SMALLDATETIME,(s,o) => s.AddDateTime((DateTime)o) }, //datetime
            { MsType.SMALLINT,(s,o) => s.AddInt32((short)o) }, //short
            { MsType.SMALLMONEY,(s,o) => s.AddNumber(Convert.ToDouble((decimal)o)) }, //decimal
            { MsType.TEXT,(s,o) => s.AddString((string)o) }, //string

            //TODO: [NOT IMPLEMENTED YET]
            { MsType.TIME,(s,o) => s.AddNull() }, // timespan
            { MsType.UNIQUEIDENTIFIER,(s,o) => s.AddNull() },  // guid
            { MsType.DATETIMEOFFSET,(s,o) => s.AddNull() }, //DateTimeOffset
            { MsType.VARBINARY,(s,o) => s.AddNull() }, // byte[]
            { MsType.BINARY,(s,o) => s.AddNull() }, //byte[]
            { MsType.TIMESTAMP,(s,o) => s.AddNull() }, // byte[]
            { MsType.IMAGE,(s,o) => s.AddNull() }, //byte[]
            { MsType.ROWVERSION,(s,o) => s.AddNull() }, //byte[]
            { MsType.XML,(s,o) => s.AddNull() }, // Xml
        };
    }
}
