using System;
using System.Data;

namespace CopyDb.MetaData
{
    public class Column
    {
        public string TableName { get; set; }
        public string Name { get; set; }
        public int Position { get; set; }
        public bool IsNullable { get; set; }
        public string DataType { get; set; }
        public string DefaultValue { get; set; }
        public int? MaxLenth { get; set; }
        public int? NumericPrecision { get; set; }
        public int? NumericScale { get; set; }

        public override string ToString() => $"{Name} {DataType}";

        public Column(IDataReader reader)
        {
            _reader = reader;

            TableName = Get<string>("TABLE_NAME");
            Name = Get<string>("COLUMN_NAME");
            Position = Get<int>("ORDINAL_POSITION");
            DefaultValue = Get<string>("COLUMN_DEFAULT");
            IsNullable = Get<string>("IS_NULLABLE") == "YES";
            DataType = Get<string>("DATA_TYPE").ToUpper();
            MaxLenth = Get<int?>("CHARACTER_MAXIMUM_LENGTH");
            NumericPrecision = Get<byte?>("NUMERIC_PRECISION");
            NumericScale = Get<int?>("NUMERIC_SCALE");
        }

        private readonly IDataReader _reader;
        private T Get<T>(string key)
        {
            return _reader[key] == DBNull.Value ? default(T) : (T)_reader[key];
        }
    }
}