using System;
using System.Collections.Generic;
using System.Data;
using System.Text.RegularExpressions;

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
        public int? DateTymePrecision { get; set; }
        public MsType Type => (MsType) Enum.Parse(typeof(MsType), DataType.Replace(" ", "_"));
        public bool IsMax => MaxLenth.HasValue && MaxLenth.Value == -1;

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
            DateTymePrecision = Get<short?>("DATETIME_PRECISION");
        }

        private readonly IDataReader _reader;
        private T Get<T>(string key)
        {
            return _reader[key] == DBNull.Value ? default(T) : (T)_reader[key];
        }

        public string Render()
        {
            var parts = new List<string>();
            parts.Add($"\"{Name}\"");
            parts.Add(TypeConverter.GetPostgresType(this));
            if(!String.IsNullOrEmpty(DefaultValue))
                parts.Add($"DEFAULT {GetDefaultValue()}");
            
            if(!IsNullable)
                parts.Add("NOT NULL");
            
            return "\t" + String.Join(" ", parts);
        }

        private string GetDefaultValue()
        {
            var value = DefaultValue
                .Replace("(", "")
                .Replace(")", "")
                .Replace("N'", "'");

            value = Regex.Replace(value, "getdate\\(?\\)?", "now()", RegexOptions.IgnoreCase);

            if (Type != MsType.BIT)
                return value;

            if (value == "0")
                return "false";

            if (value == "1")
                return "true";

            return value;
        }
    }
}