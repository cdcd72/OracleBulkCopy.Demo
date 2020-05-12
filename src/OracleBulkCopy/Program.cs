using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace OracleBulkCopy
{
    static class Program
    {
        // 連線字串
        private static readonly string connectionString = "";

        static void Main(string[] args)
        {
            DataTable table = GetDataTable();

            // 將 DataTable 塞入 Oracle 資料庫...
            ImportToOra(table);
        }

        static DataTable GetDataTable()
        {
            #region DataTable 定義

            // Create a new DataTable.
            DataTable table = new DataTable("MQ_T_BAM432");

            // Declare variables for DataColumn and DataRow objects.
            DataColumn column;
            DataRow row;

            column = new DataColumn()
            {
                DataType = Type.GetType("System.String"),
                ColumnName = "QSEQNO",
                ReadOnly = true,
                Unique = true
            };

            // Add the Column to the DataColumnCollection.
            table.Columns.Add(column);

            // Create new DataColumn, set DataType, 
            // ColumnName and add to DataTable.    
            column = new DataColumn()
            {
                DataType = Type.GetType("System.Int32"),
                ColumnName = "TOTAL_QTY",
                ReadOnly = false,
                Unique = false
            };

            // Add the Column to the DataColumnCollection.
            table.Columns.Add(column);

            // Create second column.
            column = new DataColumn()
            {
                DataType = Type.GetType("System.String"),
                ColumnName = "BANK_CODE",
                ReadOnly = false,
                Unique = false
            };

            // Add the column to the table.
            table.Columns.Add(column);

            // Create second column.
            column = new DataColumn()
            {
                DataType = Type.GetType("System.String"),
                ColumnName = "BANK_NAME",
                ReadOnly = false,
                Unique = false
            };

            // Add the column to the table.
            table.Columns.Add(column);

            // Make the ID column the primary key column.
            DataColumn[] PrimaryKeyColumns = new DataColumn[1];
            PrimaryKeyColumns[0] = table.Columns["QSEQNO"];
            table.PrimaryKey = PrimaryKeyColumns;

            #endregion

            #region DataTable 塞入資料

            // Create three new DataRow objects and add 
            // them to the DataTable
            for (int i = 1; i <= 1025; i++)
            {
                row = table.NewRow();
                row["QSEQNO"] = i.ToString() + " QQ ";
                row["TOTAL_QTY"] = i;
                row["BANK_CODE"] = "5555555";
                row["BANK_NAME"] = "6666666";
                table.Rows.Add(row);
            }

            #endregion

            return table;
        }

        /// <summary>
        /// 批次匯入大量 Table 資料
        /// </summary>
        /// <param name="dataTable">表格資料</param>
        static void ImportToOra(DataTable dataTable)
        {
            const int BATCH_SIZE = 1024;

            var json = JsonConvert.SerializeObject(dataTable, Formatting.Indented);

            object data = ToCollections(JArray.Parse(json));

            var tableName = dataTable.TableName;

            var propNames = dataTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName);

            var oraParams = dataTable.Columns.Cast<DataColumn>().ToDictionary(o => o.ColumnName, o => {
                var p = new OracleParameter()
                {
                    ParameterName = o.ColumnName
                };
                switch (o.DataType.Name)
                {
                    case "String":
                        p.DbType = DbType.String;
                        break;
                    case "DateTime":
                        p.DbType = DbType.DateTime;
                        break;
                    case "Int32":
                        p.DbType = DbType.Int32;
                        break;
                    default:
                        throw new NotImplementedException(o.DataType.ToString());
                }
                return p;
            });

            string insertSql =
            $"INSERT INTO {tableName} ({string.Join(",", propNames)}) VALUES ({string.Join(",", propNames.Select(o => $":{o}").ToArray())})";

            using (var cn = new OracleConnection(connectionString))
            {
                cn.Open();

                using (var trans = cn.BeginTransaction())
                {

                    try
                    {
                        var cmd = cn.CreateCommand();
                        cmd.BindByName = true;
                        cmd.CommandText = insertSql;

                        foreach (var batchData in SplitBatch(data as IEnumerable<object>, BATCH_SIZE))
                        {
                            InsertWithArrayBinding(cmd, oraParams, batchData);
                        }

                        trans.Commit();
                    }
                    catch (Exception)
                    {
                        trans.Rollback();
                        throw;
                    }
                }
            }
        }

        /// <summary>
        /// 將 JArray  轉換為字典陣列
        /// 將 JObject 轉換為字典
        /// 字典：
        ///  Key   型態為 string,
        ///  Value 型態為 object
        /// </summary>
        /// <param name="o">JArray or JObject</param>
        /// <returns></returns>
        static object ToCollections(object o)
        {
            if (o is JObject jo) return jo.ToObject<IDictionary<string, object>>().ToDictionary(k => k.Key, v => ToCollections(v.Value));
            if (o is JArray ja) return ja.ToObject<List<object>>().Select(ToCollections).ToList();
            return o;
        }

        /// <summary>
        /// 資料分批
        /// </summary>
        /// <param name="items">欲分批資料</param>
        /// <param name="batchSize">資料分批大小</param>
        /// <returns></returns>
        static IEnumerable<object[]> SplitBatch(IEnumerable<object> items, int batchSize)
        {
            return items.Select((item, idx) => new { item, idx })
                        .GroupBy(o => o.idx / batchSize)
                        .Select(o => o.Select(p => p.item).ToArray());
        }

        /// <summary>
        /// 透過陣列繫結寫入資料
        /// </summary>
        /// <param name="cmd">命令</param>
        /// <param name="oraParams">參數</param>
        /// <param name="data">資料</param>
        static void InsertWithArrayBinding(OracleCommand cmd, Dictionary<string, OracleParameter> oraParams, object[] data)
        {
            cmd.ArrayBindCount = data.Length;
            cmd.Parameters.Clear();

            foreach (var pn in oraParams.Keys)
            {
                var p = oraParams[pn];

                p.Value = data.Select(x => { IDictionary<string, object> dic = (IDictionary<string, object>)x; return dic[pn]; })
                              .ToArray();

                cmd.Parameters.Add(p);
            }

            cmd.ExecuteNonQuery();
        }
    }
}
