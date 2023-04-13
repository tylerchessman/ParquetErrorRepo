using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Parquet;
using Parquet.Schema;

using System.Data;
using System.IO;
using System.Collections;


// https://github.com/dazfuller/datatable-to-parquet
namespace Parq
{
    public class ParquetHelper
    {
        private const int RowGroupSize = 100;
        private const string OutputFilePath = "example.parquet";

        //public static byte[] DataTableToParquet(System.Data.DataTable oDT)
        //{
        //    try {
        //        Schema schema = SchemaBuilder.FromDataTable(oDT).Build();
        //    }
        //    catch (Exception Exc)
        //    {
        //        Console.WriteLine(Exc.ToString());
        //        throw Exc;
        //    }
        //}

        public async Task DataTableToFile(System.Data.DataTable oDT)
        {
            try
            {
                var dt = oDT; //  GenerateTestData();
                var fields = GenerateSchema(dt);

                // Open the output file for writing
                using (var stream = File.Open(OutputFilePath, FileMode.Create, FileAccess.Write))
                {
                    using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(fields), stream))
                    {
                        var startRow = 0;

                        // Keep on creating row groups until we run out of data
                        while (startRow < dt.Rows.Count)
                        {
                            // using(ParquetRowGroupWriter groupWriter = writer.CreateRowGroup()) {
                            using (var rgw = writer.CreateRowGroup())
                            {
                                // Data is written to the row group column by column
                                for (var i = 0; i < dt.Columns.Count; i++)
                                {
                                    var columnIndex = i;

                                    // Determine the target data type for the column
                                    var targetType = dt.Columns[columnIndex].DataType;
                                    if (targetType == typeof(DateTime)) targetType = typeof(DateTimeOffset);

                                    // Generate the value type, this is to ensure it can handle null values
                                    var valueType = targetType.IsClass
                                        ? targetType
                                        : typeof(Nullable<>).MakeGenericType(targetType);

                                    // Create a list to hold values of the required type for the column
                                    var list = (IList)typeof(List<>)
                                        .MakeGenericType(valueType)
                                        .GetConstructor(Type.EmptyTypes)
                                        .Invoke(null);

                                    // Get the data to be written to the parquet stream
                                    foreach (var row in dt.AsEnumerable().Skip(startRow).Take(RowGroupSize))
                                    {
                                        // Check if value is null, if so then add a null value
                                        if (row[columnIndex] == null || row[columnIndex] == DBNull.Value)
                                        {
                                            list.Add(null);
                                        }
                                        else
                                        {
                                            // Add the value to the list, but if it's a DateTime then create it as a DateTimeOffset first
                                            list.Add(dt.Columns[columnIndex].DataType == typeof(DateTime)
                                                ? new DateTimeOffset((DateTime)row[columnIndex])
                                                : row[columnIndex]);
                                        }
                                    }

                                    // Copy the list values to an array of the same type as the WriteColumn method expects
                                    // and Array
                                    var valuesArray = Array.CreateInstance(valueType, list.Count);
                                    list.CopyTo(valuesArray, 0);

                                    // Write the column
                                    await rgw.WriteColumnAsync(new Parquet.Data.DataColumn(fields[i], valuesArray));

                                }
                            }

                            startRow += RowGroupSize;
                        }
                    }
                }

            }
            catch (Exception Exc)
            {
                throw Exc;
            }
            
        }


        private static List<DataField> GenerateSchema(DataTable dt)
        {
            var fields = new List<DataField>(dt.Columns.Count);
            bool testDate = false;

            foreach (System.Data.DataColumn column in dt.Columns)
            {
                // Attempt to parse the type of column to a parquet data type
                var success = Enum.TryParse<DataType>(column.DataType.Name, true, out var type);

                // If the parse was not successful and it’s source is a DateTime then use a DateTimeOffset, otherwise default to a string
                if (!success && column.DataType == typeof(DateTime))
                {
                    type = DataType.DateTimeOffset;
                    testDate = true;  //tlc - try forcing DataTime to string for now?
                }
                else if (!success)
                {
                    type = DataType.String;
                }

                if (testDate == true)
                {
                    //https://github.com/aloneguid/parquet-dotnet
                    //fields.Add(new DataField<DateTime>(column.ColumnName));
                    fields.Add(new DataField<string>(column.ColumnName));   // DateTime forced to string, but doesn't matter...
                }
                else
                {
                    fields.Add(new DataField(column.ColumnName, type));
                }
                    

                testDate = false;
            }

            return fields;
        }

    }
}
