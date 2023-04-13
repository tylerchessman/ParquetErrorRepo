using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace ConsApp_ParquetDTEg
{
    class Program
    {
        const string _Usr = "TestUser";
        const string _Pwd = "Your_Password_Here";
        const string _SvrName = "Server_Name_Here";

        //static void Main(string[] args)
        private static async Task Main(string[] args)
        {        
            try
            {
                var GP = new GenerateParquet(_Usr, _Pwd, _SvrName);
                await GP.ParquetFromDTExample();
            }
            catch (Exception Exc)
            {
                Console.WriteLine(Exc.ToString());
            }
        }
    }

    public class GenerateParquet
    {
        private System.Data.SqlClient.SqlConnection _oConn;
        private string _Usr;  private string _Pwd; private string _SvrName;

        public GenerateParquet(string userName, string pwd, string svrName)
        {
            _Usr = userName;
            _Pwd = pwd;
            _SvrName = svrName;
        }

        public async Task ParquetFromDTExample()
        {
            try
            {
                this.OpenConn();
                // Get DataTable
                string SQLQuery = "select top(100) API, Completion_No, Well_Completion_Date FROM dbo.WellCompletionTest;";
                var oDT = GetDataSetFromSQLClient(SQLQuery).Tables[0];
                Console.WriteLine(oDT.Rows.Count);

                // Generate Parquet File
                var PH = new Parq.ParquetHelper();
                await PH.DataTableToFile(oDT);
            }
            catch(Exception Exc) { throw Exc; }
            finally { this.CloseConn(); }

        }

        private void OpenConn()
        {
            try
            {
                string connectionString = "Data Source=" + _SvrName + ".database.windows.net;Initial Catalog=UsersDev2;Persist Security Info=True;User ID=" + _Usr + ";Password=" + _Pwd + ";MultipleActiveResultSets=True";
                _oConn = new System.Data.SqlClient.SqlConnection(connectionString);  // OleDb.OleDbConnection(sConn)
                _oConn.Open();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private void CloseConn()
        {
            try
            {
                if (_oConn != null)
                {
                    if (_oConn.State != System.Data.ConnectionState.Closed)
                        _oConn.Close();
                    _oConn.Dispose();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private DataSet GetDataSetFromSQLClient(string QueryString, System.Data.CommandType QueryType = CommandType.Text)
        {
            SqlCommand oCmd = null;
            SqlDataAdapter oAd;
            System.Data.DataSet oDataSet = null;

            string sQuery = QueryString;

            try
            {
                oCmd = new SqlCommand(sQuery, _oConn); // OleDb.OleDbCommand(sQuery, oConn)
                oCmd.CommandType = QueryType;
                oAd = new SqlDataAdapter(oCmd); // OleDb.OleDbDataAdapter(oCmd)
                oDataSet = new System.Data.DataSet();
                oAd.Fill(oDataSet);


                return oDataSet;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (oCmd != null)
                    oCmd.Dispose();
            }
        }
    }
}
