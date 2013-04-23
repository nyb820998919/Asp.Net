using System;
using System.Collections.Generic;
using System.Web;
using System.Configuration;
using System.Data;
using System.Data.OleDb;

namespace CComm
{
    /// <summary>
    ///
    /// OleDb数据库操作类Beta 
	///
	/// 数据库连接字符串
	///     <add name="oledb_con" connectionString="Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" />
    ///     <add name="oledb_path" connectionString="~/App_Data/prizeSurvey.accdb" />
	///
    /// </summary>
    public class COledbDBHelperBeta
    {
        #region 属性

        private static string connectionString = ConfigurationManager.ConnectionStrings["oledb_con"].ConnectionString
        + HttpContext.Current.Server.MapPath(ConfigurationManager.ConnectionStrings["oledb_path"].ConnectionString);

        #endregion

        #region 方法

        /// <summary>
        /// 返回操作影响的记录条数(ExecuteNonQuery)
        /// </summary>
        /// <param name="sql">要执行的sql语句</param> 
        /// <example>
        /// </example>
        /// <returns></returns>
        public static int ExecuteCommand(string sql)
        {
            using (OleDbConnection connection = new OleDbConnection(connectionString))
            {
                using (OleDbCommand cmd = new OleDbCommand(sql, connection))
                {
                    try
                    {
                        connection.Open();
                        int result = cmd.ExecuteNonQuery();
                        return result;
                    }
                    catch (OleDbException e)
                    {
                        connection.Close();
                        throw new Exception(e.Message, e);
                    }
                }
            }
        }

        /// <summary>
        /// 返回操作影响的记录条数(ExecuteNonQuery)
        /// </summary>
        /// <param name="sql">要执行的sql语句</param>
        /// <param name="values">sql语句中的参数</param>
        /// <returns></returns>
        public static int ExecuteCommand(string sql, params OleDbParameter[] values)
        {
            using (OleDbConnection connection = new OleDbConnection(connectionString))
            {
                using (OleDbCommand cmd = new OleDbCommand(sql, connection))
                {
                    try
                    {
                        cmd.Parameters.AddRange(values);
                        connection.Open();
                        int result = cmd.ExecuteNonQuery();
                        cmd.Parameters.Clear();
                        return result;
                    }
                    catch (OleDbException e)
                    {
                        connection.Close();
                        throw new Exception(e.Message, e);
                    }
                }
            }
        }

        /// <summary>
        /// 返回查询结果的第一行第一列(ExecuteScalar)
        /// </summary>
        /// <param name="sql">要执行的sql语句</param>
        /// <returns></returns>
        public static int GetScalar(string sql)
        {
            using (OleDbConnection connection = new OleDbConnection(connectionString))
            {
                using (OleDbCommand cmd = new OleDbCommand(sql, connection))
                {
                    try
                    {
                        connection.Open();
                        object obj = cmd.ExecuteScalar();
                        if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
                        {
                            return 0;
                        }
                        else
                        {
                            return int.Parse(obj.ToString());
                        }
                    }
                    catch (OleDbException e)
                    {
                        connection.Close();
                        throw new Exception(e.Message, e);
                    }
                }
            }
        }

        /// <summary>
        /// 返回查询结果的第一行第一列(ExecuteScalar)
        /// </summary>
        /// <param name="sql">要执行的sql语句</param>
        /// <param name="values">sql语句中的参数</param>
        /// <returns></returns>
        public static int GetScalar(string sql, params OleDbParameter[] values)
        {
            using (OleDbConnection connection = new OleDbConnection(connectionString))
            {
                using (OleDbCommand cmd = new OleDbCommand(sql, connection))
                {
                    try
                    {
                        cmd.Parameters.AddRange(values);
                        connection.Open();
                        object obj = cmd.ExecuteScalar();
                        cmd.Parameters.Clear();
                        if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
                        {
                            return 0;
                        }
                        else
                        {
                            return int.Parse(obj.ToString());
                        }
                    }
                    catch (OleDbException e)
                    {
                        connection.Close();
                        throw new Exception(e.Message, e);
                    }
                }
            }
        }

        /// <summary>
        /// 返回查询结果的集合(只进只读,ExecuteReader)
        /// </summary>
        /// <param name="sql">要执行的sql语句</param>
        /// <returns></returns>
        public static OleDbDataReader GetReader(string sql)
        {
            OleDbConnection connection = new OleDbConnection(connectionString);
            OleDbCommand cmd = new OleDbCommand(sql, connection);
            try
            {
                connection.Open();
                OleDbDataReader reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                return reader;
            }
            catch (OleDbException e)
            {
                connection.Close();
                throw new Exception(e.Message, e);
            }
        }

        /// <summary>
        /// 返回查询结果的集合(只进只读,ExecuteReader)
        /// </summary>
        /// <param name="sql">要执行的sql语句</param>
        /// <param name="values">sql语句中的参数</param>
        /// <returns></returns>
        public static OleDbDataReader GetReader(string sql, params OleDbParameter[] values)
        {
            OleDbConnection connection = new OleDbConnection(connectionString);
            OleDbCommand cmd = new OleDbCommand(sql, connection);
            try
            {
                connection.Open();
                cmd.Prepare();
                cmd.Parameters.AddRange(values);
                OleDbDataReader reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                cmd.Parameters.Clear();
                return reader;
            }
            catch (OleDbException e)
            {
                connection.Close();
                throw new Exception(e.Message, e);
            }
        }

        /// <summary>
        /// 返回DataSet
        /// </summary>
        /// <param name="sql">要执行的sql语句</param>
        /// <returns></returns>
        public static DataSet GetDataSet(string sql)
        {
            using (OleDbConnection connection = new OleDbConnection(connectionString))
            {
                DataSet ds = new DataSet();
                try
                {
                    connection.Open();
                    OleDbDataAdapter command = new OleDbDataAdapter(sql, connection);
                    command.Fill(ds);
                }
                catch (OleDbException e)
                {
                    connection.Close();
                    throw new Exception(e.Message, e);
                }
                return ds;
            }
        }

        /// <summary>
        /// 返回DataSet
        /// </summary>
        /// <param name="sql">要执行的sql语句</param>
        /// <param name="values">sql语句中的参数</param>
        /// <returns></returns>
        public static DataSet GetDataSet(string sql, params OleDbParameter[] values)
        {
            using (OleDbConnection connection = new OleDbConnection(connectionString))
            {
                OleDbCommand cmd = new OleDbCommand(sql, connection);
                cmd.Prepare();
                cmd.Parameters.AddRange(values);
                using (OleDbDataAdapter da = new OleDbDataAdapter(cmd))
                {
                    DataSet ds = new DataSet();
                    try
                    {
                        da.Fill(ds);
                        cmd.Parameters.Clear();
                    }
                    catch (OleDbException e)
                    {
                        connection.Close();
                        throw new Exception(e.Message, e);
                    }
                    return ds;
                }
            }
        }

        /// <summary>
        /// 返回DataTable
        /// </summary>
        /// <param name="sql">要执行的sql语句</param>
        /// <returns></returns>
        public static DataTable GetDataTable(string sql)
        {
            using (OleDbConnection connection = new OleDbConnection(connectionString))
            {
                DataTable dt = new DataTable();
                try
                {
                    connection.Open();
                    OleDbDataAdapter command = new OleDbDataAdapter(sql, connection);
                    command.Fill(dt);
                }
                catch (OleDbException e)
                {
                    connection.Close();
                    throw new Exception(e.Message, e);
                }
                return dt;
            }
        }

        /// <summary>
        /// 返回DataTable
        /// </summary>
        /// <param name="sql">要执行的sql语句</param>
        /// <param name="values">sql语句中的参数</param>
        /// <returns></returns>
        public static DataTable GetDataTable(string sql, params OleDbParameter[] values)
        {
            using (OleDbConnection connection = new OleDbConnection(connectionString))
            {
                OleDbCommand cmd = new OleDbCommand(sql, connection);
                cmd.Prepare();
                cmd.Parameters.AddRange(values);
                using (OleDbDataAdapter da = new OleDbDataAdapter(cmd))
                {
                    DataTable dt = new DataTable();
                    try
                    {
                        da.Fill(dt);
                        cmd.Parameters.Clear();
                    }
                    catch (OleDbException e)
                    {
                        connection.Close();
                        throw new Exception(e.Message, e);
                    }
                    return dt;
                }
            }
        }

        #endregion

    }
}