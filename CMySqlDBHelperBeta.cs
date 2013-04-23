using System;
using System.Collections.Generic;
using System.Web;
using MySql.Data;
using MySql.Data.MySqlClient;
using System.Configuration;
using System.Data;
using System.Data.Common;

namespace CComm
{
    // ================================================================================
    // Name: 对MySql数据库的增 删 改sql语句的执行
    // Author: ningyb
    // Date: 2012-3-15
    // Description: 对MySql数据库的增 删 改sql语句的执行
    // ================================================================================
    //变量、方法说明
    // connectionString;//连接数据库的字符串
    // ExecuteCommand(string sql) 返回操作影响的记录条数
    // ExecuteCommand(string sql, params MySqlParameter[] values) 返回操作影响的记录条数(对上一个方法的重载)
    // GetScalar(string sql) 返回查询结果的第一行第一列
    // GetScalar(string sql, params MySqlParameter[] values) 返回查询结果的第一行第一列(对上一个方法的重载)
    // GetReader(string sql) 返回查询结果的集合(只进只读,ExecuteReader)
    // GetReader(string sql, params MySqlParameter[] values) 返回查询结果的集合(只进只读,ExecuteReader)(对上一个方法的重载)
    // GetDataSet(string sql) 返回DataSet
    // GetDataSet(string sql, params MySqlParameter[] values)返回DataSet(对上一个方法的重载)
    // GetDataTable(string sql)返回DataTable
    // GetDataTable(string sql, params MySqlParameter[] values)返回DataTable(对上一个方法的重载)
    // ================================================================================
    // Change History
    // ================================================================================
    // 		Date:		Author:				Description:
    // 		--------	--------			-------------------
    //    
    // ================================================================================
    public class CMySqlDBHelperBeta
    {
        #region 属性

        public static string connectionString = ConfigurationManager.ConnectionStrings["mysqlConnectionString"].ConnectionString;

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
            using (MySqlConnection connection = new MySqlConnection(connectionString))
            {
                using (MySqlCommand cmd = new MySqlCommand(sql, connection))
                {
                    try
                    {
                        connection.Open();
                        int result = cmd.ExecuteNonQuery();
                        return result;
                    }
                    catch (MySqlException e)
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
        public static int ExecuteCommand(string sql, params MySqlParameter[] values)
        {
            using (MySqlConnection connection = new MySqlConnection(connectionString))
            {
                using (MySqlCommand cmd = new MySqlCommand(sql, connection))
                {
                    try
                    {
                        cmd.Parameters.AddRange(values);
                        connection.Open();
                        int result = cmd.ExecuteNonQuery();
                        cmd.Parameters.Clear();
                        return result;
                    }
                    catch (MySqlException e)
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
            using (MySqlConnection connection = new MySqlConnection(connectionString))
            {
                using (MySqlCommand cmd = new MySqlCommand(sql, connection))
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
                    catch (Exception e)
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
        public static int GetScalar(string sql, params MySqlParameter[] values)
        {
            using (MySqlConnection connection = new MySqlConnection(connectionString))
            {
                using (MySqlCommand cmd = new MySqlCommand(sql, connection))
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
                    catch (Exception e)
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
        public static MySqlDataReader GetReader(string sql)
        {
            MySqlConnection connection = new MySqlConnection(connectionString);
            MySqlCommand cmd = new MySqlCommand(sql, connection);
            try
            {
                connection.Open();
                MySqlDataReader reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                return reader;
            }
            catch (Exception e)
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
        public static MySqlDataReader GetReader(string sql, params MySqlParameter[] values)
        {
            MySqlConnection connection = new MySqlConnection(connectionString);
            MySqlCommand cmd = new MySqlCommand(sql, connection);
            try
            {
                connection.Open();
                cmd.Prepare();
                cmd.Parameters.AddRange(values);
                MySqlDataReader reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                cmd.Parameters.Clear();
                return reader;
            }
            catch (Exception e)
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
            using (MySqlConnection connection = new MySqlConnection(connectionString))
            {
                DataSet ds = new DataSet();
                try
                {
                    connection.Open();
                    MySqlDataAdapter command = new MySqlDataAdapter(sql, connection);
                    command.Fill(ds);
                }
                catch (Exception e)
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
        public static DataSet GetDataSet(string sql, params MySqlParameter[] values)
        {
            using (MySqlConnection connection = new MySqlConnection(connectionString))
            {
                MySqlCommand cmd = new MySqlCommand(sql,connection);
                cmd.Prepare();
                cmd.Parameters.AddRange(values);
                using (MySqlDataAdapter da = new MySqlDataAdapter(cmd))
                {
                    DataSet ds = new DataSet();
                    try
                    {
                        da.Fill(ds);
                        cmd.Parameters.Clear();
                    }
                    catch (Exception e)
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
            using (MySqlConnection connection = new MySqlConnection(connectionString))
            {
                DataTable dt = new DataTable();
                try
                {
                    connection.Open();
                    MySqlDataAdapter command = new MySqlDataAdapter(sql, connection);
                    command.Fill(dt);
                }
                catch (Exception e)
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
        public static DataTable GetDataTable(string sql, params MySqlParameter[] values)
        {
            using (MySqlConnection connection = new MySqlConnection(connectionString))
            {
                MySqlCommand cmd = new MySqlCommand(sql, connection);
                cmd.Prepare();
                cmd.Parameters.AddRange(values);
                using (MySqlDataAdapter da = new MySqlDataAdapter(cmd))
                {
                    DataTable dt = new DataTable();
                    try
                    {
                        da.Fill(dt);
                        cmd.Parameters.Clear();
                    }
                    catch (Exception e)
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
