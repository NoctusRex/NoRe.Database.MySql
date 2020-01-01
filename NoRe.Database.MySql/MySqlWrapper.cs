using MySql.Data.MySqlClient;
using NoRe.Database.Core.Models;
using System;
using System.Collections.Generic;

namespace NoRe.Database.MySql
{
    public class MySqlWrapper : Core.IDatabase
    {
        private static MySqlConfiguration Configuration { get; set; }
        public MySqlConnection Connection { get; set; }
        public MySqlTransaction Transaction { get; set; }

        /// <summary>
        /// Creates a new MySqlWrapper
        /// Uses specefied values as connection string
        /// Does not read or write a configuration file
        /// Throws an exception if the database is not reachable
        /// </summary>
        /// <param name="server"></param>
        /// <param name="database"></param>
        /// <param name="uid"></param>
        /// <param name="pwd"></param>
        /// <param name="port"></param>
        public MySqlWrapper(string server, string database, string uid, string pwd, string port = "", bool doWrite = false)
        {
            Configuration = new MySqlConfiguration
            {
                Server = server,
                Database = database,
                Uid = uid,
                Pwd = pwd,
                Port = port
            };
            if (doWrite) Configuration.Write();

            Connection = new MySqlConnection(Configuration.ToString());

            if (!TestConnection(out string error)) throw new Exception(error);
        }

        /// <summary>
        /// Creates a new MySqlWrapper
        /// Creats and loads the connection string from the configuration file
        /// Throws an exception if the database is not reachable
        /// </summary>
        public MySqlWrapper()
        {
            Configuration = new MySqlConfiguration();
            Configuration.Read();

            Connection = new MySqlConnection(Configuration.ToString());

            if (!TestConnection(out string error)) throw new Exception(error);
        }

        public int ExecuteNonQuery(string commandText, params object[] parameters)
        {
            try
            {
                Connection.Open();
                return GetCommand(commandText, parameters).ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                Connection.Close();
            }
        }

        public Table ExecuteReader(string commandText, params object[] parameters)
        {
            try
            {
                Connection.Open();

                MySqlDataReader reader = GetCommand(commandText, parameters).ExecuteReader();

                Table table = new Table
                {
                    DataTable = reader.GetSchemaTable()
                };

                while (reader.Read())
                {
                    Row row = new Row();

                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        row.Columns.Add(new Column(reader.GetName(i), reader.GetValue(i)));
                    }

                    table.Rows.Add(row);
                }

                return table;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                Connection.Close();
            }
        }

        public T ExecuteScalar<T>(string commandText, params object[] parameters)
        {
            try
            {
                Connection.Open();
                return (T)GetCommand(commandText, parameters).ExecuteScalar();
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                Connection.Close();
            }
        }

        public void ExecuteTransaction(List<Query> queries)
        {
            try
            {
                StartTransaction();

                foreach (Query query in queries)
                {
                    GetCommand(query.CommandText, query.Parameters).ExecuteNonQuery();
                }

                CommitTransaction();
            }
            catch (Exception ex)
            {
                RollbackTransaction();
                throw ex;
            }
            finally
            {
                Connection.Close();
            }
        }

        public void ExecuteTransaction(string commandText, params object[] parameters)
        {
            ExecuteTransaction(new List<Query>
            {
                new Query(commandText, parameters)
            });
        }

        public bool TestConnection(out string error)
        {
            try
            {
                Connection.Open();

                error = "";
                return true;
            }
            catch (Exception ex)
            {
                error = ex.Message;
                return false;
            }
            finally
            {
                Connection.Close();
            }
        }

        /// <summary>
        /// Rolls back the current transaction and closes the connection
        /// </summary>
        private void RollbackTransaction()
        {
            try
            {
                Transaction.Rollback();
            }
            finally
            {
                Connection.Close();
                Transaction.Dispose();
                Transaction = null;
            }

        }

        /// <summary>
        /// Committs the current transaction and closes the connection
        /// </summary>
        private void CommitTransaction()
        {
            try
            {
                Transaction.Commit();
            }
            finally
            {
                Connection.Close();
                Transaction.Dispose();
                Transaction = null;
            }
        }

        /// <summary>
        /// Starts the current transaction and opens a connection
        /// </summary>
        private void StartTransaction()
        {
            try
            {
                if (Connection.State != System.Data.ConnectionState.Open)
                {
                    Connection.Open();
                }

                Transaction = Connection.BeginTransaction();
            }
            catch (Exception ex)
            {
                if (Transaction != null) Transaction.Dispose();
                Transaction = null;
                throw ex;
            }

        }

        /// <summary>
        /// Creates a new command with parameters and prepares it
        /// </summary>
        /// <param name="commandText">The command to be executed</param>
        /// <param name="parameters">The parameters of the command</param>
        /// <returns></returns>
        private MySqlCommand GetCommand(string commandText, params object[] parameters)
        {
            MySqlCommand command = Connection.CreateCommand();

            command.CommandText = commandText;
            command.Connection = Connection;
            if (Transaction != null) command.Transaction = Transaction;

            for (int i = 0; i < parameters.Length; i++) { command.Parameters.AddWithValue($"@{i}", parameters[i]); }

            command.Prepare();

            return command;
        }

        /// <summary>
        /// Disposes the transaction and the connection
        /// </summary>
        public void Dispose()
        {
            if (Transaction != null)
            {
                Transaction.Dispose();
            }

            if (Connection != null)
            {
                Connection.Close();
                Connection.Dispose();
            }

        }
    }
}
