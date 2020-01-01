using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NoRe.Core;

namespace NoRe.Database.MySql
{
    public class MySqlConfiguration : Configuration
    {
        public MySqlConfiguration() : base(System.IO.Path.Combine(Pathmanager.ConfigurationDirectory, "MySqlConfiguration.xml")) { }

        /// <summary>
        /// Server address
        /// </summary>
        public string Server { get; set; }
        /// <summary>
        /// Database name 
        /// </summary>
        public string Database { get; set; }
        /// <summary>
        /// Username
        /// </summary>
        public string Uid { get; set; }
        /// <summary>
        /// Password
        /// </summary>
        public string Pwd { get; set; }
        /// <summary>
        /// Port of the server
        /// can be left empty to use the default port
        /// </summary>
        public string Port { get; set; }

        /// <summary>
        /// Reads the configuration from the xml file and fills the values of this object
        /// </summary>
        public override void Read()
        {
            MySqlConfiguration temp = Read<MySqlConfiguration>();
            if (temp is null) throw new Exception("Could not load configuration file");

            Server = temp.Server;
            Database = temp.Database;
            Uid = temp.Uid;
            Pwd = temp.Pwd;
            Port = temp.Port;
        }

        /// <summary>
        /// Returns the MySql connection string
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            string connectionString = "";

            if (!string.IsNullOrEmpty(Port)) connectionString += $"Port={Port};";
            if (!string.IsNullOrEmpty(Server)) connectionString += $"Server={Server};";
            if (!string.IsNullOrEmpty(Database)) connectionString += $"Database={Database};";
            if (!string.IsNullOrEmpty(Uid)) connectionString += $"Uid={Uid};";
            if (!string.IsNullOrEmpty(Pwd)) connectionString += $"Pwd={Pwd};";

            return connectionString;
        }
    }
}
