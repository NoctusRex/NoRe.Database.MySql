using Microsoft.VisualStudio.TestTools.UnitTesting;
using NoRe.Core;
using NoRe.Database.Core.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace NoRe.Database.MySql.Test
{
    [TestClass]
    public class MySqlTests
    {
        [TestMethod]
        public void TestConfiguration()
        {
            MySqlWrapper wrapper = null;

            try
            {
                try { wrapper = new MySqlWrapper(); Assert.Fail(); } catch { }
                try { wrapper = new MySqlWrapper("123.123.123.123", "test", "test", "test"); } catch (Exception ex) { Assert.IsTrue(ex.Message.Contains("Unable to connect")); }
                try { wrapper = new MySqlWrapper(); Assert.Fail(); } catch { }

                wrapper = new MySqlWrapper("localhost", "test", "test", "test", "3306", true);
                Assert.IsTrue(File.Exists(Path.Combine(Pathmanager.ConfigurationDirectory, "MySqlConfiguration.xml")));

                wrapper = new MySqlWrapper();

            }
            finally
            {
                if (wrapper != null) wrapper.Dispose();
                DeleteConfiguration();
            }
        }

        [TestMethod()]
        public void TestNonQuery()
        {
            using (MySqlWrapper wrapper = new MySqlWrapper("localhost", "test", "test", "test"))
            {
                int key = new Random().Next(3, 9999999);
                Assert.IsTrue(wrapper.ExecuteNonQuery($"INSERT INTO test (id, value) VALUES (@0, @1)", key, "test") == 1);
                Assert.IsTrue(wrapper.ExecuteNonQuery($"DELETE FROM test WHERE id = {key};") == 1);
            }
        }

        [TestMethod()]
        public void TestQuery()
        {
            using (MySqlWrapper wrapper = new MySqlWrapper("localhost", "test", "test", "test"))
            {
                Table t = wrapper.ExecuteReader("SELECT * FROM test");

                Assert.AreEqual(1, t.GetValue<int>(0, "id"));
                Assert.AreEqual("already exists", t.GetValue<string>(0, "value"));
                Assert.AreEqual(2, t.GetValue<int>(1, "id"));
                Assert.AreEqual("Hello World", t.GetValue<string>(1, "value"));
            }
        }

        [TestMethod()]
        public void TestScalar()
        {
            using (MySqlWrapper wrapper = new MySqlWrapper("localhost", "test", "test", "test"))
            {
                Assert.AreEqual("Hello World", wrapper.ExecuteScalar<string>("SELECT value FROM test WHERE id = @0", 2));
            }
        }

        [TestMethod()]
        public void TestTransaction1()
        {
            using (MySqlWrapper wrapper = new MySqlWrapper("localhost", "test", "test", "test"))
            {
                Table t1 = wrapper.ExecuteReader("SELECT * FROM test");

                try
                {
                    List<Query> queries = new List<Query>();
                    queries.Add(new Query("INSERT INTO test (id, value) VALUES (45618, 'test')"));
                    queries.Add(new Query("INSERT INTO test (id, value) VALUES (1, 'test')"));

                    wrapper.ExecuteTransaction(queries);
                    Assert.Fail();
                }
                catch { }

                Assert.IsTrue(CompareTables(t1, wrapper.ExecuteReader("SELECT * FROM test")));
            }
        }

        [TestMethod()]
        public void TestTransaction2()
        {
            using (MySqlWrapper wrapper = new MySqlWrapper("localhost", "test", "test", "test"))
            {
                Table t1 = wrapper.ExecuteReader("SELECT * FROM test");

                try
                {
                    List<Query> queries = new List<Query>();
                    queries.Add(new Query("INSERT INTO test (id, value) VALUES (45618, 'test')"));
                    queries.Add(new Query("INSERT INTO test (id, value) VALUES (3, 'test')"));

                    wrapper.ExecuteTransaction(queries);
                    Assert.Fail();
                }
                catch { }

                Assert.IsFalse(CompareTables(t1, wrapper.ExecuteReader("SELECT * FROM test")));

                wrapper.ExecuteTransaction("DELETE FROM test WHERE id = @0 OR id = @1", 45618, 3);

                Assert.IsTrue(CompareTables(t1, wrapper.ExecuteReader("SELECT * FROM test")));
            }
        }

        private bool CompareTables(Table t1, Table t2)
        {
            if (t1.Rows.Count != t2.Rows.Count) return false;

            for (int i = 0; i < t1.Rows.Count; i++)
            {
                for (int c = 0; c < t1.DataTable.Columns.Count; c++)
                {
                    if (!Equals(t1.DataTable.Rows[i][c], t2.DataTable.Rows[i][c]))
                        return false;
                }
            }
            return true;
        }

        private void DeleteConfiguration()
        {
            try
            {
                Directory.Delete(Pathmanager.ConfigurationDirectory, true);
            }
            catch { }
        }

    }
}
