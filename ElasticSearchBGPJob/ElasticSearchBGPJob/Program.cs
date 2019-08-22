using Elasticsearch.Net;
using Microsoft.Extensions.Configuration;
using Nest;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace ElasticSearchBGPJob
{
    class Program
    {
        public static string CONNECTION_STRING = string.Empty;
        public static string INDEX_NAME = "elastic_search_sales";
        public static string INDEX_TYPE = "salesreport";

        static void Main(string[] args)
        {
            try
            {
                // read the config file ...
                var configuration = new ConfigurationBuilder()
                .SetBasePath(@"C:\Users\Ramanareddy\source\repos\ElasticSearchBGPJob\ElasticSearchBGPJob\")
                .AddJsonFile("appsettings.json", false)
                .Build();

                CONNECTION_STRING = configuration.GetSection("DefaultConnection").Value;

                if (string.IsNullOrEmpty(CONNECTION_STRING))
                    throw new ArgumentException("No connection string in appsettings.json");

                // 1. Connection URL's elastic search
                var listOfUrls = new Uri[]
                {
                // here we can set multple connectionn URL's...
                 new Uri("http://localhost:9200/")
                };

                StaticConnectionPool connPool = new StaticConnectionPool(listOfUrls);
                ConnectionSettings connSett = new ConnectionSettings(connPool);
                ElasticClient eClient = new ElasticClient(connSett);

                //  var see = eClient.DeleteIndex(INDEX_NAME);

                // check the connection health
                var checkClusterHealth = eClient.ClusterHealth();
                if (checkClusterHealth.ApiCall.Success && checkClusterHealth.IsValid)
                {
                    // 2. check the index exist or not 
                    var checkResult = eClient.IndexExists(INDEX_NAME);
                    if (!checkResult.Exists)
                    {
                        var createIndexResponse = eClient.CreateIndex(INDEX_NAME, c => c
                                         .Mappings(ms => ms.Map<Salesreport>(m => m.AutoMap()))
                                             );
                        if (createIndexResponse.ApiCall.Success && createIndexResponse.IsValid)
                        {
                            // index is created successfully....
                        }
                        else
                        {
                            // fail log the exception further use
                            var exception = createIndexResponse.OriginalException.ToString();
                            var debugException = createIndexResponse.DebugInformation.ToString();
                        }
                    }

                    // 3. get the last documet id of index
                    var lastRecordResponse = eClient.Search<Salesreport>(s => s
                        .Index(INDEX_NAME)
                        .Type(INDEX_TYPE)
                        .From(0)
                        .Size(1).Sort(sr => sr.Descending(f => f.Salesid)));

                    if (lastRecordResponse.ApiCall.Success && lastRecordResponse.IsValid)
                    {
                        Console.WriteLine("Start " + DateTime.Now);
                        long salesRecordId = 0;
                        var listofrecords = new List<Salesreport>();
                        if (lastRecordResponse.Documents.Count >= 1)
                        {
                            var obj = lastRecordResponse.Documents;
                            foreach (var item in obj)
                            {
                                salesRecordId = item.Salesid;
                            }

                            listofrecords = GetAllRecords(salesRecordId);

                        }
                        else
                        {
                            listofrecords = GetAllRecords(salesRecordId);
                        }

                        Console.WriteLine("END " + DateTime.Now);

                        //   Insert the data into document format corresponding index...
                        if (listofrecords.Count > 0)
                        {
                            Console.WriteLine("===== START========= " + DateTime.Now);
                            BulkInsertData(listofrecords, eClient).Wait();
                            Console.WriteLine("===== END========= " + DateTime.Now);
                        }
                    }
                    else
                    {
                        // fail log the exception further use
                        var exception = lastRecordResponse.OriginalException.ToString();
                        var debugException = lastRecordResponse.DebugInformation.ToString();
                    }
                }
                else
                {
                    // fail log the exception further use
                    var exception = checkClusterHealth.OriginalException.ToString();
                    var debugException = checkClusterHealth.DebugInformation.ToString();
                }
                Console.WriteLine("Hello World!");
                Console.ReadLine();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                Console.ReadLine();
            }
        }

        public static List<Salesreport> GetAllRecords(long LastSalesId)
        {
            List<Salesreport> salesReports = new List<Salesreport>();


            string sqlQuery = String.Format(@"SELECT SR.Id AS Salesid,SO.Id AS Orderid,SP.Id AS Priceid,Sr.Region,SR.Itemtype,SO.Orderdate,SP.Unitprice,SP.Totalrevenue FROM dbo.SalesReport SR WITH(NOLOCK) 
INNER JOIN dbo.SalesOrder SO WITH(NOLOCK)  ON SR.Id = SO.SalesReportId
INNER JOIN dbo.SalesPrice SP WITH(NOLOCK)  ON SO.Id = SP.SalesOrderId where SR.Id > {0}", LastSalesId);
            using (SqlConnection connection = new SqlConnection(CONNECTION_STRING))
            {
                connection.Open();
                using (SqlCommand command = new SqlCommand(sqlQuery, connection))
                {
                    command.CommandTimeout = 1000;
                    using (SqlDataReader dataReader = command.ExecuteReader())
                    {

                        if (dataReader.HasRows)
                        {
                            while (dataReader.Read())
                            {
                                Salesreport salesR = new Salesreport();

                                salesR.Salesid = Convert.ToInt64(dataReader["Salesid"]);
                                salesR.Orderid = Convert.ToInt64(dataReader["Orderid"]);
                                salesR.Priceid = Convert.ToInt64(dataReader["Priceid"]);
                                salesR.Region = Convert.ToString(dataReader["Region"]);
                                salesR.Itemtype = Convert.ToString(dataReader["Itemtype"]);
                                salesR.Orderdate = Convert.ToString(dataReader["Orderdate"]);
                                salesR.Unitprice = Convert.ToString(dataReader["Unitprice"]);
                                salesR.Totalrevenue = Convert.ToString(dataReader["Totalrevenue"]);

                                salesReports.Add(salesR);
                            }
                        }
                    }
                }
                connection.Close();
            }

            return salesReports;
        }
        static async Task BulkInsertData(List<Salesreport> ListofData, ElasticClient Eclient)
        {
            try
            {
                var splitTheLargeList = ChunkBy(ListofData);
                var test = splitTheLargeList.LastOrDefault();

                foreach (var item in splitTheLargeList)
                {
                    var bulkResponse = await Eclient.BulkAsync(b => b
                                        .Index(INDEX_NAME)
                                        // .Type(INDEX_TYPE)
                                        .IndexMany(item));

                    if (bulkResponse.ApiCall.Success && bulkResponse.IsValid)
                    {
                        // success fully inserted...
                    }
                    else
                    {
                        // fail log the exception further use
                        var exception = bulkResponse.OriginalException.ToString();
                        var debugException = bulkResponse.DebugInformation.ToString();
                    }
                }


            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.InnerException.ToString());
            }
        }

        public static List<List<T>> ChunkBy<T>(List<T> source, int chunkSize = 1000)
        {
            return source
                .Select((x, i) => new { Index = i, Value = x })
                .GroupBy(x => x.Index / chunkSize)
                .Select(x => x.Select(v => v.Value).ToList())
                .ToList();
        }

        public static IEnumerable<List<T>> SplitList<T>(List<T> ListofData, int listSize = 1000)
        {
            for (int i = 0; i < ListofData.Count; i += listSize)
            {
                yield return ListofData.GetRange(i, Math.Min(listSize, ListofData.Count - i));
            }
        }
    }

    class Salesreport
    {
        public long Salesid { get; set; }
        public long Orderid { get; set; }
        public long Priceid { get; set; }
        public string Region { get; set; }
        public string Itemtype { get; set; }
        public string Orderdate { get; set; }
        public string Unitprice { get; set; }
        public string Totalrevenue { get; set; }
    }
}
