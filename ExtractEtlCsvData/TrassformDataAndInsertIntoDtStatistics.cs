 using System;
    using System.IO;
    using System.Data.SqlClient;
using EventLoggerETL;
using System.Windows.Forms;
using MySql.Data.MySqlClient;



namespace ExtractEtlCsvData
    {
        public class TrassformDataAndInsertIntoDtStatistics
    {
            private int rowCountTransactions;
            private double averagePrice;
            private double minPrice;
            private double maxPrice;
            private string statisticsFilePath = Path.Combine(Application.StartupPath, "statistics.ini");
        
        // grnera el pipe line de archivos 
        public void PipelineStatistics(string trascaction, string[] values, MySqlConnection connection)
        {
            string price = values[1];
            if (! double.TryParse(price, out double valorConvertido))
            {
                EventLoggerSQL.LogEvent("Error", "No se pudo convertir " + price + "el valor a doble para realizar ", "valorConvertido");
            }
            if (File.Exists(statisticsFilePath))
                {
                    string[] lines = File.ReadAllLines(statisticsFilePath);
                    rowCountTransactions = int.Parse(lines[0]);
                    averagePrice = double.Parse(lines[1]);
                    minPrice = double.Parse(lines[2]);
                    maxPrice = double.Parse(lines[3]);
                }
                else
                {

                //se controla si el archivo esta vacio o no existe 
                    rowCountTransactions = 0;
                    averagePrice = 0;
                    minPrice = valorConvertido;
                    maxPrice = valorConvertido;
                }
           
            if ( valorConvertido>=0)
            {
                statisticsUpdate(valorConvertido);
            }
            else
            {
                EventLoggerSQL.LogEvent("Error", "No se pudo convertir " + price + "el valor a doble para realizar ", "PipelineStatistics valorConvertido");

            }
                
            }

            public void statisticsUpdate(double price)
            {
                try
                {
                    rowCountTransactions++;
                    averagePrice = ((averagePrice * (rowCountTransactions - 1)) + price) / rowCountTransactions;
                    minPrice = Math.Min(minPrice, price);
                    maxPrice = Math.Max(maxPrice, price);

                    string[] lines = { rowCountTransactions.ToString(), averagePrice.ToString(), minPrice.ToString(), maxPrice.ToString() };
                    File.WriteAllLines(statisticsFilePath, lines);
                }
                catch (Exception ex)
                {
                  
                        EventLoggerSQL.LogEvent("Error", ex.Message, ex.StackTrace);
                }
            }

            public void InsertarEnBaseDeDatos(DateTime timestamp, int transactionId, string connectionString)
            {
                try
                {
                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                        connection.Open();

                        using (SqlCommand command = new SqlCommand("INSERT INTO statistics (TIMESTAMP, rowcounttrasactions, average_price, min_price, max_price, transaction_id) VALUES (@timestamp, @rowCount, @averagePrice, @minPrice, @maxPrice, @transactionId)", connection))
                        {
                            command.Parameters.AddWithValue("@timestamp", timestamp);
                            command.Parameters.AddWithValue("@rowCount", rowCountTransactions);
                            command.Parameters.AddWithValue("@averagePrice", averagePrice);
                            command.Parameters.AddWithValue("@minPrice", minPrice);
                            command.Parameters.AddWithValue("@maxPrice", maxPrice);
                            command.Parameters.AddWithValue("@transactionId", transactionId);

                            command.ExecuteNonQuery();
                        }
                    }
                }
                catch (Exception ex)
                {
                    if (!ex.Message.Contains("Duplicate"))
                        EventLoggerSQL.LogEvent("Error", ex.Message, ex.StackTrace);
                }
            }
        }
    }


