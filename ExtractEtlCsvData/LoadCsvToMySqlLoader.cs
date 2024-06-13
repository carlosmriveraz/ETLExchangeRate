using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using MySql.Data.MySqlClient;
using EventLoggerETL;
using ExtractEtlCsvData;
using static ExtractEtlCsvData.ExtractEtl;
using EventLogger;

namespace LoadCsvToMySqlLoaderClass
{
    public class LoadCsvToMySqlLoader
    {
        EventLoggerSQL EventLogger = new EventLoggerSQL();
       
        public static  string FolderZipFiles = ExtractEtl.AppConfig.ConfigDictionary["FolderZipFiles"]; // Ruta del archivo de origen
        public static string FolderProcessedCsvZipFiles = ExtractEtl.AppConfig.ConfigDictionary["FolderProcessedCsvZipFiles"]; // Ruta del directorio de destino

        public static void LoadCsvFiles()
        {
            try
            {
                List<string> filePaths = ListFilesInDirectory(FolderZipFiles);
                filePaths.Sort();
                foreach (var filePath in filePaths)
                {
                    EventLoggerSQL.LogEvent("OK", "IniciaProcesamiento del archhivo " + filePath, " class LoadCsvToMySqlLoader metodo LoadCsvFiles"); 
                    ReadAndInsertCsv(filePath);

                    //muevo los archivos procesados a la ruta configurada para que no se procese infinitamente. 
                    MoveFile(filePath, FolderProcessedCsvZipFiles);


                    EventLoggerSQL.LogEvent("OK", "Termina del archhivo " + filePath, " class LoadCsvToMySqlLoader metodo LoadCsvFiles");
                    EventLoggerApp.LogEvent("OK", "Termina del archhivo " + filePath, " class LoadCsvToMySqlLoader metodo LoadCsvFiles");
                    break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al cargar archivos CSV: {ex.Message}");
            }
        }

        public static void ReadAndInsertCsv(string filePath)
        {
            string connectionString = ExtractEtl.AppConfig.ConfigDictionary["ConnectionString"];
            try
            {
                var lines = File.ReadAllLines(filePath);
                using (var connection = new MySqlConnection(connectionString))
                {
                    connection.Open();
                    foreach (var line in lines)
                    {
                        if (!line.Contains("timestamp")) //se controla que no inserte el encavezado del csv 
                        {
                            string trascaction = Path.GetFileNameWithoutExtension(filePath);
                            var values = line.Split(',');
                            InsertDataIntoDtTransactions(trascaction, values, connection);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al leer e insertar CSV: {ex.Message}");
            }
        }

        /// <summary>
        /// InsertDataIntoDtTransactions en el sieño de la base datos, se controla que no ingrese los mismo valores y no afecte los datos 
        /// </summary>
          public static void InsertDataIntoDtTransactions(string trascaction, string[] values, MySqlConnection connection)
        {
            DateTime fechaConvertida ;

            var commandText = "INSERT INTO transactions (transactionid, timestamptransaction, price, user_id) VALUES (@transactionid, @timestamptransaction, @price, @user_id);";
            try
            {
                using (var command = new MySqlCommand(commandText, connection))
                {
                    command.Parameters.AddWithValue($"@transactionid", trascaction);
                     fechaConvertida = DateTime.ParseExact(values[0].ToString(), "M/d/yyyy", System.Globalization.CultureInfo.InvariantCulture);
                    command.Parameters.AddWithValue($"@timestamptransaction", fechaConvertida);
                    command.Parameters.AddWithValue($"@price", values[1]);
                    command.Parameters.AddWithValue($"@user_id", values[2]);
                                       int result = command.ExecuteNonQuery();
                    if (result > 0)
                    {
                        EventLoggerApp.LogEvent("OK", trascaction + fechaConvertida.ToString() + values[0] + values[1] + values[2],"Insertado Correctamente");
                        InsertDataIntoDtStatistics(trascaction, values, connection);
                    }
                }
            }
            catch (Exception ex)
            {                
                if (!ex.Message .Contains("Duplicate")) //se controla que no llene el log para transacciones duplicadas
                    EventLoggerApp.LogEvent("Error", ex.Message, ex.StackTrace);

            }
        }
        public static void InsertDataIntoDtStatistics(string trascaction, string[] values, MySqlConnection connection)
        { 
        
            TrassformDataAndInsertIntoDtStatistics TrassformDataAndInsertIntoDtStatistics = new TrassformDataAndInsertIntoDtStatistics();
            TrassformDataAndInsertIntoDtStatistics.PipelineStatistics(trascaction, values, connection);
        }

        public static List<string> ListFilesInDirectory(string path)
        {
            List<string> fileList = new List<string>();
            try
            {
                if (Directory.Exists(path))
                {
                    string[] files = Directory.GetFiles(path, "*.*", SearchOption.AllDirectories);
                    foreach (string file in files)
                    {
                        fileList.Add(file);
                    }
                }
                else
                {
                    Console.WriteLine("El directorio no existe.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Ocurrió un error al listar los archivos: " + ex.Message);
            }

            return fileList;
        }

        public static void MoveFile(string sourceFilePath, string targetDirectory)
        {
            if (!File.Exists(sourceFilePath))
            {
                throw new FileNotFoundException("El archivo de origen no existe.", sourceFilePath);
            }

            string fileName = Path.GetFileName(sourceFilePath);
            string targetFilePath = Path.Combine(targetDirectory, fileName);

            if (File.Exists(targetFilePath))
            {
                // Si el archivo ya existe en el directorio de destino, renombrarlo con una marca de tiempo
                string timestamp = DateTime.Now.ToString("yyyyMMddHHmmss");
                string newFileName = $"{Path.GetFileNameWithoutExtension(fileName)}_{timestamp}{Path.GetExtension(fileName)}";
                targetFilePath = Path.Combine(targetDirectory, newFileName);
            }

            File.Move(sourceFilePath, targetFilePath);
        }

    }
}
