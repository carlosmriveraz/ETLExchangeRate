namespace ExtractEtlCsvData
{
    using LoadCsvToMySqlLoaderClass;
    using EventLogger;
    using ExtractToFileZip;
    using System;
    using System.Collections.Generic;
    using EventLoggerETL;
    using System.IO;

    /// <summary>
    /// Defines the <see cref="ExtractEtl" />
    /// </summary>
    public class ExtractEtl
    {
        public static class AppConfig
        {
            public static  Dictionary<string, string> ConfigDictionary = new Dictionary<string, string>();

        }

        /// <summary>
        /// The ExtractEtlCsvData
        /// </summary>
        /// <returns>The <see cref="bool"/></returns>
        public  bool ExtractEtlCsvData()
        {
            bool extractEtlCsvData = false ;
            try
            {
              if (Directory.GetFiles(AppConfig. ConfigDictionary["FolderZipFiles"], "*.*", SearchOption.AllDirectories).Length > 0 )
                {

                
                ExtractToFileZipPath();        //Extraigo los datos del archivo ZIP a la carpeta configurada        

                
                LoadCsvFiles();//Cargo archivos, los leo inserto a mysql 

                extractEtlCsvData = true;
                }
            }
             catch (Exception ex)
            {
                EventLoggerSQL.LogEvent("ERROR", ex.Message + " Ingesta LoadCsvFiles", ex.StackTrace + "LoadCsvFiles");
            }        
            return extractEtlCsvData; 
        }

        private void LoadCsvFiles()
        {
            LoadCsvToMySqlLoader.LoadCsvFiles();
            EventLoggerApp.LogEvent("OK", " Ingesta LoadCsvFiles", "LoadCsvFiles");
           
        }

        private void ExtractToFileZipPath()
        {

           
            ExtractToFile.ExtractToFileZipPath();
            EventLoggerApp.LogEvent("OK", " Se descomprime exitosamente ", "ExtractToFileZipPathExtractToFileZipPath");
        }
    }

}
