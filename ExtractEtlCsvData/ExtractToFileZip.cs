using ExtractEtlCsvData;
using System;
using System.IO;
using System.IO.Compression;
using LoadCsvToMySqlLoaderClass; 

namespace ExtractToFileZip
{
    public class ExtractToFile
    {
        public static string  FolderZipFiles ="" ;
        public static string FolderProcessedCsvZipFiles = "" ;
        public static void ExtractToFileZipPath()
        {
            FolderZipFiles = ExtractEtl.AppConfig.ConfigDictionary["FolderZipFiles"];
            FolderProcessedCsvZipFiles = ExtractEtl.AppConfig.ConfigDictionary["FolderProcessedCsvZipFiles"];
            if (!Directory.Exists(FolderZipFiles)) Directory.CreateDirectory(FolderZipFiles); 

            if (!FolderZipFiles.EndsWith(Path.DirectorySeparatorChar.ToString(), StringComparison.Ordinal)) FolderZipFiles += Path.DirectorySeparatorChar;


            //busco en la ruta los arcchivos zip  y se controla que no mueva el archivo  validation
            string[] zipFiles = Directory.GetFiles(FolderZipFiles, "*.zip");

            foreach (string zipFile in zipFiles)
            {          
                        using (ZipArchive archive = ZipFile.OpenRead(zipFile))
            {
                foreach (ZipArchiveEntry entry in archive.Entries)
                {
                    if (entry.FullName.EndsWith(".csv", StringComparison.OrdinalIgnoreCase)&& !entry.FullName.ToString().Contains("validation.csv"))
                    {
                        Directory.CreateDirectory(Path.Combine(FolderZipFiles));
                        // Gets the full path to ensure that relative segments are removed.
                        string destinationPath = Path.GetFullPath(Path.Combine(FolderZipFiles, entry.FullName));

                            if (!Directory.Exists(Path .GetDirectoryName (destinationPath)))Directory.CreateDirectory(Path.GetDirectoryName(destinationPath));
                            if (!File.Exists(destinationPath))  entry.ExtractToFile(destinationPath);
                    }
                }
            }
                LoadCsvToMySqlLoaderClass.LoadCsvToMySqlLoader. MoveFile(zipFile, FolderProcessedCsvZipFiles);     
            }
        }
    }
}