using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Logging;

namespace FolderWatcher.Tasks
{
    public class FileProcessor : IWantToRunWhenBusStartsAndStops
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof (FileProcessor));

        private const string DesktopFolder = "{Desktop}";
        private const string LocalApplicationData = "{LocalApplicationData}";
        private const string ApplicationData = "{ApplicationData}";
        // todo: add more special folder constants here

        public void Start()
        {
            Logger.Info("Starting FileProcessor");

            var interval = Convert.ToInt32(ConfigurationManager.AppSettings["ProcessFilesInterval"] ?? "1");

            CheckFilePaths();

            Logger.InfoFormat("Scheduling process to run every {0} minutes", interval);
            //Schedule.Every(TimeSpan.FromMinutes(interval)).Action(ProcessFilesInWatchFolder);
            Schedule.Every(TimeSpan.FromSeconds(10)).Action(ProcessFilesInWatchFolder);
        }

        public void Stop()
        {
            Logger.Info("Stopping FileProcessor");
        }

        private void ProcessFilesInWatchFolder()
        {
            Logger.Debug("Processing files in watch folder");

            var folderToWatch = GetFolderToWatchPath();
            var filePattern = ConfigurationManager.AppSettings["FilePattern"] ?? "*.pdf";

            if (!Directory.Exists(folderToWatch))
            {
                Logger.ErrorFormat("Folder To Watch is not found or is inaccsesible: {0}", folderToWatch);
                return;
            }

            var files = Directory.GetFiles(folderToWatch, filePattern);

            CheckFilePaths();

            if (!files.Any()) return;

            Logger.InfoFormat("Processing {0} files", files.Length);
                
            foreach (var filePath in files)
            {
                var fileName = Path.GetFileNameWithoutExtension(filePath) ?? string.Empty;

                if (fileName.StartsWith("TEST-FILE", StringComparison.InvariantCultureIgnoreCase))
                {
                    ProcessTestFile(filePath);
                    continue;
                }

                if (fileName.Length < 9)
                {
                    HandleFileError(filePath, "Filename length too short");
                    continue;
                }

                ProcessFile(filePath);
            }
        }

        private void ProcessFile(string filePath)
        {
            Logger.InfoFormat("Processing file: {0}", filePath);

            try
            {
                var processingFolder = GetProcessingFolderPath();
                var targetFilePath = GetSafeTargetFileName(filePath, processingFolder);

                if (IsFileLocked(filePath))
                {
                    Logger.WarnFormat("File access locked, not ready for move: {0}", filePath);
                    return;
                }

                Logger.InfoFormat("Moving file to processing folder: {0} -> {1}", filePath, targetFilePath);
                File.Move(filePath, targetFilePath);

                // todo: send message to process file using targetFilePath in case the name changed

            }
            catch (Exception ex)
            {
                Logger.Error(string.Format("Error processing file: {0}", filePath), ex);
            }
        }

        private void ProcessTestFile(string filePath)
        {
            Logger.InfoFormat("Processing TEST file: {0}", filePath);

            try
            {
                if (IsFileLocked(filePath))
                {
                    Logger.WarnFormat("File access locked, not ready for test: {0}", filePath);
                    return;
                }

                // Change extension as confirmation of file detection
                var targetFilePath = GetSafeTargetFileName(Path.ChangeExtension(filePath, "processed"), Path.GetDirectoryName(filePath));
                File.Copy(filePath, targetFilePath);
                File.Delete(filePath);
            }
            catch (Exception ex)
            {
                Logger.Error(string.Format("Error processing test file: {0}", filePath), ex);
            }
        }

        private void HandleFileError(string filePath, string errorMsg)
        {
            Logger.ErrorFormat("Handling file error: '{0}' :: {1}", errorMsg, filePath);

            try
            {
                if (IsFileLocked(filePath))
                {
                    Logger.WarnFormat("File access locked, not ready for move: {0}", filePath);
                    return;
                }

                var errorFolderPath = GetErrorFolderPath();
                var targetFilePath = GetSafeTargetFileName(filePath, errorFolderPath);
                File.Move(filePath, targetFilePath);
            }
            catch (Exception ex)
            {
                Logger.Error(string.Format("Error handling file error: {0}", filePath), ex);
            }
        }

        private bool IsFileLocked(string filePath)
        {
            FileStream stream = null;

            try
            {
                stream = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.None);
            }
            catch (IOException)
            {
                return true;
            }
            finally
            {
                if (stream != null)
                {
                    stream.Close();
                }
            }

            return false;
        }

        private string GetFolderToWatchPath()
        {
            return GetFolderPathFromSettings("FolderToWatch");
        }

        private string GetProcessingFolderPath()
        {
            return GetFolderPathFromSettings("ProcessingFolder");
        }

        private string GetErrorFolderPath()
        {
            return GetFolderPathFromSettings("ErrorFolder");
        }

        private string GetFolderPathFromSettings(string settingName)
        {
            var folderPath = ConfigurationManager.AppSettings[settingName];

            return ReplaceSpecialFolders(folderPath);
        }

        private void CheckFilePaths()
        {
            Logger.Debug("Checking file paths");

            var folderToWatch = GetFolderToWatchPath();
            if (!Directory.Exists(folderToWatch))
            {
                Logger.InfoFormat("Creating watch folder: {0}", folderToWatch);
                Directory.CreateDirectory(folderToWatch);
            }

            var processingFolder = GetProcessingFolderPath();
            if (!Directory.Exists(processingFolder))
            {
                Logger.InfoFormat("Creating processing folder: {0}", processingFolder);
                Directory.CreateDirectory(processingFolder);
            }

            var errorFolder = GetErrorFolderPath();
            if (!Directory.Exists(errorFolder))
            {
                Logger.InfoFormat("Creating error folder: {0}", errorFolder);
                Directory.CreateDirectory(errorFolder);
            }

            Logger.Debug("Checking file paths completed");
        }

        /// <summary>
        /// If a target folder already contains a file with the same name, append a version number to the end to avoid overwrites or errors
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="targetFolder"></param>
        /// <returns></returns>
        private string GetSafeTargetFileName(string filePath, string targetFolder)
        {
            var fileName = Path.GetFileName(filePath);
            var version = 1;

            if (fileName == null)
            {
                throw new NullReferenceException("Filename in filePath is null");
            }

            while (File.Exists(Path.Combine(targetFolder, fileName)))
            {
                fileName = string.Format("{0}.[{1:000}]{2}", Path.GetFileNameWithoutExtension(filePath), version++, Path.GetExtension(filePath));
            }

            return Path.Combine(targetFolder, fileName);
        }

        private string ReplaceSpecialFolders(string filePath)
        {
            if (filePath.Contains(DesktopFolder))
            {
                filePath = filePath.Replace(DesktopFolder, Environment.GetFolderPath(Environment.SpecialFolder.Desktop));
            }

            if (filePath.Contains(LocalApplicationData))
            {
                filePath = filePath.Replace(LocalApplicationData, Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData));
            }

            if (filePath.Contains(ApplicationData))
            {
                filePath = filePath.Replace(ApplicationData, Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData));
            }

            // todo: add more support for special folders

            return filePath;
        }
    }
}
