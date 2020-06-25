using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text.RegularExpressions;
using Sitecore.Analytics.Data.DataAccess.SubmitQueue;
using Sitecore.Data.DataProviders;
using Sitecore.Framework.Conditions;

namespace ReadSubmitQueue
{
    /// <summary>
    /// Modified code from IL Spy
    /// </summary>
    public class FileSubmitQueue
    {
        private static readonly Retryer Retryer = new Retryer(TimeSpan.FromMilliseconds(10.0), 100);

        private static readonly BinaryFormatter Formatter = new BinaryFormatter();

        private static readonly Regex FileNameRegex = new Regex("^[0-9]+$");

        private readonly string _folderPath;

        public FileSubmitQueue(string absoluteFolderPath)
        {
            Condition.Requires(absoluteFolderPath, "absoluteFolderPath").IsNotNullOrEmpty();
            _folderPath = absoluteFolderPath;
        }

        public long ReadNumberOfRecordsAndResetCursor()
        {
            using (FileStream fileStream = Retryer.Execute(GetDequeueingStream))
            {
                if (fileStream == null)
                {
                    return 0;
                }

                fileStream.Position = 0L;
                var entryCount = (long)Formatter.Deserialize(fileStream);
                var currentReadPosition = (long)Formatter.Deserialize(fileStream);
                WriteHeader(fileStream, entryCount, fileStream.Position);

                return entryCount;
            }
        }

        public SubmitQueueEntry Dequeue()
        {
            using (FileStream fileStream = Retryer.Execute(GetDequeueingStream))
            {
                if (fileStream == null)
                {
                    return null;
                }
                ReadHeader(fileStream, out long entryCount, out long _);
                SubmitQueueEntry result = (SubmitQueueEntry)Formatter.Deserialize(fileStream);
                WriteHeader(fileStream, entryCount, fileStream.Position);
                return result;
            }
        }

        private static void WriteHeader(FileStream fileStream, long entryCount, long currentReadPosition)
        {
            Condition.Requires(fileStream, "fileStream").IsNotNull();
            long position = fileStream.Position;
            fileStream.Seek(0L, SeekOrigin.Begin);
            Formatter.Serialize(fileStream, entryCount);
            Formatter.Serialize(fileStream, currentReadPosition);
            if (position != 0)
            {
                fileStream.Seek(position, SeekOrigin.Begin);
            }
        }

        private FileStream GetDequeueingStream()
        {
            Directory.CreateDirectory(_folderPath);
            string dequeueingFilePath = GetDequeueingFilePath();
            if (string.IsNullOrEmpty(dequeueingFilePath))
            {
                return null;
            }
            FileStream fileStream = null;
            try
            {
                fileStream = File.Open(dequeueingFilePath, FileMode.Open, FileAccess.ReadWrite);
                ReadHeader(fileStream, out long _, out long currentReadPosition);
                if (currentReadPosition != 0)
                {
                    fileStream.Position = currentReadPosition;
                }
                if (fileStream.Position >= fileStream.Length)
                {
                    fileStream.Position = 0L;
                    var entryCount = (long)Formatter.Deserialize(fileStream);
                    var _ = (long)Formatter.Deserialize(fileStream);
                    WriteHeader(fileStream, entryCount, fileStream.Position);
                    fileStream.Dispose();
                    fileStream = null;
                    return null;
                }
                return fileStream;
            }
            catch
            {
                fileStream?.Dispose();
                throw;
            }
        }

        private void ReadHeader(FileStream fileStream, out long entryCount, out long currentReadPosition)
        {
            Condition.Requires(fileStream, "fileStream").IsNotNull();
            long position = fileStream.Position;
            fileStream.Position = 0L;
            entryCount = (long)Formatter.Deserialize(fileStream);
            currentReadPosition = (long)Formatter.Deserialize(fileStream);
            if (position != 0)
            {
                fileStream.Seek(position, SeekOrigin.Begin);
            }
        }

        private string GetDequeueingFilePath()
        {
            IEnumerable<string> enumerable = Directory.EnumerateFiles(_folderPath);
            string text = null;
            foreach (string item in enumerable)
            {
                string fileName = Path.GetFileName(item);
                if (fileName != null && FileNameRegex.IsMatch(fileName) && (text == null || string.Compare(item, text, CultureInfo.InvariantCulture, CompareOptions.IgnoreCase) < 0))
                {
                    text = item;
                }
            }
            return text;
        }
    }
}
