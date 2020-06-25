using System;
using System.Reflection;
using Sitecore.Analytics.Data.DataAccess.SubmitQueue;
using Sitecore.Analytics.Tracking;

namespace ReadSubmitQueue
{
    internal class Program
    {
        private static void Main()
        {
            Console.WriteLine("Enter absolute path to folder with the submit queue files");
            var path = Console.ReadLine();

            var submitQueue = new FileSubmitQueue(path);
            var numberOfRecords = submitQueue.ReadNumberOfRecordsAndResetCursor();
            
            Console.WriteLine("Starting the data reading.");
            Console.WriteLine($"Number of records: {numberOfRecords}");

            SubmitQueueEntry entry;

            while ((entry = submitQueue.Dequeue()) != null)
            {
                if (entry is ContactSubmitQueueEntry queueEntry)
                {
                    Console.WriteLine($"ContactSubmitQueueEntry found, contact id: {GetContactId(queueEntry)}");
                }

                if (entry is SessionSubmitQueueEntry)
                {
                    Console.WriteLine("SessionSubmitQueueEntry found");
                }
            }

            Console.WriteLine("Data reading is finished");
            Console.ReadKey();
        }

        private static Guid GetContactId(ContactSubmitQueueEntry entry)
        {
            var contact =
                entry.GetType()
                    .GetField("_contact", BindingFlags.GetField | BindingFlags.Instance | BindingFlags.NonPublic)
                    .GetValue(entry) as Contact;
            return contact.ContactId;
        }
    }
}