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
                    var contact = GetContact(queueEntry);
                    Console.WriteLine($"ContactSubmitQueueEntry found, contact id: {contact.ContactId}, IsNew: {contact.IsNew}, NeedToBeRecreated: {GetNeedToBeRecreated(contact)}");
                }

                if (entry is SessionSubmitQueueEntry)
                {
                    Console.WriteLine("SessionSubmitQueueEntry found");
                }
            }

            Console.WriteLine("Data reading is finished");
            Console.ReadKey();
        }

        private static Contact GetContact(ContactSubmitQueueEntry entry)
        {
            var contact =
                entry.GetType()
                    .GetField("_contact", BindingFlags.GetField | BindingFlags.Instance | BindingFlags.NonPublic)
                    .GetValue(entry) as Contact;
            return contact;
        }

        private static bool GetNeedToBeRecreated(Contact contactContext)
        {
            var contact = ((ContactContext)contactContext).Contact;
            if (!contact.Extensions.Groups.Contains("NeedToBeRecreated") || !contact.Extensions.Groups["NeedToBeRecreated"].Entries.Contains("NeedToBeRecreated") || !bool.TryParse(contact.Extensions.Groups["NeedToBeRecreated"].Entries["NeedToBeRecreated"].Value, out bool result))
            {
                return false;
            }
            return result;
        }
    }
}