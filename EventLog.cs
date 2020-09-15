using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OIMNTFS_Service
{
    class EventLog
    {
        private System.Diagnostics.EventLog eventLog;
        private string logName = "OIMNTFSServiceLog";
        private int eventID = 1;
        private string entry;

        public EventLog(string source)
        {
            eventLog = new System.Diagnostics.EventLog(logName);
            if (!System.Diagnostics.EventLog.SourceExists(source))
            {
                System.Diagnostics.EventLog.CreateEventSource(source, logName);
            }
            eventLog.Source = source;
            eventLog.Log = logName;

            entry = "";
            Write("Log {0} started.", source);

        }
        public void Write(string format, params object[] args)
        {
            Flush();

            var message = "\n>>> " + (args.Length == 0 ? format : string.Format(format, args)) + "\n";
            eventLog.WriteEntry(message, System.Diagnostics.EventLogEntryType.Information, eventID++);
        }
        public void Buffer(string format, params object[] args)
        {
            entry += DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss") + " - " + (args.Length == 0 ? format : string.Format(format, args)) + "\n";
        }
        public void Flush()
        {
            if (entry.Length > 0)
            {
                eventLog.WriteEntry(entry, System.Diagnostics.EventLogEntryType.Information, eventID++);
                entry = "";
            }
        }
    }
}
