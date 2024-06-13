using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventLogger
{
    public static class EventLoggerApp
    {
        public static List<string> events = new List<string>();

        public  static void LogEvent(string messageType, string messageLog, string messageStactTrace)
        {
            events.Add($"{DateTime.Now}: {" Type:"} {messageType }{" - Event: " + messageLog}{messageStactTrace}");
        }

        public static string GetEventLog()
        {
            return string.Join(Environment.NewLine, events);
        }
    }
}
