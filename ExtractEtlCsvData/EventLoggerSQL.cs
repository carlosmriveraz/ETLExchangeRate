namespace EventLoggerETL
{
    using MySql.Data.MySqlClient;
    using System;

    /// <summary>
    /// Defines the <see cref="EventLogger" />
    /// </summary>
    public class EventLoggerSQL
    {

        /// <summary>
        /// The LogEvent
        /// </summary>
        /// <param name="messageType">The messageType Ok or ERROR<see cref="string"/></param>
        /// <param name="logMessage">The logMessage<see cref="string"/></param>
        /// <param name="stackTrace">The stackTrace<see cref="string"/></param>
        /// <returns>The <see cref="string"/></returns>
        public  static string LogEvent(string messageType, string logMessage, string stackTrace="")
        {
                    string connectionString = "Server = sql10.freesqldatabase.com;Database = sql10708775;User ID = sql10708775;Password = CRL7W3yvqF;Port = 3306;";
                   // string connectionString = ConfiguracionApp.ConfiguracionApp.;
            try
            {
                using (MySqlConnection conn = new MySqlConnection(connectionString))
                {
                    conn.Open();

                    string query = "INSERT INTO event_log (timestamp,messaje_type,log_message, stack_trace) VALUES (@timestamp,@MessageType,@logMessage, @stackTrace)";

                    MySqlCommand cmd = new MySqlCommand(query, conn);
                    cmd.Parameters.AddWithValue("@timestamp", DateTime.Now);
                    cmd.Parameters.AddWithValue("@MessageType", messageType);
                    cmd.Parameters.AddWithValue("@logMessage", logMessage);
                    cmd.Parameters.AddWithValue("@stackTrace", stackTrace);

                    cmd.ExecuteNonQuery();

                    return "OK";
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());   
                return "Error: " + ex.Message;
            }
        }
    }
}
