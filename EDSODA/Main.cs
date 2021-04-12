/*
    EDSODA - Elite Dangerous Ships Onboard Data Acquisition
    Copyright (C) 2021  David McMurray

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security;
using System.Windows.Forms;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace EDSODA
{
    public partial class Main : Form
    {
        private string connStr = string.Empty;
        private string journalPath = string.Empty;
        private string reprocessEvents = string.Empty;
        private DirectoryInfo journalDirInfo = null;
        private FileInfo curActiveFile = null;
        private FileInfo lastActiveFile = null;
        private FileInfo newActiveFile = null;
        private BackgroundWorker bgwSync = null;
        private BackgroundWorker bgwActive = null;

        /// <summary>
        /// Main
        /// </summary>
        public Main()
        {
            InitializeComponent();

            this.connStr = ConfigurationManager.ConnectionStrings["EDSODS"].ToString();
            this.journalPath = ConfigurationManager.AppSettings["journalPath"].ToString();
            this.journalDirInfo = new DirectoryInfo(this.journalPath);
            this.reprocessEvents = ConfigurationManager.AppSettings["ReprocessEvents"].ToString();

            BGWActive_Configure();
            BGWSync_Configure();

            // Reprocess events if specified in application configuration
            if (this.reprocessEvents.Length > 0)
            {
                string oldText = btnControl.Text;
                btnControl.Enabled = false;
                btnControl.Text = "Reprocessing Events...";
                ReprocessEvents();
                btnControl.Text = oldText;
                btnControl.Enabled = true;
            }
        }

        /// <summary>
        /// Configure the active background worker process.
        /// </summary>
        private void BGWActive_Configure()
        {
            bgwActive = new BackgroundWorker
            {
                WorkerReportsProgress = false,
                WorkerSupportsCancellation = true,
            };
            bgwActive.DoWork += BGWActive_DoWork;
            bgwActive.RunWorkerCompleted += BGWActive_RunWorkerCompleted;
        }

        /// <summary>
        /// Start the active and sync background worker process.
        /// </summary>
        private void BGW_Start()
        {
            if (!bgwActive.IsBusy)
                bgwActive.RunWorkerAsync();
            else
                LogMessage("BGWActive_Start: Active process cannot start as it is already running.");

            if (!bgwSync.IsBusy)
                bgwSync.RunWorkerAsync();
            else
                LogMessage("BGWSync_Start: Sync process cannot start as it is already running.");

            if (bgwActive.IsBusy || bgwSync.IsBusy)
            {
                btnControl.Text = "Stop";
                btnControl.Enabled = true;
            }
        }

        /// <summary>
        /// Stop the active amd sync background worker process.
        /// </summary>
        private void BGW_Stop()
        {
            if (bgwActive.IsBusy)
                bgwActive.CancelAsync();
            else
                LogMessage("BGWActive_Stop: Active process cannot stop as it is already stopped.");

            if (bgwSync.IsBusy)
                bgwSync.CancelAsync();
            else
                LogMessage("BGWActive_Stop: Sync process cannot stop as it is already stopped.");
        }

        /// <summary>
        /// Active background worker process.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void BGWActive_DoWork(object sender, DoWorkEventArgs e)
        {
            bool finished = false;
            System.IO.FileStream fs = null;
            System.IO.StreamReader sr = null;
            int linesImported = 0;
            int lineCount = 0;
            string line;
            this.curActiveFile = null;
            this.lastActiveFile = null;
            this.newActiveFile = null;

            while (!finished)
            {
                // If no current journal file (i.e. not started or just finished), grab latest journal file
                LocateLatestJournalFile();
                if (this.curActiveFile == null && this.newActiveFile != null)
                {
                    this.curActiveFile = this.newActiveFile;
                    this.lastActiveFile = this.newActiveFile;
                    this.newActiveFile = null;

                    linesImported = CreateAndGetJournalRecord(this.curActiveFile);
                    lineCount = 0;

                    fs = new FileStream(this.curActiveFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                    sr = new StreamReader(fs);
                }

                // Open file and read a line
                if (sr != null)
                {
                    line = sr.ReadLine();
                    if (line == null)
                    {
                        // End of file, check for new file
                        if (this.newActiveFile != null)
                        {
                            // There is a new file so complete this one and then clear the current file pointer
                            UpdateJournalRecord(this.curActiveFile.Name, linesImported, true);
                            if (sr != null)
                                sr.Close();
                            if (fs != null)
                                fs.Close();
                            this.curActiveFile = null;
                        }
                    }
                    else
                    {
                        lineCount++;
                        if (lineCount > linesImported)
                        {
                            // Line has not been read before
                            linesImported = lineCount;
                            // Parse imported line
                            dynamic eventLine = JsonToObject(line);
                            // Create event and related records
                            int eventId = GetEventId(this.curActiveFile.Name, linesImported);
                            if (eventId == 0 && CreateEventRecord(eventLine, line, this.curActiveFile.Name, linesImported))
                            {
                                eventId = GetEventId(this.curActiveFile.Name, linesImported);
                            }
                            CreateEventTypeRecord(eventId, eventLine);
                            // Update the journal file as we progress line by line
                            UpdateJournalRecord(this.curActiveFile.Name, linesImported, false);
                        }
                    }
                }

                // Check for cancelling process
                if (bgwActive.CancellationPending)
                {
                    e.Cancel = true;
                    finished = true;
                    if (sr != null)
                        sr.Close();
                    if (fs != null)
                        fs.Close();
                }
            }
        }

        /// <summary>
        /// Active background worker process cancelled or failed.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void BGWActive_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            if (e.Cancelled)
            {
                btnControl.Text = "Start";
                btnControl.Enabled = true;
                LogMessage("Active background worker process cancelled");
            }
            else if (e.Error != null)
                LogMessage(String.Format("Active background worker process failed with: {0}", e.Error.Message));
            else
                LogMessage("Active background worker process completed somehow");
        }

        /// <summary>
        /// Configure the sync background worker process.
        /// </summary>
        private void BGWSync_Configure()
        {
            bgwSync = new BackgroundWorker
            {
                WorkerReportsProgress = false,
                WorkerSupportsCancellation = true,
            };
            bgwSync.DoWork += BGWSync_DoWork;
            bgwSync.RunWorkerCompleted += BGWSync_RunWorkerCompleted;
        }

        /// <summary>
        /// Synchronises the journal files with the stored journal information.
        /// </summary>
        private void BGWSync_DoWork(object sender, DoWorkEventArgs e)
        {
            DataTable journalsImported = GetImportedJournals();
            FileInfo[] journalFiles = this.journalDirInfo.GetFiles("Journal.*.log");

            foreach (FileInfo file in journalFiles.OrderBy(f => f.CreationTime).Take(journalFiles.Length - 1))
            {
                IEnumerable<DataRow> ImportedFile = journalsImported.AsEnumerable()
                    .Where(f => f.Field<string>("Filename") == file.Name);
                if (ImportedFile.Count() == 0)
                {
                    // Journal file not in the database, so import it.
                    ImportNewJournalFile(file);
                }
                else if (ImportedFile.Count() == 1 && ImportedFile.First<DataRow>().Field<string>("completed") == "N")
                {
                    // Journal file is in the database, but not completed, so check it.
                    ImportJournalFile(file);
                }
                // Only other possibilities:
                // 1) The filename appears more than once, but this cannot happen as the filename is the primary key.
                // 2) The file is present and completed, and therefore we can ignore it completely.
                if (bgwSync.CancellationPending)
                {
                    e.Cancel = true;
                    return;
                }
            }
        }

        /// <summary>
        /// Journal file synchronisation cancelled, failed or completed.
        /// </summary>
        /// <param name="sender">Sender object</param>
        /// <param name="e">Completed process data</param>
        private void BGWSync_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            if (e.Cancelled)
                LogMessage("Sync background worker process cancelled");
            else if (e.Error != null)
                LogMessage(String.Format("Sync background worker process failed with: {0}", e.Error.Message));
            else
                LogMessage("Sync background worker process completed");
        }

        /// <summary>
        /// Retrieves all imported journal information from the database.
        /// </summary>
        /// <returns></returns>
        private DataTable GetImportedJournals()
        {
            DataTable journalInfo = new DataTable();

            using (MySqlConnection mysqlCnn = new MySqlConnection(connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "SELECT filename, date_created, lines_imported, completed FROM journal";
                    using (MySqlDataAdapter mysqlDAd = new MySqlDataAdapter(mysqlCmd))
                    {
                        mysqlDAd.Fill(journalInfo);
                    }
                }
            }

            return journalInfo;
        }

        /// <summary>
        /// Import a new journal file not imported before.
        /// </summary>
        /// <param name="newJournalFile">New file to be imported</param>
        private void ImportNewJournalFile(FileInfo newJournalFile)
        {
            if (CreateJournalRecord(newJournalFile))
                ImportJournalFile(newJournalFile);
        }

        /// <summary>
        /// Logs a new journal file record if one does not already exist, then retrieves journal file record details.
        /// </summary>
        /// <param name="journalFile">Journal to create and get</param>
        /// <returns>Journal object of record values</returns>
        private int CreateAndGetJournalRecord(FileInfo journalFile)
        {
            if (CreateJournalRecord(journalFile))
                return GetJournalLinesImported(journalFile);
            return 0;
        }

        /// <summary>
        /// Creates a journal record in the database, with no lines imported and not completed.
        /// </summary>
        /// <param name="journalFile">Journal file to create</param>
        /// <returns>Success</returns>
        private bool CreateJournalRecord(FileInfo journalFile)
        {
            bool result = false;

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO journal (filename, date_created, lines_imported, completed) " +
                        "SELECT * FROM (SELECT @filename filename, @date_created date_created, 0 lines_imported, 'N' completed) tmp " +
                        "WHERE NOT EXISTS (SELECT filename FROM journal WHERE filename = @filename) LIMIT 1";
                    // Param: @filename
                    mysqlCmd.Parameters.Add("@filename", MySqlDbType.VarChar, 100);
                    mysqlCmd.Parameters["@filename"].Value = journalFile.Name;
                    // Param: @date_created
                    mysqlCmd.Parameters.Add("@date_created", MySqlDbType.DateTime);
                    mysqlCmd.Parameters["@date_created"].Value = journalFile.CreationTime;

                    try
                    {
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                        result = true;
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateJournalRecord: Exception, {0}", ex.Message));
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Retrieve a journal record by filename.
        /// </summary>
        /// <param name="journalFile">Name of journal file</param>
        /// <returns>Journal object of record values</returns>
        private int GetJournalLinesImported(FileInfo journalFile)
        {
            int result = 0;

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "SELECT lines_imported FROM journal WHERE filename = @filename";
                    // Param: @filename
                    mysqlCmd.Parameters.Add("@filename", MySqlDbType.VarChar, 100);
                    mysqlCmd.Parameters["@filename"].Value = journalFile.Name;

                    using (MySqlDataAdapter mysqlDAd = new MySqlDataAdapter(mysqlCmd))
                    {
                        DataTable journalInfo = new DataTable();
                        mysqlDAd.Fill(journalInfo);
                        if (journalInfo != null && journalInfo.Rows.Count == 1)
                            result = (int)journalInfo.Rows[0]["lines_imported"];
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Imports a single journal file.
        /// </summary>
        /// <param name="journalFile">Journal file to be imported</param>
        private void ImportJournalFile(FileInfo journalFile)
        {
            string line;
            int counter = 0;

            try
            {
                using (System.IO.StreamReader fileStream = new System.IO.StreamReader(journalFile.FullName))
                {
                    while ((line = fileStream.ReadLine()) != null)
                    {
                        // Parse line and count
                        dynamic eventLine = JsonToObject(line);
                        counter++;
                        // Create records
                        if (CreateEventRecord(eventLine, line, journalFile.Name, counter))
                        {
                            int eventId = GetEventId(journalFile.Name, counter);
                            CreateEventTypeRecord(eventId, eventLine);
                        }
                    }
                    UpdateJournalRecord(journalFile.Name, counter, true);
                }
            }
            catch (Exception ex)
            {
                LogMessage(String.Format("ImportJournalFile: Exception, {0}", ex.Message));
            }
        }

        /// <summary>
        /// Update the lines imported and completed flag in the journal record.
        /// </summary>
        /// <param name="filename">Journal filename</param>
        /// <param name="counter">Number of lines imported</param>
        /// <param name="completed">File fully imported flag</param>
        private bool UpdateJournalRecord(string filename, int linesImported, bool completed)
        {
            bool result = false;

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "UPDATE journal SET lines_imported = @lines_imported, completed = @completed WHERE filename = @filename";
                    // Param: @lines_imported
                    mysqlCmd.Parameters.Add("@lines_imported", MySqlDbType.Int32);
                    mysqlCmd.Parameters["@lines_imported"].Value = linesImported;
                    // Param: @completed
                    mysqlCmd.Parameters.Add("@completed", MySqlDbType.VarChar, 1);
                    mysqlCmd.Parameters["@completed"].Value = (completed) ? "Y" : "N";
                    // Param: @filename
                    mysqlCmd.Parameters.Add("@filename", MySqlDbType.VarChar, 100);
                    mysqlCmd.Parameters["@filename"].Value = filename;

                    try
                    {
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                        result = true;
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("UpdateJournalRecord: Exception, {0}", ex.Message));
                        throw new Exception("UpdateJournalRecord:Exception", ex);
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Creates an event record in the database.
        /// </summary>
        /// <param name="eventLine">Parsed JSON object of the event line</param>
        /// <param name="line">Actual string from the journal file</param>
        /// <param name="filename">Journal filename</param>
        /// <param name="lineNumber">Line number in the journal file</param>
        /// <returns>Success</returns>
        private bool CreateEventRecord(dynamic eventLine, string line, string filename, int lineNumber)
        {
            bool result = false;

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT IGNORE INTO event (timestamp, type, data, filename, line, parsed) VALUES (@timestamp, @type, @data, @filename, @line, @parsed)";
                    // Param: @timestamp
                    mysqlCmd.Parameters.Add("@timestamp", MySqlDbType.DateTime);
                    mysqlCmd.Parameters["@timestamp"].Value = GetTimestamp(eventLine);
                    // Param: @type
                    mysqlCmd.Parameters.Add("@type", MySqlDbType.VarChar, 100);
                    mysqlCmd.Parameters["@type"].Value = eventLine["event"].ToString();
                    // Param: @data
                    mysqlCmd.Parameters.Add("@data", MySqlDbType.Text);
                    mysqlCmd.Parameters["@data"].Value = line;
                    // Param: @filename
                    mysqlCmd.Parameters.Add("@filename", MySqlDbType.VarChar, 100);
                    mysqlCmd.Parameters["@filename"].Value = filename;
                    // Param: @line
                    mysqlCmd.Parameters.Add("@line", MySqlDbType.Int32);
                    mysqlCmd.Parameters["@line"].Value = lineNumber;
                    // Param: @parsed
                    mysqlCmd.Parameters.Add("@parsed", MySqlDbType.VarChar, 1);
                    mysqlCmd.Parameters["@parsed"].Value = "N";

                    try
                    {
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                        result = true;
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEventRecord: Exception, {0}", ex.Message));
                        throw new Exception("CreateEventRecord:Exception", ex);
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Return the event ID for a specific journal file and line number
        /// </summary>
        /// <param name="filename">Journal filename</param>
        /// <param name="lineNumber">Line number in journal file</param>
        /// <returns>Event ID</returns>
        private int GetEventId(string filename, int lineNumber)
        {
            int result = 0;

            using (MySqlConnection mysqlCnn = new MySqlConnection(connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "SELECT id FROM event WHERE filename = @filename AND line = @line";
                    // Param: @filename
                    mysqlCmd.Parameters.Add("@filename", MySqlDbType.VarChar, 100);
                    mysqlCmd.Parameters["@filename"].Value = filename;
                    // Param: @line
                    mysqlCmd.Parameters.Add("@line", MySqlDbType.Int32);
                    mysqlCmd.Parameters["@line"].Value = lineNumber;

                    using (MySqlDataAdapter mysqlDAd = new MySqlDataAdapter(mysqlCmd))
                    {
                        DataTable eventId = new DataTable();
                        try
                        {
                            mysqlDAd.Fill(eventId);
                            if (eventId.Rows.Count == 1)
                                result = eventId.Rows[0].Field<Int32>("id");
                        }
                        catch (Exception ex)
                        {
                            LogMessage(String.Format("GetEventId: Exception, {0}", ex.Message));
                            throw new Exception("GetEventId:Exception", ex);
                        }
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Checks if an event record exists for a given event ID in a given table.
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="tableName">Name of table to check</param>
        /// <returns>True if a record exists, otherwise false</returns>
        private bool EventTypeRecordExists(int eventId, string tableName)
        {
            bool result = true;

            DataTable eventInfo = new DataTable();

            using (MySqlConnection mysqlCnn = new MySqlConnection(connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = String.Format("SELECT COUNT(1) FROM {0} WHERE event_id = @event_id", tableName);
                    // Param: @event_id
                    mysqlCmd.Parameters.Add("@event_id", MySqlDbType.Int32);
                    mysqlCmd.Parameters["@event_id"].Value = eventId;

                    using (MySqlDataAdapter mysqlDAd = new MySqlDataAdapter(mysqlCmd))
                    {
                        mysqlDAd.Fill(eventInfo);
                        result = eventInfo.Rows[0][0].ToString() != "0";
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Creates a specific event type record
        /// </summary>
        /// <param name="eventId">Global event ID</param>
        /// <param name="eventLine">Imported journal event</param>
        /// <returns>Success</returns>
        private void CreateEventTypeRecord(int eventId, dynamic eventData)
        {
            switch (eventData["event"].ToString())
            {
                case "AfmuRepairs":
                    CreateEvent_AfmuRepairs(eventId, eventData);
                    break;
                case "ApproachBody":
                    CreateEvent_ApproachBody(eventId, eventData);
                    break;
                case "ApproachSettlement":
                    CreateEvent_ApproachSettlement(eventId, eventData);
                    break;
                case "AsteroidCracked":
                    CreateEvent_AsteroidCracked(eventId, eventData);
                    break;
                case "Bounty":
                    CreateEvent_Bounty(eventId, eventData);
                    break;
                case "BuyAmmo":
                    CreateEvent_BuyAmmo(eventId, eventData);
                    break;
                case "Commander":
                    CreateEvent_Commander(eventId, eventData);
                    break;
                case "EngineerProgress":
                    CreateEvent_EngineerProgress(eventId, eventData);
                    break;
                case "FSDJump":
                    CreateEvent_FSDJump(eventId, eventData);
                    break;
                case "FSDTarget":
                    CreateEvent_FSDTarget(eventId, eventData);
                    break;
                case "FSSDiscoveryScan":
                    CreateEvent_FSSDiscoveryScan(eventId, eventData);
                    break;
                case "LoadGame":
                    CreateEvent_LoadGame(eventId, eventData);
                    break;
                case "Location":
                    CreateEvent_Location(eventId, eventData);
                    break;
                case "Materials":
                    CreateEvent_Materials(eventId, eventData);
                    break;
                case "Progress":
                    CreateEvent_Progress(eventId, eventData);
                    break;
                case "Rank":
                    CreateEvent_Rank(eventId, eventData);
                    break;
                case "ReceiveText":
                    CreateEvent_ReceiveText(eventId, eventData);
                    break;
                case "Reputation":
                    CreateEvent_Reputation(eventId, eventData);
                    break;
                case "Scan":
                    CreateEvent_Scan(eventId, eventData);
                    break;
                case "StartJump":
                    CreateEvent_StartJump(eventId, eventData);
                    break;
            }
            FlagEventParsed(eventId);
        }

        /// <summary>
        /// Flags an event record as parsed
        /// </summary>
        /// <param name="eventId">ID of event to flag as parsed</param>
        private void FlagEventParsed(int eventId)
        {
            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "UPDATE event SET parsed = 'Y' WHERE id = @id";
                    // Param: @id
                    mysqlCmd.Parameters.Add("@id", MySqlDbType.Int32);
                    mysqlCmd.Parameters["@id"].Value = eventId;

                    try
                    {
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("FlagEventParsed: Exception, {0}", ex.Message));
                        throw new Exception("FlagEventParsed:Exception", ex);
                    }
                }
            }
        }

        /// <summary>
        /// Creates an AFMU repairs record.
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventLine">JSON event data</param>
        private void CreateEvent_AfmuRepairs(int eventId, dynamic eventData)
        {
            if (EventTypeRecordExists(eventId, "event_AfmuRepairs")) { return; }

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_AfmuRepairs (event_id, event_timestamp, Module, Module_Localised, FullyRepaired, Health) " +
                        "VALUES (@event_id, @event_timestamp, @Module, @Module_Localised, @FullyRepaired, @Health)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParam_EventTimestamp(eventData));
                        mysqlCmd.Parameters.Add(NewParam("@Module", eventData["Module"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Module_Localised", eventData["Module_Localised"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@FullyRepaired", eventData["FullyRepaired"], MySqlDbType.VarChar, 1));
                        mysqlCmd.Parameters.Add(NewParam("@Health", eventData["Health"], MySqlDbType.Decimal, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_AfmuRepairs: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Creates an approach body record.
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventLine">JSON event data</param>
        private void CreateEvent_ApproachBody(int eventId, dynamic eventData)
        {
            if (EventTypeRecordExists(eventId, "event_ApproachBody")) { return; }

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_ApproachBody (event_id, event_timestamp, StarSystem, SystemAddress, Body, BodyID) " +
                        "VALUES (@event_id, @event_timestamp, @StarSystem, @SystemAddress, @Body, @BodyID)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParam_EventTimestamp(eventData));
                        mysqlCmd.Parameters.Add(NewParam("@StarSystem", eventData["StarSystem"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemAddress", eventData["SystemAddress"], MySqlDbType.Int64, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Body", eventData["Body"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@BodyID", eventData["BodyID"], MySqlDbType.Int32, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_ApproachBody: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Creates an approach settlement record.
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventLine">JSON event data</param>
        private void CreateEvent_ApproachSettlement(int eventId, dynamic eventData)
        {
            if (EventTypeRecordExists(eventId, "event_ApproachSettlement")) { return; }

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_ApproachSettlement (event_id, event_timestamp, Name, MarketID, SystemAddress, BodyID, BodyName, Latitude, Longitude) " +
                        "VALUES (@event_id, @event_timestamp, @Name, @MarketID, @SystemAddress, @BodyID, @BodyName, @Latitude, @Longitude)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParam_EventTimestamp(eventData));
                        mysqlCmd.Parameters.Add(NewParam("@Name", eventData["Name"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@MarketID", eventData["MarketID"], MySqlDbType.Int64, 0));
                        mysqlCmd.Parameters.Add(NewParam("@SystemAddress", eventData["SystemAddress"], MySqlDbType.Int64, 0));
                        mysqlCmd.Parameters.Add(NewParam("@BodyID", eventData["BodyID"], MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@BodyName", eventData["BodyName"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Latitude", eventData["Latitude"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Longitude", eventData["Longitude"], MySqlDbType.Decimal, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_ApproachSettlement: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Creates an asteroid cracked record.
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventLine">JSON event data</param>
        private void CreateEvent_AsteroidCracked(int eventId, dynamic eventData)
        {
            if (EventTypeRecordExists(eventId, "event_AsteroidCracked")) { return; }

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_AsteroidCracked (event_id, event_timestamp, Body) " +
                        "VALUES (@event_id, @event_timestamp, @Body)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParam_EventTimestamp(eventData));
                        mysqlCmd.Parameters.Add(NewParam("@Body", eventData["Body"], MySqlDbType.VarChar, 100));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_AsteroidCracked: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Creates a bounty record.
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventLine">JSON event data</param>
        private void CreateEvent_Bounty(int eventId, dynamic eventData)
        {
            if (EventTypeRecordExists(eventId, "event_Bounty")) { return; }

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_Bounty (event_id, event_timestamp, Target, TotalReward, " +
                        "VictimFaction, SharedWithOthers, Faction, Reward)" +
                        "VALUES (@event_id, @event_timestamp, @Target, @TotalReward, @VictimFaction, @SharedWithOthers, " +
                        "@Faction, @Reward)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParam_EventTimestamp(eventData));
                        mysqlCmd.Parameters.Add(NewParam("@Target", eventData["Target"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@TotalReward", eventData["TotalReward"], MySqlDbType.Int64, 0));
                        mysqlCmd.Parameters.Add(NewParam("@VictimFaction", eventData["VictimFaction"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SharedWithOthers", eventData["SharedWithOthers"], MySqlDbType.Int64, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Faction", eventData["Faction"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Reward", eventData["Reward"], MySqlDbType.Int64, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                        // Rewards
                        if (eventData["Rewards"] != null && eventData["Rewards"].Type == JTokenType.Array)
                            for (int idx = 0; idx < eventData["Rewards"].Count; idx++)
                            {
                                dynamic reward = eventData["Rewards"][idx];
                                CreateEvent_Bounty_Rewards(eventId, idx, reward);
                            }
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_Bounty: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Creates a bounty reward record.
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventLine">JSON event data</param>
        private void CreateEvent_Bounty_Rewards(int eventId, int idx, dynamic reward)
        {
            // TODO: if (EventTypeRecordExists(eventId, "event_Bounty_Reward")) { return; }
            //       Ok, going to ignore errors for now, but we need a function to check sub-event records with more than the event_id as the primary key!

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT IGNORE INTO event_Bounty_Rewards (event_id, idx, Faction, Reward) " +
                        "VALUES (@event_id, @idx, @Faction, @Reward)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParamStr("@idx", idx.ToString(), MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Faction", reward["Faction"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Reward", reward["Reward"], MySqlDbType.Int64, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_Bounty_Rewards: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Creates a buy ammo record.
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventLine">JSON event data</param>
        private void CreateEvent_BuyAmmo(int eventId, dynamic eventData)
        {
            if (EventTypeRecordExists(eventId, "event_BuyAmmo")) { return; }

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_BuyAmmo (event_id, event_timestamp, Cost) " +
                        "VALUES (@event_id, @event_timestamp, @Cost)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParam_EventTimestamp(eventData));
                        mysqlCmd.Parameters.Add(NewParam("@Cost", eventData["Cost"], MySqlDbType.Int64, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_BuyAmmo: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Creates a commander record.
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventLine">JSON event data</param>
        private void CreateEvent_Commander(int eventId, dynamic eventData)
        {
            if (EventTypeRecordExists(eventId, "event_Commander")) { return; }

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_Commander (event_id, event_timestamp, FID, Name) " +
                        "VALUES (@event_id, @event_timestamp, @FID, @Name)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParam_EventTimestamp(eventData));
                        mysqlCmd.Parameters.Add(NewParam("@FID", eventData["FID"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Name", eventData["Name"], MySqlDbType.VarChar, 100));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_Commander: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Create an engineer progress event record
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventData">JSON event data</param>
        private void CreateEvent_EngineerProgress(int eventId, dynamic eventData)
        {
            //if (EventTypeRecordExists(eventId, "event_EngineerProgress")) { return; } - Primary key is event_id and EngineerID

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                mysqlCnn.Open();
                if (eventData["Engineers"] != null && eventData["Engineers"].Type == JTokenType.Array)
                {
                    foreach (dynamic engineer in eventData["Engineers"])
                    {
                        using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                        {
                            mysqlCmd.CommandType = CommandType.Text;
                            mysqlCmd.CommandText = "INSERT INTO event_EngineerProgress (event_id, event_timestamp, " +
                                "Engineer, EngineerID, Progress, RankProgress, Rank) " +
                                "VALUES (@event_id, @event_timestamp, @Engineer, @EngineerID, @Progress, @RankProgress, @Rank)";
                            try
                            {
                                mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                                mysqlCmd.Parameters.Add(NewParam_EventTimestamp(eventData));
                                mysqlCmd.Parameters.Add(NewParam("@Engineer", engineer["Engineer"], MySqlDbType.VarChar, 100));
                                mysqlCmd.Parameters.Add(NewParam("@EngineerID", engineer["EngineerID"], MySqlDbType.Int32, 0));
                                mysqlCmd.Parameters.Add(NewParam("@Progress", engineer["Progress"], MySqlDbType.VarChar, 100));
                                mysqlCmd.Parameters.Add(NewParam("@RankProgress", engineer["RankProgress"], MySqlDbType.Int32, 0));
                                mysqlCmd.Parameters.Add(NewParam("@Rank", engineer["Rank"], MySqlDbType.Int32, 0));
                                mysqlCmd.ExecuteNonQuery();
                            }
                            catch (Exception ex)
                            {
                                LogMessage(String.Format("CreateEvent_EngineerProgress: Exception, {0}", ex.Message), eventId);
                            }
                        }
                    }
                }
                else if (eventData["Engineer"] != null)
                {
                    using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                    {
                        mysqlCmd.CommandType = CommandType.Text;
                        mysqlCmd.CommandText = "INSERT INTO event_EngineerProgress (event_id, event_timestamp, " +
                            "Engineer, EngineerID, Progress, RankProgress, Rank) " +
                            "VALUES (@event_id, @event_timestamp, @Engineer, @EngineerID, @Progress, @RankProgress, @Rank)";
                        try
                        {
                            mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                            mysqlCmd.Parameters.Add(NewParam_EventTimestamp(eventData));
                            mysqlCmd.Parameters.Add(NewParam("@Engineer", eventData["Engineer"], MySqlDbType.VarChar, 100));
                            mysqlCmd.Parameters.Add(NewParam("@EngineerID", eventData["EngineerID"], MySqlDbType.Int32, 0));
                            mysqlCmd.Parameters.Add(NewParam("@Progress", eventData["Progress"], MySqlDbType.VarChar, 100));
                            mysqlCmd.Parameters.Add(NewParam("@RankProgress", eventData["RankProgress"], MySqlDbType.Int32, 0));
                            mysqlCmd.Parameters.Add(NewParam("@Rank", eventData["Rank"], MySqlDbType.Int32, 0));
                            mysqlCmd.ExecuteNonQuery();
                        }
                        catch (Exception ex)
                        {
                            LogMessage(String.Format("CreateEvent_EngineerProgress: Exception, {0}", ex.Message), eventId);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Creates an FSD jump record.
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventLine">JSON event data</param>
        private void CreateEvent_FSDJump(int eventId, dynamic eventData)
        {
            if (EventTypeRecordExists(eventId, "event_FSDJump")) { return; }

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_FSDJump " +
                        "(event_id, event_timestamp, StarSystem, SystemAddress, StarPosX, StarPosY, StarPosZ, SystemAllegiance, SystemEconomy, " +
                        "SystemEconomy_Localised, SystemSecondEconomy, SystemSecondEconomy_Localised, SystemGovernment, SystemGovernment_Localised, " +
                        "SystemSecurity, SystemSecurity_Localised, Population, Body, BodyID, BodyType, JumpDist, FuelUsed, FuelLevel, " +
                        "SystemFaction_Name, SystemFaction_FactionState) " +
                        "VALUES (@event_id, @event_timestamp, @StarSystem, @SystemAddress, @StarPosX, @StarPosY, @StarPosZ, @SystemAllegiance, @SystemEconomy, " +
                        "@SystemEconomy_Localised, @SystemSecondEconomy, @SystemSecondEconomy_Localised, @SystemGovernment, @SystemGovernment_Localised, " +
                        "@SystemSecurity, @SystemSecurity_Localised, @Population, @Body, @BodyID, @BodyType, @JumpDist, @FuelUsed, @FuelLevel, " +
                        "@SystemFaction_Name, @SystemFaction_FactionState)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParam_EventTimestamp(eventData));
                        mysqlCmd.Parameters.Add(NewParam("@StarSystem", eventData["StarSystem"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemAddress", eventData["SystemAddress"], MySqlDbType.Int64, 0));
                        mysqlCmd.Parameters.Add(NewParam("@StarPosX", (eventData["StarPos"] != null) ? eventData["StarPos"][0] : null, MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@StarPosY", (eventData["StarPos"] != null) ? eventData["StarPos"][1] : null, MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@StarPosZ", (eventData["StarPos"] != null) ? eventData["StarPos"][2] : null, MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@SystemAllegiance", eventData["SystemAllegiance"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemEconomy", eventData["SystemEconomy"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemEconomy_Localised", eventData["SystemEconomy_Localised"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemSecondEconomy", eventData["SystemSecondEconomy"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemSecondEconomy_Localised", eventData["SystemSecondEconomy_Localised"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemGovernment", eventData["SystemGovernment"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemGovernment_Localised", eventData["SystemGovernment_Localised"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemSecurity", eventData["SystemSecurity"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemSecurity_Localised", eventData["SystemSecurity_Localised"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Population", eventData["Population"], MySqlDbType.Int64, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Body", eventData["Body"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@BodyID", eventData["BodyID"], MySqlDbType.Int64, 0));
                        mysqlCmd.Parameters.Add(NewParam("@BodyType", eventData["BodyType"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@JumpDist", eventData["JumpDist"], MySqlDbType.Decimal, 100));
                        mysqlCmd.Parameters.Add(NewParam("@FuelUsed", eventData["FuelUsed"], MySqlDbType.Decimal, 100));
                        mysqlCmd.Parameters.Add(NewParam("@FuelLevel", eventData["FuelLevel"], MySqlDbType.Decimal, 100));
                        if (eventData["SystemFaction"] == null)
                        {
                            mysqlCmd.Parameters.Add(NewParam("@SystemFaction_Name", null, MySqlDbType.VarChar, 100));
                            mysqlCmd.Parameters.Add(NewParam("@SystemFaction_FactionState", null, MySqlDbType.VarChar, 100));
                        }
                        else if (eventData["SystemFaction"].Type == JTokenType.String)
                        {
                            mysqlCmd.Parameters.Add(NewParam("@SystemFaction_Name", eventData["SystemFaction"], MySqlDbType.VarChar, 100));
                            mysqlCmd.Parameters.Add(NewParam("@SystemFaction_FactionState", (eventData["FactionState"] != null) ? eventData["FactionState"] : null, MySqlDbType.VarChar, 100));
                        }
                        else
                        {
                            mysqlCmd.Parameters.Add(NewParam("@SystemFaction_Name", eventData["SystemFaction"]["Name"], MySqlDbType.VarChar, 100));
                            mysqlCmd.Parameters.Add(NewParam("@SystemFaction_FactionState", eventData["SystemFaction"]["FactionState"], MySqlDbType.VarChar, 100));
                        }
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                        // Factions
                        if (eventData["Factions"] != null && eventData["Factions"].Type == JTokenType.Array)
                            for (int idx = 0; idx < eventData["Factions"].Count; idx++)
                            {
                                dynamic faction = eventData["Factions"][idx];
                                CreateEvent_FSDJump_Factions(eventId, idx, faction);
                                // Factions Active states
                                if (faction["ActiveStates"] != null && faction["ActiveStates"].Type == JTokenType.Array)
                                    for (int as_idx = 0; as_idx < faction["ActiveStates"].Count; as_idx++)
                                        CreateEvent_FSDJump_Factions_ActiveStates(eventId, idx, as_idx, faction["ActiveStates"][as_idx]);
                                // Factions Pending States
                                if (faction["PendingStates"] != null && faction["PendingStates"].Type == JTokenType.Array)
                                    for (int ps_idx = 0; ps_idx < faction["PendingStates"].Count; ps_idx++)
                                        CreateEvent_FSDJump_Factions_PendingStates(eventId, idx, ps_idx, faction["PendingStates"][ps_idx]);
                                // Factions Recovering states
                                if (faction["RecoveringStates"] != null && faction["RecoveringStates"].Type == JTokenType.Array)
                                    for (int rs_idx = 0; rs_idx < faction["RecoveringStates"].Count; rs_idx++)
                                        CreateEvent_FSDJump_Factions_RecoveringStates(eventId, idx, rs_idx, faction["RecoveringStates"][rs_idx]);
                            }
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_FSDJump: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Creates an FSD jump factions record.
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="idx">faction index</param>
        /// <param name="faction">JSON faction data</param>
        private void CreateEvent_FSDJump_Factions(int eventId, int idx, dynamic faction)
        {
            // TODO: if (EventTypeRecordExists(eventId, "event_FSDJump_Factions")) { return; }
            //       Ok, going to ignore errors for now, but we need a function to check sub-event records with more than the event_id as the primary key!

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT IGNORE INTO event_FSDJump_Factions " +
                        "(event_id, idx, Name, FactionState, Government, Influence, Allegiance, Happiness, Happiness_Localised, MyReputation) " +
                        "VALUES (@event_id, @idx, @Name, @FactionState, @Government, @Influence, @Allegiance, @Happiness, @Happiness_Localised, @MyReputation)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParamStr("@idx", idx.ToString(), MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Name", faction["Name"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@FactionState", faction["FactionState"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Government", faction["Government"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Influence", faction["Influence"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Allegiance", faction["Allegiance"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Happiness", faction["Happiness"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Happiness_Localised", faction["Happiness_Localised"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@MyReputation", faction["MyReputation"], MySqlDbType.Decimal, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_FSDJump_Factions: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Creates an FSD jump faction active state record
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="factionIdx">Faction index</param>
        /// <param name="idx">Active state index</param>
        /// <param name="state">JSON active state data</param>
        private void CreateEvent_FSDJump_Factions_ActiveStates(int eventId, int factionIdx, int idx, dynamic state)
        {
            // TODO: if (EventTypeRecordExists(eventId, "event_FSDJump_Factions_ActiveStates")) { return; }
            //       Ok, going to ignore errors for now, but we need a function to check sub-event records with more than the event_id as the primary key!

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT IGNORE INTO event_FSDJump_Factions_ActiveStates " +
                        "(event_id, factionIdx, idx, State) VALUES (@event_id, @factionIdx, @idx, @State)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParamStr("@factionIdx", factionIdx.ToString(), MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParamStr("@idx", idx.ToString(), MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@State", state["State"], MySqlDbType.VarChar, 100));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_FSDJump_Factions_ActiveStates: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Creates an FSD jump faction pending state record
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="factionIdx">Faction index</param>
        /// <param name="idx">Pending state index</param>
        /// <param name="state">JSON pending state data</param>
        private void CreateEvent_FSDJump_Factions_PendingStates(int eventId, int factionIdx, int idx, dynamic state)
        {
            // TODO: if (EventTypeRecordExists(eventId, "event_FSDJump_Factions_PendingStates")) { return; }
            //       Ok, going to ignore errors for now, but we need a function to check sub-event records with more than the event_id as the primary key!

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT IGNORE INTO event_FSDJump_Factions_PendingStates " +
                        "(event_id, factionIdx, idx, State, Trend) VALUES (@event_id, @factionIdx, @idx, @State, @Trend)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParamStr("@factionIdx", factionIdx.ToString(), MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParamStr("@idx", idx.ToString(), MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@State", state["State"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Trend", state["Trend"], MySqlDbType.Int32, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_FSDJump_Factions_PendingStates: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Creates an FSD jump faction recovering state record
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="factionIdx">Faction index</param>
        /// <param name="idx">Recovering state index</param>
        /// <param name="state">JSON recovering state data</param>
        private void CreateEvent_FSDJump_Factions_RecoveringStates(int eventId, int factionIdx, int idx, dynamic state)
        {
            // TODO: if (EventTypeRecordExists(eventId, "event_FSDJump_Factions_RecoveringStates")) { return; }
            //       Ok, going to ignore errors for now, but we need a function to check sub-event records with more than the event_id as the primary key!

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT IGNORE INTO event_FSDJump_Factions_RecoveringStates " +
                        "(event_id, factionIdx, idx, State, Trend) VALUES (@event_id, @factionIdx, @idx, @State, @Trend)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParamStr("@factionIdx", factionIdx.ToString(), MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParamStr("@idx", idx.ToString(), MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@State", state["State"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Trend", state["Trend"], MySqlDbType.Int32, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_FSDJump_Factions_RecoveringStates: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Creates an FSD target record.
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventLine">JSON event data</param>
        private void CreateEvent_FSDTarget(int eventId, dynamic eventData)
        {
            if (EventTypeRecordExists(eventId, "event_FSDTarget")) { return; }

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_FSDTarget (event_id, event_timestamp, Name, SystemAddress, RemainingJumpsInRoute) " +
                        "VALUES (@event_id, @event_timestamp, @Name, @SystemAddress, @RemainingJumpsInRoute)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParam_EventTimestamp(eventData));
                        mysqlCmd.Parameters.Add(NewParam("@Name", eventData["Name"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemAddress", eventData["SystemAddress"], MySqlDbType.Int64, 0));
                        mysqlCmd.Parameters.Add(NewParam("@RemainingJumpsInRoute", eventData["RemainingJumpsInRoute"], MySqlDbType.Int32, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_FSDTarget: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Creates an FSS Discovery Scan record.
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventLine">JSON event data</param>
        private void CreateEvent_FSSDiscoveryScan(int eventId, dynamic eventData)
        {
            if (EventTypeRecordExists(eventId, "event_FSSDiscoveryScan")) { return; }

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_FSSDiscoveryScan (event_id, event_timestamp, Progress, BodyCount, NonBodyCount, SystemName, SystemAddress) " +
                        "VALUES (@event_id, @event_timestamp, @Progress, @BodyCount, @NonBodyCount, @SystemName, @SystemAddress)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParam_EventTimestamp(eventData));
                        mysqlCmd.Parameters.Add(NewParam("@Progress", eventData["Progress"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@BodyCount", eventData["BodyCount"], MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@NonBodyCount", eventData["NonBodyCount"], MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@SystemName", eventData["SystemName"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemAddress", eventData["SystemAddress"], MySqlDbType.Int64, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_FSSDiscoveryScan: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Creates a LoadGame event record
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventData">JSON event data</param>
        private void CreateEvent_LoadGame(int eventId, dynamic eventData)
        {
            if (EventTypeRecordExists(eventId, "event_LoadGame")) { return; }

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_LoadGame (event_id, event_timestamp, FID, Commander, Horizons, Ship, ShipID, ShipName, " +
                        "ShipIdent, FuelLevel, FuelCapacity, GameMode, GroupName, Credits, Loan) " +
                        "VALUES (@event_id, @event_timestamp, @FID, @Commander, @Horizons, @Ship, @ShipID, @ShipName, @ShipIdent, @FuelLevel, " +
                        "@FuelCapacity, @GameMode, @GroupName, @Credits, @Loan)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParam_EventTimestamp(eventData));
                        mysqlCmd.Parameters.Add(NewParam("@FID", eventData["FID"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Commander", eventData["Commander"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Horizons", eventData["Horizons"], MySqlDbType.VarChar, 1));
                        mysqlCmd.Parameters.Add(NewParam("@Ship", eventData["Ship"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@ShipID", eventData["ShipID"], MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@ShipName", eventData["ShipName"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@ShipIdent", eventData["ShipIdent"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@FuelLevel", eventData["FuelLevel"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@FuelCapacity", eventData["FuelCapacity"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@GameMode", eventData["GameMode"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@GroupName", eventData["GroupName"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Credits", eventData["Credits"], MySqlDbType.Int64, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Loan", eventData["Loan"], MySqlDbType.Int64, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_LoadGame: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Creates a location event record
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventData">JSON event data</param>
        private void CreateEvent_Location(int eventId, dynamic eventData)
        {
            if (EventTypeRecordExists(eventId, "event_Location")) { return; }

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_Location (" +
                        "event_id, event_timestamp, Docked, StationName, StationType, MarketID, StationFaction_Name, StationFaction_State, " +
                        "StationGovernment, StationGovernment_Localised, StationAllegiance, StationEconomy, StationEconomy_Localised, " +
                        "StarSystem, SystemAddress, StarPosX, StarPosY, StarPosZ, SystemAllegiance, SystemEconomy, SystemEconomy_Localised, " +
                        "SystemSecondEconomy, SystemSecondEconomy_Localised, SystemGovernment, SystemGovernment_Localised, SystemSecurity, " +
                        "SystemSecurity_Localised, Population, Body, BodyID, BodyType, SystemFaction_Name, SystemFaction_State) " +
                        "VALUES (" +
                        "@event_id, @event_timestamp, @Docked, @StationName, @StationType, @MarketID, @StationFaction_Name, @StationFaction_State, " +
                        "@StationGovernment, @StationGovernment_Localised, @StationAllegiance, @StationEconomy, @StationEconomy_Localised, " +
                        "@StarSystem, @SystemAddress, @StarPosX, @StarPosY, @StarPosZ, @SystemAllegiance, @SystemEconomy, @SystemEconomy_Localised, " +
                        "@SystemSecondEconomy, @SystemSecondEconomy_Localised, @SystemGovernment, @SystemGovernment_Localised, @SystemSecurity, " +
                        "@SystemSecurity_Localised, @Population, @Body, @BodyID, @BodyType, @SystemFaction_Name, @SystemFaction_State)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParam_EventTimestamp(eventData));
                        mysqlCmd.Parameters.Add(NewParam("@Docked", eventData["Docked"], MySqlDbType.VarChar, 1));
                        mysqlCmd.Parameters.Add(NewParam("@StationName", eventData["StationName"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@StationType", eventData["StationType"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@MarketID", eventData["MarketID"], MySqlDbType.Int64, 0));
                        if (eventData["StationFaction"] == null)
                        {
                            mysqlCmd.Parameters.Add(NewParam("@StationFaction_Name", null, MySqlDbType.VarChar, 100));
                            mysqlCmd.Parameters.Add(NewParam("@StationFaction_State", null, MySqlDbType.VarChar, 100));
                        }
                        else if (eventData["StationFaction"].Type == JTokenType.String)
                        {
                            mysqlCmd.Parameters.Add(NewParam("@StationFaction_Name", eventData["StationFaction"], MySqlDbType.VarChar, 100));
                            mysqlCmd.Parameters.Add(NewParam("@StationFaction_State", eventData["FactionState"], MySqlDbType.VarChar, 100));
                        }
                        else
                        {
                            mysqlCmd.Parameters.Add(NewParam("@StationFaction_Name", eventData["StationFaction"]["Name"], MySqlDbType.VarChar, 100));
                            mysqlCmd.Parameters.Add(NewParam("@StationFaction_State", eventData["StationFaction"]["FactionState"], MySqlDbType.VarChar, 100));
                        }
                        mysqlCmd.Parameters.Add(NewParam("@StationGovernment", eventData["StationGovernment"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@StationGovernment_Localised", eventData["StationGovernment_Localised"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@StationAllegiance", eventData["StationAllegiance"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@StationEconomy", eventData["StationEconomy"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@StationEconomy_Localised", eventData["StationEconomy_Localised"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@StarSystem", eventData["StarSystem"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemAddress", eventData["SystemAddress"], MySqlDbType.Int64, 0));
                        mysqlCmd.Parameters.Add(NewParam("@StarPosX", eventData["StarPosX"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@StarPosY", eventData["StarPosY"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@StarPosZ", eventData["StarPosZ"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@SystemAllegiance", eventData["SystemAllegiance"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemEconomy", eventData["SystemEconomy"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemEconomy_Localised", eventData["SystemEconomy_Localised"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemSecondEconomy", eventData["SystemSecondEconomy"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemSecondEconomy_Localised", eventData["SystemSecondEconomy_Localised"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemGovernment", eventData["SystemGovernment"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemGovernment_Localised", eventData["SystemGovernment_Localised"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemSecurity", eventData["SystemSecurity"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemSecurity_Localised", eventData["SystemSecurity_Localised"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Population", eventData["Population"], MySqlDbType.Int64, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Body", eventData["Body"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@BodyID", eventData["BodyID"], MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@BodyType", eventData["BodyType"], MySqlDbType.VarChar, 100));
                        if (eventData["SystemFaction"] == null)
                        {
                            mysqlCmd.Parameters.Add(NewParam("@SystemFaction_Name", null, MySqlDbType.VarChar, 100));
                            mysqlCmd.Parameters.Add(NewParam("@SystemFaction_State", null, MySqlDbType.VarChar, 100));
                        }
                        else if (eventData["SystemFaction"].Type == JTokenType.String)
                        {
                            mysqlCmd.Parameters.Add(NewParam("@SystemFaction_Name", eventData["SystemFaction"], MySqlDbType.VarChar, 100));
                            mysqlCmd.Parameters.Add(NewParam("@SystemFaction_State", eventData["FactionState"], MySqlDbType.VarChar, 100));
                        }
                        else
                        {
                            mysqlCmd.Parameters.Add(NewParam("@SystemFaction_Name", eventData["SystemFaction"]["Name"], MySqlDbType.VarChar, 100));
                            mysqlCmd.Parameters.Add(NewParam("@SystemFaction_State", eventData["SystemFaction"]["FactionState"], MySqlDbType.VarChar, 100));
                        }
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                        // Conflicts
                        if (eventData["Conflicts"] != null && eventData["Conflicts"].Type == JTokenType.Array)
                            for (int idx = 0; idx < eventData["Conflicts"].Count; idx++)
                                CreateEvent_Location_Conflicts(eventId, idx, eventData["Conflicts"][idx]);
                        // Factions
                        if (eventData["Factions"] != null && eventData["Factions"].Type == JTokenType.Array)
                            for (int idx = 0; idx < eventData["Factions"].Count; idx++)
                            {
                                dynamic faction = eventData["Factions"][idx];
                                CreateEvent_Location_Factions(eventId, idx, faction);
                                // Factions ActiveStates
                                if (faction["ActiveStates"] != null && faction["ActiveStates"].Type == JTokenType.Array)
                                    for (int as_idx = 0; as_idx < faction["ActiveStates"].Count; as_idx++)
                                        CreateEvent_Location_Factions_ActiveStates(eventId, idx, as_idx, faction["ActiveStates"][as_idx]);
                                // Factions RecoveringStates
                                if (faction["RecoveringStates"] != null && faction["RecoveringStates"].Type == JTokenType.Array)
                                    for (int rs_idx = 0; rs_idx < faction["RecoveringStates"].Count; rs_idx++)
                                        CreateEvent_Location_Factions_RecoveringStates(eventId, idx, rs_idx, faction["RecoveringStates"][rs_idx]);
                            }
                        // Station Economies
                        if (eventData["StationEconomies"] != null && eventData["StationEconomies"].Type == JTokenType.Array)
                            for (int idx = 0; idx < eventData["StationEconomies"].Count; idx++)
                                CreateEvent_Location_StationEconomies(eventId, idx, eventData["StationEconomies"][idx]);
                        // Station Services
                        if (eventData["StationServices"] != null && eventData["StationServices"].Type == JTokenType.Array)
                            for (int idx = 0; idx < eventData["StationServices"].Count; idx++)
                                CreateEvent_Location_StationServices(eventId, idx, eventData["StationServices"][idx]);
                    }
                    catch (Exception ex)
                    {
                        LogMessage("CreateEvent_Location", eventId, ex);
                    }
                }
            }
        }

        /// <summary>
        /// Create location event conflict records
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="conflict">JSON conflict data</param>
        private void CreateEvent_Location_Conflicts(int eventId, int idx, dynamic conflict)
        {
            // TODO: if (EventTypeRecordExists(eventId, "event_Location_Conflicts")) { return; }
            //       Ok, going to ignore errors for now, but we need a function to check sub-event records with more than the event_id as the primary key!

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_Location_Conflicts (event_id, idx, WarType, Status, Faction1_Name, Faction1_Stake, " +
                        "Faction1_WonDays, Faction2_Name, Faction2_Stake, Faction2_WonDays) " +
                        "VALUES (@event_id, @idx, @WarType, @Status, @Faction1_Name, @Faction1_Stake, @Faction1_WonDays, @Faction2_Name, @Faction2_Stake, " +
                        "@Faction2_WonDays)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParamStr("@idx", idx.ToString(), MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@WarType", conflict["WarType"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Status", conflict["Status"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Faction1_Name", conflict["Faction1"]["Name"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Faction1_Stake", conflict["Faction1"]["Stake"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Faction1_WonDays", conflict["Faction1"]["WonDays"], MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Faction2_Name", conflict["Faction2"]["Name"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Faction2_Stake", conflict["Faction2"]["Stake"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Faction2_WonDays", conflict["Faction2"]["WonDays"], MySqlDbType.Int32, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage("CreateEvent_Location_Conflicts", eventId, ex);
                    }
                }
            }
        }

        /// <summary>
        /// Create location event faction record
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="faction">JSON faction data</param>
        private void CreateEvent_Location_Factions(int eventId, int idx, dynamic faction)
        {
            // TODO: if (EventTypeRecordExists(eventId, "event_Location_Factions")) { return; }
            //       Ok, going to ignore errors for now, but we need a function to check sub-event records with more than the event_id as the primary key!

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_Location_Factions (event_id, idx, Name, State, Government, Influence, " +
                        "Allegiance, Happiness, Happiness_Localised, MyReputation) " +
                        "VALUES (@event_id, @idx, @Name, @State, @Government, @Influence, @Allegiance, @Happiness, @Happiness_Localised, @MyReputation)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParamStr("@idx", idx.ToString(), MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Name", faction["Name"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@State", faction["FactionState"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Government", faction["Government"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Influence", faction["Influence"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Allegiance", faction["Allegiance"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Happiness", faction["Happiness"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Happiness_Localised", faction["Happiness_Localised"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@MyReputation", faction["MyReputation"], MySqlDbType.Decimal, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage("CreateEvent_Location_Factions", eventId, ex);
                    }
                }
            }
        }

        /// <summary>
        /// Create location event faction active state records
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="factionIdx">Index of faction</param>
        /// <param name="idx">Index of active state</param>
        /// <param name="activeState">JSON active state data</param>
        private void CreateEvent_Location_Factions_ActiveStates(int eventId, int factionIdx, int idx, dynamic activeState)
        {
            // TODO: if (EventTypeRecordExists(eventId, "event_Location_Factions_ActiveStates")) { return; }
            //       Ok, going to ignore errors for now, but we need a function to check sub-event records with more than the event_id as the primary key!

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_Location_Factions_ActiveStates (event_id, factionIdx, idx, State) " +
                        "VALUES (@event_id, @factionIdx, @idx, @State)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParamStr("@factionIdx", factionIdx.ToString(), MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParamStr("@idx", idx.ToString(), MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@State", activeState["State"], MySqlDbType.VarChar, 100));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage("CreateEvent_Location_Factions_ActiveStates", eventId, ex);
                    }
                }
            }
        }

        /// <summary>
        /// Create location event faction recovering state records
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="factionIdx">Index of faction</param>
        /// <param name="idx">Index of recovering state</param>
        /// <param name="recoveringState">JSON recovering state data</param>
        private void CreateEvent_Location_Factions_RecoveringStates(int eventId, int factionIdx, int idx, dynamic recoveringState)
        {
            // TODO: if (EventTypeRecordExists(eventId, "event_Location_Factions_RecoveringStates")) { return; }
            //       Ok, going to ignore errors for now, but we need a function to check sub-event records with more than the event_id as the primary key!

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_Location_Factions_RecoveringStates (event_id, factionIdx, idx, State, Trend) " +
                        "VALUES (@event_id, @factionIdx, @idx, @State, @Trend)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParamStr("@factionIdx", factionIdx.ToString(), MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParamStr("@idx", idx.ToString(), MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@State", recoveringState["State"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Trend", recoveringState["Trend"], MySqlDbType.Int32, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage("CreateEvent_Location_Factions_RecoveringStates", eventId, ex);
                    }
                }
            }
        }

        /// <summary>
        /// Create location event station economy records
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="stationEconomy">JSON station economy data</param>
        private void CreateEvent_Location_StationEconomies(int eventId, int idx, dynamic stationEconomy)
        {
            // TODO: if (EventTypeRecordExists(eventId, "event_Location_StationEconomies")) { return; }
            //       Ok, going to ignore errors for now, but we need a function to check sub-event records with more than the event_id as the primary key!

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_Location_StationEconomies (event_id, idx, Name, Name_Localised, Proportion) " +
                        "VALUES (@event_id, @idx, @Name, @Name_Localised, @Proportion)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParamStr("@idx", idx.ToString(), MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Name", stationEconomy["Name"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Name_Localised", stationEconomy["Name_Localised"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Proportion", stationEconomy["Proportion"], MySqlDbType.Decimal, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage("CreateEvent_Location_StationEconomies", eventId, ex);
                    }
                }
            }
        }

        /// <summary>
        /// Creates location event station service record
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="stationService">Name of station service provided</param>
        private void CreateEvent_Location_StationServices(int eventId, int idx, dynamic stationService)
        {
            // TODO: if (EventTypeRecordExists(eventId, "event_Location_StationServices")) { return; }
            //       Ok, going to ignore errors for now, but we need a function to check sub-event records with more than the event_id as the primary key!

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_Location_StationServices (event_id, idx, StationService) VALUES (@event_id, @idx, @StationService)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParamStr("@idx", idx.ToString(), MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@StationService", stationService, MySqlDbType.VarChar, 100));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage("CreateEvent_Location_StationServices", eventId, ex);
                    }
                }
            }
        }

        /// <summary>
        /// Creates Materials records
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventData">JSON event data</param>
        private void CreateEvent_Materials(int eventId, dynamic eventData)
        {
            //if (EventTypeRecordExists(eventId, "event_Materials")) { return; } - Primary key is event_id and Name

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                mysqlCnn.Open();
                // Raw Materials
                if (eventData["Raw"] != null && eventData["Raw"].Type == JTokenType.Array)
                    foreach (dynamic material in eventData["Raw"])
                        CreateEvent_Materials_Material(eventId, eventData, "Raw", material, mysqlCnn);
                // Manufactured Materials
                if (eventData["Manufactured"] != null && eventData["Manufactured"].Type == JTokenType.Array)
                    foreach (dynamic material in eventData["Manufactured"])
                        CreateEvent_Materials_Material(eventId, eventData, "Manufactured", material, mysqlCnn);
                // Encoded Materials
                if (eventData["Encoded"] != null && eventData["Encoded"].Type == JTokenType.Array)
                    foreach (dynamic material in eventData["Encoded"])
                        CreateEvent_Materials_Material(eventId, eventData, "Encoded", material, mysqlCnn);
            }
        }

        /// <summary>
        /// Creates a single materials record
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventData">JSON event data</param>
        /// <param name="category">Material category</param>
        /// <param name="material">Material object</param>
        /// <param name="mysqlCnn">Database connection</param>
        /// <param name="mysqlCmd">Database command object</param>
        private void CreateEvent_Materials_Material(int eventId, dynamic eventData, string category, dynamic material, MySqlConnection mysqlCnn)
        {
            using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
            {
                mysqlCmd.CommandType = CommandType.Text;
                mysqlCmd.CommandText = "INSERT INTO event_Materials (event_id, event_timestamp, Category, Name, Count) " +
                    "VALUES (@event_id, @event_timestamp, @Category, @Name, @count)";
                try
                {
                    mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                    mysqlCmd.Parameters.Add(NewParam_EventTimestamp(eventData));
                    mysqlCmd.Parameters.Add(NewParamStr("@Category", category, MySqlDbType.VarChar, 100));
                    mysqlCmd.Parameters.Add(NewParam("@Name", material["Name"], MySqlDbType.VarChar, 100));
                    mysqlCmd.Parameters.Add(NewParam("@Count", material["Count"], MySqlDbType.Int32, 0));
                    mysqlCmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    LogMessage(String.Format("CreateEvent_Materials_Material: Exception, {0}", ex.Message), eventId);
                }
            }
        }

        /// <summary>
        /// Creates a progress record.
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventLine">JSON event data</param>
        private void CreateEvent_Progress(int eventId, dynamic eventData)
        {
            if (EventTypeRecordExists(eventId, "event_Progress")) { return; }

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_Progress (event_id, event_timestamp, Combat, Trade, Explore, Empire, Federation, CQC) " +
                        "VALUES (@event_id, @event_timestamp, @Combat, @Trade, @Explore, @Empire, @Federation, @CQC)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParam_EventTimestamp(eventData));
                        mysqlCmd.Parameters.Add(NewParam("@Combat", eventData["Combat"], MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Trade", eventData["Trade"], MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Explore", eventData["Explore"], MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Empire", eventData["Empire"], MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Federation", eventData["Federation"], MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@CQC", eventData["CQC"], MySqlDbType.Int32, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_Progress: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Creates a rank record.
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventLine">JSON event data</param>
        private void CreateEvent_Rank(int eventId, dynamic eventData)
        {
            if (EventTypeRecordExists(eventId, "event_Rank")) { return; }

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_Rank (event_id, event_timestamp, Combat, Trade, Explore, Empire, Federation, CQC) " +
                        "VALUES (@event_id, @event_timestamp, @Combat, @Trade, @Explore, @Empire, @Federation, @CQC)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParam_EventTimestamp(eventData));
                        mysqlCmd.Parameters.Add(NewParam("@Combat", eventData["Combat"], MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Trade", eventData["Trade"], MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Explore", eventData["Explore"], MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Empire", eventData["Empire"], MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Federation", eventData["Federation"], MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@CQC", eventData["CQC"], MySqlDbType.Int32, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_Rank: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Creates a received text event
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventData">JSON event data</param>
        private void CreateEvent_ReceiveText(int eventId, dynamic eventData)
        {
            if (EventTypeRecordExists(eventId, "event_ReceiveText")) { return; }

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_ReceiveText (event_id, event_timestamp, MsgFrom, Message, Message_Localised, Channel) " +
                        "VALUES (@event_id, @event_timestamp, @MsgFrom, @Message, @Message_Localised, @Channel)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParam_EventTimestamp(eventData));
                        mysqlCmd.Parameters.Add(NewParam("@MsgFrom", eventData["From"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Message", eventData["Message"], MySqlDbType.VarChar, 255));
                        mysqlCmd.Parameters.Add(NewParam("@Message_Localised", eventData["Message_Localised"], MySqlDbType.VarChar, 255));
                        mysqlCmd.Parameters.Add(NewParam("@Channel", eventData["Channel"], MySqlDbType.VarChar, 100));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_ReceiveText: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Creates a reputation record.
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventLine">JSON event data</param>
        private void CreateEvent_Reputation(int eventId, dynamic eventData)
        {
            if (EventTypeRecordExists(eventId, "event_Reputation")) { return; }

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_Reputation (event_id, event_timestamp, Empire, Federation, Independent, Alliance) " +
                        "VALUES (@event_id, @event_timestamp, @Empire, @Federation, @Independent, @Alliance)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParam_EventTimestamp(eventData));
                        mysqlCmd.Parameters.Add(NewParam("@Empire", eventData["Empire"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Federation", eventData["Federation"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Independent", eventData["Independent"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Alliance", eventData["Alliance"], MySqlDbType.Decimal, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_Reputation: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Creates a scan record
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventData">JSON event data</param>
        private void CreateEvent_Scan(int eventId, dynamic eventData)
        {
            if (EventTypeRecordExists(eventId, "event_Scan")) { return; }

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_Scan (event_id, event_timestamp, ScanType, BodyName, BodyID, StarSystem, SystemAddress, " +
                        "DistanceFromArrivalLS, TidalLock, TerraformState, StarType, SubClass, StellarMass, AbsoluteMagnitude, Age_MY, Luminosity, " +
                        "PlanetClass, Atmosphere, AtmosphereType, Volcanism, MassEM, Radius, SurfaceGravity, SurfaceTemperature, SurfacePressure, Landable, " +
                        "Composition_Ice, Composition_Rock, Composition_Metal, SemiMajorAxis, Eccentricity, OrbitalInclination, Periapsis, OrbitalPeriod, " +
                        "RotationPeriod, AxialTilt, ReserveLevel, WasDiscovered, WasMapped) " +
                        "VALUES (@event_id, @event_timestamp, @ScanType, @BodyName, @BodyID, @StarSystem, @SystemAddress, @DistanceFromArrivalLS, " +
                        "@TidalLock, @TerraformState, @StarType, @SubClass, @StellarMass, @AbsoluteMagnitude, @Age_MY, @Luminosity, @PlanetClass, " +
                        "@Atmosphere, @AtmosphereType, @Volcanism, @MassEM, @Radius, @SurfaceGravity, @SurfaceTemperature, @SurfacePressure, @Landable, " +
                        "@Composition_Ice, @Composition_Rock, @Composition_Metal, @SemiMajorAxis, @Eccentricity, @OrbitalInclination, @Periapsis, " +
                        "@OrbitalPeriod, @RotationPeriod, @AxialTilt, @ReserveLevel, @WasDiscovered, @WasMapped)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParam_EventTimestamp(eventData));
                        mysqlCmd.Parameters.Add(NewParam("@ScanType", eventData["ScanType"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@BodyName", eventData["BodyName"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@BodyID", eventData["BodyID"], MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@StarSystem", eventData["StarSystem"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemAddress", eventData["SystemAddress"], MySqlDbType.Int64, 0));
                        mysqlCmd.Parameters.Add(NewParam("@DistanceFromArrivalLS", eventData["DistanceFromArrivalLS"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@TidalLock", eventData["TidalLock"], MySqlDbType.VarChar, 1));
                        mysqlCmd.Parameters.Add(NewParam("@TerraformState", eventData["TerraformState"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@StarType", eventData["StarType"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SubClass", eventData["SubClass"], MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@StellarMass", eventData["StellarMass"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@AbsoluteMagnitude", eventData["AbsoluteMagnitude"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Age_MY", eventData["Age_MY"], MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Luminosity", eventData["Luminosity"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@PlanetClass", eventData["PlanetClass"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Atmosphere", eventData["Atmosphere"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@AtmosphereType", eventData["AtmosphereType"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Volcanism", eventData["Volcanism"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@MassEM", eventData["MassEM"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Radius", eventData["Radius"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@SurfaceGravity", eventData["SurfaceGravity"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@SurfaceTemperature", eventData["SurfaceTemperature"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@SurfacePressure", eventData["SurfacePressure"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Landable", eventData["Landable"], MySqlDbType.VarChar, 1));
                        mysqlCmd.Parameters.Add(NewParam("@Composition_Ice", (eventData["Composition"] != null) ? eventData["Composition"]["Ice"] : null, MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Composition_Rock", (eventData["Composition"] != null) ? eventData["Composition"]["Rock"] : null, MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Composition_Metal", (eventData["Composition"] != null) ? eventData["Composition"]["Metal"] : null, MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@SemiMajorAxis", eventData["SemiMajorAxis"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Eccentricity", eventData["Eccentricity"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@OrbitalInclination", eventData["OrbitalInclination"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Periapsis", eventData["Periapsis"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@OrbitalPeriod", eventData["OrbitalPeriod"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@RotationPeriod", eventData["RotationPeriod"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@AxialTilt", eventData["AxialTilt"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@ReserveLevel", eventData["ReserveLevel"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@WasDiscovered", eventData["WasDiscovered"], MySqlDbType.VarChar, 1));
                        mysqlCmd.Parameters.Add(NewParam("@WasMapped", eventData["WasMapped"], MySqlDbType.VarChar, 1));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                        // AtmosphereComposition
                        if (eventData["AtmosphereComposition"] != null && eventData["AtmosphereComposition"].Type == JTokenType.Array)
                            for (int idx = 0; idx < eventData["AtmosphereComposition"].Count; idx++)
                                CreateEvent_Scan_AtmosphereComposition(eventId, idx, eventData["AtmosphereComposition"][idx]);
                        // Materials
                        if (eventData["Materials"] != null && eventData["Materials"].Type == JTokenType.Array)
                            for (int idx = 0; idx < eventData["Materials"].Count; idx++)
                                CreateEvent_Scan_Materials(eventId, idx, eventData["Materials"][idx]);
                        // Parents
                        if (eventData["Parents"] != null && eventData["Parents"].Type == JTokenType.Array)
                            for (int idx = 0; idx < eventData["Parents"].Count; idx++)
                                CreateEvent_Scan_Parents(eventId, idx, eventData["Parents"][idx].ToObject<Dictionary<string, string>>());
                        // Rings
                        if (eventData["Rings"] != null && eventData["Rings"].Type == JTokenType.Array)
                            for (int idx = 0; idx < eventData["Rings"].Count; idx++)
                                CreateEvent_Scan_Rings(eventId, idx, eventData["Rings"][idx]);
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_Scan: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Create a scan atmosphere composition record
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="idx">Atmosphere composition index</param>
        /// <param name="element">JSON Atmosphere composition data</param>
        private void CreateEvent_Scan_AtmosphereComposition(int eventId, int idx, dynamic element)
        {
            // TODO: if (EventTypeRecordExists(eventId, "event_Scan_AtmosphereComposition")) { return; }
            //       Ok, going to ignore errors for now, but we need a function to check sub-event records with more than the event_id as the primary key!

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT IGNORE INTO event_Scan_AtmosphereComposition (event_id, idx, Name, Percent) VALUES " +
                        "(@event_id, @idx, @Name, @Percent)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParamStr("@idx", idx.ToString(), MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Name", element["Name"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Percent", element["Percent"], MySqlDbType.Decimal, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_Scan_AtmosphereComposition: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Create a scan materials record
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="idx">Scan material record index</param>
        /// <param name="element">JSON Scan material data</param>
        private void CreateEvent_Scan_Materials(int eventId, int idx, dynamic element)
        {
            // TODO: if (EventTypeRecordExists(eventId, "event_Scan_Materials")) { return; }
            //       Ok, going to ignore errors for now, but we need a function to check sub-event records with more than the event_id as the primary key!

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT IGNORE INTO event_Scan_Materials (event_id, idx, Name, Percent) VALUES (@event_id, @idx, @Name, @Percent)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParamStr("@idx", idx.ToString(), MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Name", element["Name"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@Percent", element["Percent"], MySqlDbType.Decimal, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_Scan_Materials: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Create a scan parents record
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="idx">Parent index</param>
        /// <param name="parent">Parent data</param>
        private void CreateEvent_Scan_Parents(int eventId, int idx, Dictionary<string, string> parent)
        {
            // TODO: if (EventTypeRecordExists(eventId, "event_Scan_Parents")) { return; }
            //       Ok, going to ignore errors for now, but we need a function to check sub-event records with more than the event_id as the primary key!

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT IGNORE INTO event_Scan_Parents (event_id, idx, BodyID, BodyType) " +
                        "VALUES (@event_id, @idx, @BodyID, @BodyType)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParamStr("@idx", idx.ToString(), MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParamStr("@BodyID", parent[parent.Keys.ElementAt(0)], MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParamStr("@BodyType", parent.Keys.ElementAt(0), MySqlDbType.VarChar, 100));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_Scan_Materials: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Create a scan ring record
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="idx">Scan ring index</param>
        /// <param name="ring">JSON Scan ring data</param>
        private void CreateEvent_Scan_Rings(int eventId, int idx, dynamic ring)
        {
            // TODO: if (EventTypeRecordExists(eventId, "event_Scan_Rings")) { return; }
            //       Ok, going to ignore errors for now, but we need a function to check sub-event records with more than the event_id as the primary key!

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT IGNORE INTO event_Scan_Rings (event_id, idx, Name, RingClass, MassMT, InnerRad, OuterRad) " +
                        "VALUES (@event_id, @idx, @Name, @RingClass, @MassMT, @InnerRad, @OuterRad)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParamStr("@idx", idx.ToString(), MySqlDbType.Int32, 0));
                        mysqlCmd.Parameters.Add(NewParam("@Name", ring["Name"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@RingClass", ring["RingClass"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@MassMT", ring["MassMT"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@InnerRad", ring["InnerRad"], MySqlDbType.Decimal, 0));
                        mysqlCmd.Parameters.Add(NewParam("@OuterRad", ring["OuterRad"], MySqlDbType.Decimal, 0));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_Scan_Rings: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Creates a start jump record.
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <param name="eventLine">JSON event data</param>
        private void CreateEvent_StartJump(int eventId, dynamic eventData)
        {
            if (EventTypeRecordExists(eventId, "event_StartJump")) { return; }

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO event_StartJump (event_id, event_timestamp, JumpType, StarSystem, SystemAddress, StarClass) " +
                        "VALUES (@event_id, @event_timestamp, @JumpType, @StarSystem, @SystemAddress, @StarClass)";
                    try
                    {
                        mysqlCmd.Parameters.Add(NewParam_EventId(eventId));
                        mysqlCmd.Parameters.Add(NewParam_EventTimestamp(eventData));
                        mysqlCmd.Parameters.Add(NewParam("@JumpType", eventData["JumpType"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@StarSystem", eventData["StarSystem"], MySqlDbType.VarChar, 100));
                        mysqlCmd.Parameters.Add(NewParam("@SystemAddress", eventData["SystemAddress"], MySqlDbType.Int64, 0));
                        mysqlCmd.Parameters.Add(NewParam("@StarClass", eventData["StarClass"], MySqlDbType.VarChar, 100));
                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("CreateEvent_StartJump: Exception, {0}", ex.Message), eventId);
                    }
                }
            }
        }

        /// <summary>
        /// Create a MySql query parameter.
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <param name="value">Newtonsoft JSON.NET JValue for the parameter value</param>
        /// <param name="type">MySql type for the parameter</param>
        /// <param name="length">Length of the type or 0 if not required</param>
        /// <returns>MySql parameter</returns>
        private MySqlParameter NewParam(string name, JValue value, MySqlDbType type, int length)
        {
            MySqlParameter result = (length == 0) ? new MySqlParameter(name, type) : new MySqlParameter(name, type, length);
            if (value == null)
            {
                result.Direction = ParameterDirection.Input;
                result.IsNullable = true;
            }
            else
                result.Value = ParamParseValue(value.ToString(), type);
            return result;
        }

        /// <summary>
        /// Create a MySql query parameter.
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <param name="value">String for the parameter value</param>
        /// <param name="type">MySql type for the parameter</param>
        /// <param name="length">Length of the type or 0 if not required</param>
        /// <returns>MySql parameter</returns>
        private MySqlParameter NewParamStr(string name, string value, MySqlDbType type, int length)
        {
            MySqlParameter result = (length == 0) ? new MySqlParameter(name, type) : new MySqlParameter(name, type, length);
            if (value == null)
            {
                result.Direction = ParameterDirection.Input;
                result.IsNullable = true;
            }
            else
                result.Value = ParamParseValue(value, type);
            return result;
        }

        /// <summary>
        /// Parse a parameter value correctly for the given database datatype.
        /// </summary>
        /// <param name="input">String value to set in the parameter</param>
        /// <param name="type">Database datatype</param>
        /// <returns>Parameter value to send to the database</returns>
        private object ParamParseValue(string input, MySqlDbType type)
        {
            switch (type)
            {
                case MySqlDbType.Int32:
                    return Int32.Parse(input, NumberStyles.AllowExponent | NumberStyles.AllowDecimalPoint | NumberStyles.AllowLeadingSign);
                case MySqlDbType.Int64:
                    return Int64.Parse(input, NumberStyles.AllowExponent | NumberStyles.AllowDecimalPoint | NumberStyles.AllowLeadingSign);
                case MySqlDbType.Decimal:
                    return Decimal.Parse(input, NumberStyles.AllowExponent | NumberStyles.AllowDecimalPoint | NumberStyles.AllowLeadingSign);
                case MySqlDbType.VarChar:
                    if (input.ToLower() == "true" || input.ToLower() == "false")
                        return (input.ToLower() == "true") ? "Y" : "N";
                    else
                        return input;
                default:
                    return input;
            }
        }

        /// <summary>
        /// Create a MySql string query parameter.
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <param name="value">Parameter string value</param>
        /// <param name="type">MySql type for the parameter</param>
        /// <param name="length">Length of the string</param>
        /// <returns>MySql parameter</returns>
        private MySqlParameter NewStringParam(string name, string value, MySqlDbType type, int length)
        {
            MySqlParameter result = (length == 0) ? new MySqlParameter(name, type) : new MySqlParameter(name, type, length);
            if (value == null)
            {
                result.Direction = ParameterDirection.Input;
                result.IsNullable = true;
            }
            else
                result.Value = value;
            return result;
        }

        /// <summary>
        /// MySql query parameter for the event ID
        /// </summary>
        /// <param name="eventId">Event ID</param>
        /// <returns>MySql parameter</returns>
        private MySqlParameter NewParam_EventId(int eventId)
        {
            MySqlParameter result = new MySqlParameter("@event_id", MySqlDbType.Int32);
            result.Value = eventId;
            return result;
        }

        /// <summary>
        /// MySql query parameter for the event timestamp
        /// </summary>
        /// <param name="obj">Object containing the timestamp value</param>
        /// <returns>MySqlk parameter</returns>
        private MySqlParameter NewParam_EventTimestamp(dynamic obj)
        {
            MySqlParameter result = new MySqlParameter("@event_timestamp", MySqlDbType.DateTime);
            result.Value = GetTimestamp(obj);
            return result;
        }

        /// <summary>
        /// Re-processes specified event types, also useful for processing events already imported but not previously catered for.
        /// </summary>
        private void ReprocessEvents()
        {
            // Select all events of type specified in application config file.
            DataTable eventsInfo = new DataTable();

            using (MySqlConnection mysqlCnn = new MySqlConnection(connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "SELECT * FROM event WHERE type = @event";
                    // Param: @events
                    mysqlCmd.Parameters.Add("@event", MySqlDbType.Text);
                    mysqlCmd.Parameters["@event"].Value = this.reprocessEvents;

                    using (MySqlDataAdapter mysqlDAd = new MySqlDataAdapter(mysqlCmd))
                        mysqlDAd.Fill(eventsInfo);

                    foreach (DataRow eventInfo in eventsInfo.Rows)
                    {
                        CreateEventTypeRecord((int)eventInfo["id"], JsonToObject(eventInfo["data"].ToString()));
                    }
                }
            }
        }

        /// <summary>
        /// Truncate data from a given table.
        /// </summary>
        /// <param name="tableName">Name of table to be truncated</param>
        /// <returns>Success</returns>
        private bool TruncateTable(string tableName)
        {
            bool result = false;

            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "TRUNCATE TABLE @table_name";

                    try
                    {
                        // Param: @table_name
                        mysqlCmd.Parameters.Add("@table_name", MySqlDbType.VarChar, 100);
                        mysqlCmd.Parameters["@table_name"].Value = tableName;

                        mysqlCnn.Open();
                        mysqlCmd.ExecuteNonQuery();
                        result = true;
                    }
                    catch (Exception ex)
                    {
                        LogMessage(String.Format("TruncateTable: Exception, {0}, table_name:{1}", ex.Message, tableName));
                        throw new Exception("CreateEvent_FSDTarget:Exception", ex);
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Locate the latest journal file.
        /// </summary>
        /// <returns>Success</returns>
        private void LocateLatestJournalFile()
        {
            try
            {
                FileInfo latestFile = this.journalDirInfo.GetFiles("Journal.*.log")
                    .OrderByDescending(f => f.LastWriteTime)
                    .First();

                if (latestFile != null)
                {
                    if ((this.curActiveFile != null && latestFile.Name == this.curActiveFile.Name) ||
                        (this.lastActiveFile != null && latestFile.Name == this.lastActiveFile.Name))
                        this.newActiveFile = null;
                    else
                        this.newActiveFile = latestFile;
                }
            }
            catch (ArgumentNullException)
            {
                LogMessage("LocateLatestJournalFile: Search pattern is null");
            }
            catch (ArgumentException)
            {
                LogMessage("LocateLatestJournalFile: Search pattern \"Journal.*.log\" contains one or more invalid characters");
            }
            catch (DirectoryNotFoundException)
            {
                LogMessage(String.Format("LocateLatestJournalFile: The path \"{0}\" is invalid", journalPath));
            }
            catch (SecurityException)
            {
                LogMessage(String.Format("LocateLatestJournalFile: The user does not have sufficient permission to access files in \"{0}\"", journalPath));
            }
            catch (Exception ex)
            {
                LogMessage(String.Format("LocateLatestJournalFile: Exception, {0}", ex.Message));
            }
        }

        /// <summary>
        /// Convert a JSON string to an object.
        /// </summary>
        /// <param name="json">JSON string</param>
        /// <returns>Dynamic object representing the JSON string supplied</returns>
        private dynamic JsonToObject(string json)
        {
            return JsonConvert.DeserializeObject(json, new JsonSerializerSettings() { DateParseHandling = DateParseHandling.None });
        }

        /// <summary>
        /// Retrieves the correctly formatted timestamp from an event
        /// </summary>
        /// <param name="eventLine">Journal file event line</param>
        /// <returns>Timestamp of the given event line</returns>
        private string GetTimestamp(dynamic eventLine)
        {
            return eventLine["timestamp"].ToString().Replace("T", " ").Replace("Z", "");
        }

        /// <summary>
        /// Logs a message to the log table, without an associated event.
        /// </summary>
        /// <param name="Message">Message to be logged</param>
        private void LogMessage(string message)
        {
            LogMessage(message, 0);
        }

        /// <summary>
        /// Logs a message to the log table, optionally with an associated event.
        /// </summary>
        /// <param name="message">Message to be logged</param>
        /// <param name="event_id">Event ID</param>
        private void LogMessage(string message, int eventId)
        {
            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO log (timestamp, message, event_id) VALUES (@timestamp, @message, @event_id)";
                    // Param: timestamp
                    mysqlCmd.Parameters.Add("@timestamp", MySqlDbType.DateTime);
                    mysqlCmd.Parameters["@timestamp"].Value = DateTime.Now;
                    // Param: message
                    mysqlCmd.Parameters.Add("@message", MySqlDbType.VarChar, 255);
                    mysqlCmd.Parameters["@message"].Value = message;
                    // Param: event_id
                    mysqlCmd.Parameters.Add("@event_id", MySqlDbType.Int32);
                    mysqlCmd.Parameters["@event_id"].Direction = ParameterDirection.Input;
                    mysqlCmd.Parameters["@event_id"].IsNullable = true;
                    if (eventId != 0)
                        mysqlCmd.Parameters["@event_id"].Value = eventId;

                    mysqlCnn.Open();
                    mysqlCmd.ExecuteNonQuery();
                }
            }
        }

        private void LogMessage(string functionName, int eventId, Exception ex, bool innerException = false)
        {
            using (MySqlConnection mysqlCnn = new MySqlConnection(this.connStr))
            {
                using (MySqlCommand mysqlCmd = mysqlCnn.CreateCommand())
                {
                    mysqlCmd.CommandType = CommandType.Text;
                    mysqlCmd.CommandText = "INSERT INTO log (timestamp, message, event_id) VALUES (@timestamp, @message, @event_id)";
                    // Param: timestamp
                    mysqlCmd.Parameters.Add("@timestamp", MySqlDbType.DateTime);
                    mysqlCmd.Parameters["@timestamp"].Value = DateTime.Now;
                    // Param: message
                    mysqlCmd.Parameters.Add("@message", MySqlDbType.VarChar, 255);
                    mysqlCmd.Parameters["@message"].Value = String.Format("{0}: {1}, {2}", functionName, (innerException) ? "Inner Exception" : "Exception", ex.Message);
                    // Param: event_id
                    mysqlCmd.Parameters.Add("@event_id", MySqlDbType.Int32);
                    mysqlCmd.Parameters["@event_id"].Direction = ParameterDirection.Input;
                    mysqlCmd.Parameters["@event_id"].IsNullable = true;
                    if (eventId != 0)
                        mysqlCmd.Parameters["@event_id"].Value = eventId;

                    mysqlCnn.Open();
                    mysqlCmd.ExecuteNonQuery();

                    if (ex.InnerException != null)
                        LogMessage(functionName, eventId, ex.InnerException, true);
                }
            }
        }

        /// <summary>
        /// Click event for start/stop button
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btnControl_Click(object sender, EventArgs e)
        {
            btnControl.Enabled = false;

            if (btnControl.Text == "Start")
            {
                BGW_Start();
            }
            else
            {
                BGW_Stop();
            }
        }
    }
}
