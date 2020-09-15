using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.SqlClient;
using System.Data.Odbc;
using System.IO;
using System.Security.AccessControl;
using System.Text;
using System.Threading.Tasks;
using OIMNTFS_Service;

namespace OIMNTFS_Service
{
    public class OIMNTFSScanner
    {
        OIMNTFSTableAdapters.FilesystemsTableAdapter fileSystemsTable = new OIMNTFSTableAdapters.FilesystemsTableAdapter();
        OIMNTFS.FilesystemsDataTable fileSystems = new OIMNTFS.FilesystemsDataTable();
        
        OIMNTFSTableAdapters.TopLevelNodesTableAdapter topLevelNodesTable = new OIMNTFSTableAdapters.TopLevelNodesTableAdapter();
        OIMNTFS.TopLevelNodesDataTable topLevelNodes = new OIMNTFS.TopLevelNodesDataTable();
        
        OIMNTFSTableAdapters.ExcludeNodesTableAdapter excludeNodesTable = new OIMNTFSTableAdapters.ExcludeNodesTableAdapter();
        OIMNTFS.ExcludeNodesDataTable excludeNodes = new OIMNTFS.ExcludeNodesDataTable();
        
        OIMNTFSTableAdapters.ScanRequestsTableAdapter ScanRequestsTable = new OIMNTFSTableAdapters.ScanRequestsTableAdapter();
        OIMNTFS.ScanRequestsDataTable scanRequests = new OIMNTFS.ScanRequestsDataTable();

        OIMNTFSTableAdapters.NodesTableAdapter nodesTable = new OIMNTFSTableAdapters.NodesTableAdapter();
        OIMNTFSTableAdapters.EntitlementsTableAdapter entitlementsTable = new OIMNTFSTableAdapters.EntitlementsTableAdapter();
        
        SqlCommand getNewNodeID;
        NetworkDrive networkDrive;

        long entitlementcounter = 0;
        long foldercounter = 0;
        long protectedcounter = 0;

        Int64 FullScanRunningOnTopLevelNode = 0;
        Int64 PartialScanRunningOnTopLevelNode = 0;

        EventLog scannerLog;
        ADCache ad;

        DateTime start = DateTime.Now;
        SqlConnection scannerConnection;

        bool runLoops = true;

        System.Threading.Thread FullScanWorkerThread;
        System.Threading.Thread PartialScanWorkerThread;

        public OIMNTFSScanner(string connectionString)
        {
            scannerLog = new EventLog("OIMNTFS Scanner");

            try
            {
                scannerLog.Buffer("Initializing OIMNTFS Scanner instance");

                // final preparation step: load AD objects (users, groups)
                ad = new ADCache("LDAP://10.112.128.3/DC=nrwbanki,DC=de");

                networkDrive = new NetworkDrive();

                scannerLog.Buffer("Trying to open '{0}'.", connectionString);

                scannerConnection = new SqlConnection(connectionString);
                scannerConnection.Open();

                getNewNodeID = new SqlCommand("SELECT CAST(ISNULL(IDENT_CURRENT('Nodes'), 0) as bigint)", scannerConnection);

                scannerLog.Buffer("Reading data tables from database {0}...", scannerConnection.Database);

                fileSystemsTable.Connection = scannerConnection;
                fileSystemsTable.Fill(fileSystems);
                scannerLog.Buffer("fileSystems table filled.");

                excludeNodesTable.Connection = scannerConnection;
                excludeNodesTable.Fill(excludeNodes);
                scannerLog.Buffer("excludeNodes table filled.");

                topLevelNodesTable.Connection = scannerConnection;
                topLevelNodesTable.Fill(topLevelNodes);
                scannerLog.Buffer("topLevelNodes table filled.");

                ScanRequestsTable.Connection = scannerConnection;
                ScanRequestsTable.Fill(scanRequests);

                nodesTable.Connection = scannerConnection;
                // no fill - target only

                entitlementsTable.Connection = scannerConnection;
                // no fill - target only

                FullScanWorkerThread = new System.Threading.Thread(new System.Threading.ThreadStart(RunFullScan));
                FullScanWorkerThread.Start();

                PartialScanWorkerThread = new System.Threading.Thread(new System.Threading.ThreadStart(RunPartialScan));
                PartialScanWorkerThread.Start();
            }
            catch (Exception e)
            {
                scannerLog.Buffer("Init failed: {0}", e.Message);

                if (scannerConnection.State == ConnectionState.Open) scannerConnection.Close();

                if (PartialScanWorkerThread.ThreadState == System.Threading.ThreadState.Running) PartialScanWorkerThread.Abort();
                scannerLog.Buffer("Provider shut down.");

                if (FullScanWorkerThread.ThreadState == System.Threading.ThreadState.Running) FullScanWorkerThread.Abort();
                scannerLog.Buffer("Worker shut down.");
            }
            finally
            {
                scannerLog.Flush();
            }
        }

        
        /*****************************************************************************************************
         * RunFullScan
         * Thread runs in loops to scan all Nodes below all TopLevelNodes
         * 
         *****************************************************************************************************/
        private void RunFullScan()
        {
            EventLog fullScanLog = new EventLog("Full Scan");
            while (runLoops)
            {
                fullScanLog.Buffer("Running the full scan again.");
                FullScan(fullScanLog);
                fullScanLog.Flush();
                System.Threading.Thread.Sleep(60000);
            }
        }

        private void FullScan(EventLog eventLog)
        {
            fileSystemsTable.ClearBeforeFill = true;
            fileSystemsTable.Fill(fileSystems);

            foreach (OIMNTFS.FilesystemsRow fileSystem in fileSystems.Rows)
            {
                eventLog.Buffer("Reading file system {0}", fileSystem.DriveRoot);
                networkDrive.LocalDrive = fileSystem.DriveRoot.Substring(0, 2);
                networkDrive.Persistent = false;
                networkDrive.SaveCredentials = false;
                networkDrive.Force = true;
                networkDrive.ShareName = "\\\\" + fileSystem.ProviderIP + "\\" + fileSystem.Share;
                eventLog.Buffer("Mapping drive {0} to {1}", networkDrive.LocalDrive, networkDrive.ShareName);
                try
                {
                    switch (fileSystem.Type)
                    {
                        case 0:
                            networkDrive.MapDrive();
                            break;
                        case 1:
                            networkDrive.MapDrive(fileSystem.User, fileSystem.Password);
                            break;
                        default:
                            networkDrive.MapDrive(fileSystem.User, fileSystem.Password);
                            break;
                    }
                }
                catch (Exception e)
                {
                    eventLog.Buffer("unable to map drive {0} to {1}", networkDrive.LocalDrive, networkDrive.ShareName);
                    eventLog.Buffer("{0}", e.ToString());
                    continue;
                }

                eventLog.Buffer("Updating top level folders of {0}...", fileSystem.DriveRoot);

                DirectoryInfo dInfo = null;
                DirectorySecurity dSecurity = null;

                string[] topLevelNodePaths = (string[])null;
                Int64 filesystemID = fileSystem.ID;

                try
                {
                    topLevelNodePaths = Directory.GetDirectories(fileSystem.DriveRoot, "*", SearchOption.TopDirectoryOnly);
                }
                catch (Exception e)
                {
                    eventLog.Buffer("Directories in {0} cannot be read.", fileSystem.DriveRoot);
                    eventLog.Buffer("{0}", e.Message);
                    continue;
                }

                foreach (string topLevelNodePath in topLevelNodePaths)
                {
                    if (excludeNodes.Select("'" + topLevelNodePath + "' LIKE excludeNode").Length > 0)
                        continue;
                    try
                    {
                        dInfo = new DirectoryInfo(topLevelNodePath);
                        dSecurity = dInfo.GetAccessControl();
                    }
                    catch (Exception e)
                    {
                        eventLog.Buffer("Directory info in {0} cannot be read.", topLevelNodePath);
                        eventLog.Buffer("{0}", e.Message);
                        continue;
                    }

                    DateTime lastWrite = dInfo.LastWriteTimeUtc;
                    DateTime lastAccess = dInfo.LastAccessTimeUtc;
                    string ownerSID = null;
                    string owner = null;

                    try
                    {
                        ownerSID = dSecurity.GetOwner(typeof(System.Security.Principal.SecurityIdentifier)).Value;
                        owner = ad.getObjectName(ownerSID);
                    }
                    catch (Exception e)
                    {
                        eventLog.Buffer("Unable to read owner of {0}", topLevelNodePath);
                        eventLog.Buffer(e.Message);
                    }
                    Boolean isProtected = dSecurity.AreAccessRulesProtected;

                    if (topLevelNodes.Select("FullPath = '" + dInfo.FullName + "'").Length == 0)
                    {
                        eventLog.Buffer("Found new node '{0}'", dInfo.FullName);
                        OIMNTFS.TopLevelNodesRow newTopLevelNode = topLevelNodes.NewTopLevelNodesRow();

                        newTopLevelNode.FilesystemID = filesystemID;
                        newTopLevelNode.ScanDepth = fileSystem.Depth;
                        newTopLevelNode.FullPath = dInfo.FullName;
                        newTopLevelNode.Name = dInfo.Name;
                        newTopLevelNode.LastAccessUTC = dInfo.LastAccessTimeUtc;
                        newTopLevelNode.LastWriteUTC = dInfo.LastWriteTimeUtc;
                        newTopLevelNode.LastScanned = DateTime.MinValue;
                        newTopLevelNode.FirstSeen = DateTime.UtcNow;
                        newTopLevelNode.DataOwner = owner;
                        newTopLevelNode.isProtected = isProtected;

                        topLevelNodes.AddTopLevelNodesRow(newTopLevelNode);
                    }
                }
                topLevelNodesTable.Update(topLevelNodes);
            }

            // now start to process all top level nodes
            foreach (OIMNTFS.TopLevelNodesRow topLevelNode in topLevelNodes.OrderBy(n => n.LastScanned))
            {
                eventLog.Buffer("Scanning {0} down to level {1}...", topLevelNode.FullPath, topLevelNode.ScanDepth);
                try
                {
                    while (PartialScanRunningOnTopLevelNode == topLevelNode.ID)
                    {
                        scannerLog.Write("FullScan is waiting for single scan below {0} to complete.", topLevelNode.FullPath);
                        System.Threading.Thread.Sleep(5000);
                        // busy wait :-(
                    }
                    FullScanRunningOnTopLevelNode = topLevelNode.ID;

                    start = DateTime.Now;
                    foldercounter = 0;
                    entitlementcounter = 0;
                    protectedcounter = 0;

                    ProcessNode(topLevelNode.FullPath, 1, topLevelNode.ScanDepth, topLevelNode.ID, 0, eventLog);
                    eventLog.Buffer("Done.");

                    eventLog.Buffer("Updating database...");
                    try
                    {
                        // first delete old values
                        eventLog.Buffer("Deleting old scan information for {0}...", topLevelNode.FullPath);
                        SqlCommand delnodes = scannerConnection.CreateCommand();
                        delnodes.CommandText = string.Format("DELETE FROM [OIMNTFS].[dbo].[Nodes] WHERE TopLevelNodeID = {0}", topLevelNode.ID);
                        delnodes.ExecuteNonQuery();

                        // update last scanned timestamp
                        (topLevelNodes.FindByID(topLevelNode.ID)).LastScanned = DateTime.Now;
                        // now update (insert) nodes processed
                        topLevelNodesTable.Update(topLevelNodes);
                    }
                    catch (Exception e)
                    {
                        eventLog.Buffer("Failed to update last scanned timestamp for {0}", topLevelNode.FullPath);
                        eventLog.Buffer(e.Message);
                    }
                    eventLog.Buffer("{0} completed on {1:hh:mm:ss}.\n{2} folders read ({3:0.0} folders per second)\n", topLevelNode.FullPath, DateTime.Now, foldercounter, foldercounter / (DateTime.Now - start).TotalSeconds, foldercounter);
                        
                    // Update last access and last write timestamp on TopLevelFolders
                    string cmdtext = @"
                    WITH NodesMax AS (
                        SELECT TopLevelNodes.ID, maxlastaccess = MAX(LastAccess), maxlastwrite = MAX(LastWrite)
                        FROM TopLevelNodes
                        JOIN Nodes ON Nodes.TopLevelNodeID = TopLevelNodes.ID
                        GROUP BY TopLevelNodes.ID
                    )
                    UPDATE TopLevelNodes
                    SET
                        LastTreeAccessUTC = NodesMax.maxlastaccess,
                        LastTreeWriteUTC = NodesMax.maxlastwrite
                        FROM ToplevelNodes
                        JOIN NodesMax ON NodesMax.ID = TopLevelNodes.ID";
                    (new SqlCommand(cmdtext, scannerConnection)).ExecuteNonQuery();

                    eventLog.Flush();
                }
                catch (Exception e)
                {
                    scannerLog.Write("Exception during scan below {0}: {1}", topLevelNode.FullPath, e.Message);
                    eventLog.Buffer("Exception during scan below {0}: {1}", topLevelNode.FullPath, e.Message);
                    eventLog.Flush();
                }
                finally
                {
                    FullScanRunningOnTopLevelNode = 0;
                }
            }
        }

        /*****************************************************************************************************
         * RunPartialScan
         * Thread runs in loops to scan all Nodes requested to be scanned directly
         * Waits for FullScan if the nodes requested are under full scan
         * 
         *****************************************************************************************************/
        void RunPartialScan()
        {
            EventLog partialScanLog = new EventLog("Partial Scan");
            while (runLoops)
            {
                partialScanLog.Buffer("Running the partial scan again.");

                PartialScan(partialScanLog);

                partialScanLog.Flush();
                System.Threading.Thread.Sleep(15000);
            }
        }
        void PartialScan(EventLog eventLog)
        {
            eventLog.Buffer("Reading manual scan requests from database.");
            ScanRequestsTable.ClearBeforeFill = true;
            ScanRequestsTable.Fill(scanRequests);
            eventLog.Buffer("{0} scan request lines read.", scanRequests.Count);

            foreach (OIMNTFS.ScanRequestsRow scanRequest in scanRequests)
            {
                eventLog.Buffer("Processing request for scanning {0}.", scanRequest.FullPath);

                start = DateTime.Now;
                foldercounter = 0;
                entitlementcounter = 0;
                protectedcounter = 0;

                // select the closest node to requested path for scanning
                string sql = string.Format(@"
                SELECT TOP 1 Nodes.*, Filesystems.Depth as maxlevel FROM Nodes
                JOIN TopLevelNodes ON Nodes.TopLevelNodeID = TopLevelNodes.ID
                JOIN Filesystems ON TopLevelNodes.FilesystemID = Filesystems.ID
                WHERE '{0}' LIKE Nodes.FullPath + '%' ORDER BY Level DESC", scanRequest.FullPath);

                SqlDataAdapter dataAdapter = new SqlDataAdapter(sql, scannerConnection);
                DataTable singleNode = new DataTable();
                int rows = dataAdapter.Fill(singleNode);

                eventLog.Buffer("Scanning {0} nodes.", rows);
                foreach (DataRow node in singleNode.Rows)
                {
                    try
                    {
                        eventLog.Buffer("BTW: full scan is working on TopLevelNodeID {0} right now.", FullScanRunningOnTopLevelNode);
                        while ((Int64)node["TopLevelNodeID"] == FullScanRunningOnTopLevelNode)
                        {
                            scannerLog.Write("PartialScan is waiting for full scan above {0} to complete.", node["FullPath"].ToString());
                            System.Threading.Thread.Sleep(5000);
                            // busy wait - mutex on IDs not yet found :-(
                        }
                        PartialScanRunningOnTopLevelNode = (Int64)node["TopLevelNodeID"];
                        // delete node and down

                        SqlCommand delnodes = scannerConnection.CreateCommand();
                        delnodes.CommandText = string.Format("DELETE FROM [OIMNTFS].[dbo].[Nodes] WHERE NodeID = {0}", node["ID"]);
                        delnodes.Parameters.Add("@ID", SqlDbType.BigInt);

                        eventLog.Buffer("Deleting old scan information for {0}...", node["FullPath"]);
                        delnodes.ExecuteNonQuery();

                        // scan from node down to level maxlevel
                        eventLog.Buffer("scanning from {0}", node["FullPath"]);
                        ProcessNode(node["FullPath"].ToString(), (int)node["Level"], (int)node["maxlevel"], (Int64)node["TopLevelNodeID"], (Int64)node["ParentNodeID"], eventLog);
                    }
                    catch (Exception e)
                    {
                        eventLog.Buffer("Exception while scanning {0}: {1}", node["FullPath"], e.Message);
                    }
                    finally
                    {
                        PartialScanRunningOnTopLevelNode = 0;
                    }
                }

            }
        }

        /*****************************************************************************************************
         * StopScanning
         * sets signal for threads to stop and waits for them to complete their current loop
         * 
         *****************************************************************************************************/
        public void StopScanning()
        {
            runLoops = false;

            FullScanWorkerThread.Join();
            PartialScanWorkerThread.Join();

            scannerConnection.Close();
            scannerConnection.Close();

            scannerLog.Buffer("all threads terminated, database connection closed");
        }

        /*****************************************************************************************************
         * ProcessNode
         * start from scanPath and recurse into subfolders
         * 
         *****************************************************************************************************/
        void ProcessNode(OIMNTFSTableAdapters.NodesTableAdapter nodes, string scanPath, int level, int maxlevel, Int64 TopLevelNodeID, Int64 ParentNodeID, EventLog eventLog)
        {
            DateTime start = DateTime.Now;

            DirectoryInfo dInfo = null;
            DirectorySecurity dSecurity = null;
            string fullPath = null;
            string name = null;
            string owner = "<unknown>";
            DateTime lastAccess;
            DateTime lastWrite;
            Boolean isProtected = false;
            Int64 nodeID = 0;

            // check if folder name is too long
            if (scanPath.Length > 248)
            {
                eventLog.Buffer("Path too long: {0} ({1} characters)", scanPath, scanPath.Length);
                return;
            }
            // Check if foldername is in exclusion list
            try
            {
                if (excludeNodes.Select("'" + scanPath.Replace("'", "''") + "' LIKE excludeNode").Length > 0)
                {
                    eventLog.Buffer("Path {0} is in exclusion list.", scanPath);
                    return;
                }
            }
            catch (Exception e)
            {
                eventLog.Buffer("\rFailed to check exclude list for {0}: {1}.", scanPath, e.Message);
                // do not return
            }


            foldercounter++;
            // now read directory information
            try
            {
                dInfo = new DirectoryInfo(scanPath);
                lastAccess = dInfo.LastAccessTimeUtc;
                lastWrite = dInfo.LastWriteTimeUtc;
                fullPath = dInfo.FullName;
                name = dInfo.Name;
            }
            catch (Exception e)
            {
                eventLog.Buffer("Failed to read directory info for {0}: {1}", scanPath, e.Message);
                return;
            }
            // read directory security information
            try
            {
                dSecurity = dInfo.GetAccessControl(AccessControlSections.Owner | AccessControlSections.Access);
                name = dInfo.Name;
            }
            catch (Exception e)
            {
                eventLog.Buffer("Failed to read security info for {0}: {1}", scanPath, e.Message);
                return;
            }

            // now identify owner
            try
            {
                string SID = dSecurity.GetOwner(typeof(System.Security.Principal.SecurityIdentifier)).Value;
                owner = ad.getObjectName(SID);
                isProtected = dSecurity.AreAccessRulesProtected;
            }
            catch (Exception e)
            {
                eventLog.Buffer("Failed to read ownership info for {0}: {1}", scanPath, e.Message);
            }

            if (isProtected)
                protectedcounter++;

            // insert node found into nodes table (previously emptied for related toplevelfolder)
            try
            {
                nodes.Insert(fullPath, name, level, TopLevelNodeID, ParentNodeID, owner, isProtected, lastAccess, lastWrite, DateTime.UtcNow);
                nodeID = (Int64)getNewNodeID.ExecuteScalar();
            }
            catch (Exception e)
            {
                eventLog.Buffer("INSERTing new nodes row into DB failed: {0}", e.Message);
            }

            // analyse all access rules (explicit access rules only, no inherited access rules)
            foreach (FileSystemAccessRule fsar in dSecurity.GetAccessRules(true, false, typeof(System.Security.Principal.SecurityIdentifier)))
            {
                entitlementcounter++;

                string SID = fsar.IdentityReference.Value;
                string objectName = ad.getObjectName(SID);
                string objectClass = ad.getObjectClass(SID);
                string accessRights = fsar.FileSystemRights.ToString();
                string accessType = fsar.AccessControlType.ToString();
                string rulePropagation = fsar.PropagationFlags.ToString();
                string ruleInheritance = fsar.InheritanceFlags.ToString();

                try
                {
                    entitlementsTable.Insert(nodeID, objectName, objectClass, accessRights, accessType, rulePropagation, ruleInheritance, DateTime.UtcNow);
                }
                catch (Exception e)
                {
                    eventLog.Buffer("\rFailed to insert entitlements for {0}\n{1}", objectName, e.Message);
                    return;
                }

                Console.Write("\rLevel {0}, Folders {1}, Entitlements {2}, Protected {3}, Runtime {4}               ", level, foldercounter, entitlementcounter, protectedcounter, (DateTime.Now - start).ToString());
            } // end foreach fsar

            if (level < maxlevel)
            {
                Console.Write("\rLevel {0}, Folders {1}, Entitlements {2}, Protected {3}, next level ...                    ", level, foldercounter, entitlementcounter, protectedcounter);
                string[] subDirectories = null;
                try
                {
                    subDirectories = Directory.GetDirectories(dInfo.FullName);
                }
                catch (Exception e)
                {
                    eventLog.Buffer("unable to read subdirectories of {0}", dInfo.FullName);
                    eventLog.Buffer("{0}", e.Message);
                    return;
                }
                Console.Write("\rLevel {0}, Folders {1}, Entitlements {2}, Protected {3}, Runtime {4}                      ", level, foldercounter, entitlementcounter, protectedcounter, (DateTime.Now - start).ToString());
                foreach (string subdirectory in subDirectories)
                    ProcessNode(nodes, subdirectory, level + 1, maxlevel, TopLevelNodeID, nodeID, eventLog);
            }
        }
    }
}
