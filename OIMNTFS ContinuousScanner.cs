using System;
using System.Linq;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Security.AccessControl;

namespace OIMNTFS_Service
{
    public class OIMNTFSContinuousScanner
    {
        private OIMNTFSTableAdapters.FilesystemsTableAdapter fileSystemsTable = new OIMNTFSTableAdapters.FilesystemsTableAdapter();
        private OIMNTFS.FilesystemsDataTable fileSystems = new OIMNTFS.FilesystemsDataTable();

        private OIMNTFSTableAdapters.TopLevelNodesTableAdapter topLevelNodesTable = new OIMNTFSTableAdapters.TopLevelNodesTableAdapter();
        private OIMNTFS.TopLevelNodesDataTable topLevelNodes = new OIMNTFS.TopLevelNodesDataTable();

        private OIMNTFSTableAdapters.ExcludeNodesTableAdapter excludeNodesTable = new OIMNTFSTableAdapters.ExcludeNodesTableAdapter();
        private OIMNTFS.ExcludeNodesDataTable excludeNodes = new OIMNTFS.ExcludeNodesDataTable();

        private OIMNTFSTableAdapters.ScanRequestsTableAdapter ScanRequestsTable = new OIMNTFSTableAdapters.ScanRequestsTableAdapter();
        private OIMNTFS.ScanRequestsDataTable scanRequests = new OIMNTFS.ScanRequestsDataTable();

        private OIMNTFSTableAdapters.NodesTableAdapter nodesTable = new OIMNTFSTableAdapters.NodesTableAdapter();
        private OIMNTFSTableAdapters.EntitlementsTableAdapter entitlementsTable = new OIMNTFSTableAdapters.EntitlementsTableAdapter();

        private long _EntitlementCounter = 0;
        private long _FolderCounter = 0;
        private long _ProtectedCounter = 0;

        private OIMNTFSService service;
        private EventLog eventLog;
        private DateTime start = DateTime.Now;
        private SqlConnection conn;
        private SqlCommand getNewNodeID;
        private Int64 _CurrentTopLevelNode = 0;

        private bool runLoops = true;
        private System.Threading.Thread workerThread;

        public OIMNTFSContinuousScanner(string cs, OIMNTFSService svc)
        {
            eventLog = new EventLog("OIMNTFS Continuous Scanner");

            try
            {
                eventLog.Buffer("Initializing OIMNTFS Scanner instance");
                this.service = svc;

                eventLog.Buffer("Trying to open '{0}'.", cs);
                conn = new SqlConnection(cs);
                conn.Open();

                getNewNodeID = new SqlCommand("SELECT CAST(ISNULL(IDENT_CURRENT('Nodes'), 0) as bigint)", conn);

                eventLog.Buffer("Reading data tables from database {0}...", conn.Database);

                fileSystemsTable.Connection = conn;
                fileSystemsTable.Fill(fileSystems);
                eventLog.Buffer("fileSystems table filled.");

                excludeNodesTable.Connection = conn;
                excludeNodesTable.Fill(excludeNodes);
                eventLog.Buffer("excludeNodes table filled.");

                topLevelNodesTable.Connection = conn;
                topLevelNodesTable.Fill(topLevelNodes);
                eventLog.Buffer("topLevelNodes table filled.");

                ScanRequestsTable.Connection = conn;
                ScanRequestsTable.Fill(scanRequests);

                nodesTable.Connection = conn;
                // no fill - target only

                entitlementsTable.Connection = conn;
                // no fill - target only

            }
            catch (Exception e)
            {
                eventLog.Buffer("Init failed: {0}", e.Message);

                if (conn.State == ConnectionState.Open) conn.Close();

                if (workerThread.ThreadState == System.Threading.ThreadState.Running) workerThread.Abort();
                eventLog.Buffer("Worker thread shut down.");
            }
            finally
            {
                eventLog.Buffer("Continuous scanner initialization completed.");
                eventLog.Flush();
            }
        }

        public void RunWorkerThread()
        {
            workerThread = new System.Threading.Thread(new System.Threading.ThreadStart(RunFullScan));
            workerThread.Start();
        }
        public void StopWorkerThread()
        {
            runLoops = false;

            workerThread.Join();
            conn.Close();

            eventLog.Buffer("thread terminated, database connection closed");
        }
        public Int64 CurrentTopLevelNode ()
        {
            return _CurrentTopLevelNode;
        }

        //
        // Private functions
        //
        private void RunFullScan()
        {
            while (runLoops)
            {
                FullScan();
                System.Threading.Thread.Sleep(60000);
            }
        }

        private void FullScan()
        {
            eventLog.Buffer("Refreshing file systems table.");
            fileSystemsTable.ClearBeforeFill = true;
            fileSystemsTable.Fill(fileSystems);
            eventLog.Buffer("{0} file systems read.", fileSystems.Count);

            eventLog.Buffer("Refreshing exclude nodes table.");
            excludeNodesTable.ClearBeforeFill = true;
            excludeNodesTable.Fill(excludeNodes);
            eventLog.Buffer("{0} exclude nodes read.", excludeNodes.Count);

            foreach (OIMNTFS.FilesystemsRow fileSystem in fileSystems.Rows)
            {
                eventLog.Buffer("Reading file system {0}", fileSystem.DriveRoot);
                service.networkDrive.LocalDrive = fileSystem.DriveRoot.Substring(0, 2);
                service.networkDrive.Persistent = false;
                service.networkDrive.SaveCredentials = false;
                service.networkDrive.Force = true;
                service.networkDrive.ShareName = "\\\\" + fileSystem.ProviderIP + "\\" + fileSystem.Share;
                eventLog.Buffer("Mapping drive {0} to {1}", service.networkDrive.LocalDrive, service.networkDrive.ShareName);
                try
                {
                    switch (fileSystem.Type)
                    {
                        case 0:
                            service.networkDrive.MapDrive();
                            break;
                        case 1:
                            service.networkDrive.MapDrive(fileSystem.User, fileSystem.Password);
                            break;
                        default:
                            service.networkDrive.MapDrive(fileSystem.User, fileSystem.Password);
                            break;
                    }
                }
                catch (Exception e)
                {
                    eventLog.Buffer("unable to map drive {0} to {1}", service.networkDrive.LocalDrive, service.networkDrive.ShareName);
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
                        owner = service.adCache.getObjectName(ownerSID);
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
                    // if scanner is working on same top level node, wait for it to complete
                    while (service.scanner.CurrentTopLevelNode() == topLevelNode.ID)
                    {
                        this.eventLog.Write("FullScan is waiting for single scan below {0} to complete.", topLevelNode.FullPath);
                        System.Threading.Thread.Sleep(5000);
                        // busy wait :-(
                    }

                    _CurrentTopLevelNode = topLevelNode.ID;

                    start = DateTime.Now;
                    _FolderCounter = 0;
                    _EntitlementCounter = 0;
                    _ProtectedCounter = 0;

                    ProcessNode(topLevelNode.FullPath, 1, topLevelNode.ScanDepth, topLevelNode.ID, 0);
                    eventLog.Buffer("Done.");

                    eventLog.Buffer("Updating database...");
                    try
                    {
                        // first delete old values
                        eventLog.Buffer("Deleting old scan information for {0}...", topLevelNode.FullPath);
                        SqlCommand delnodes = conn.CreateCommand();
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
                    eventLog.Buffer("{0} completed on {1:hh:mm:ss}.\n{2} folders read ({3:0.0} folders per second)\n", topLevelNode.FullPath, DateTime.Now, _FolderCounter, _FolderCounter / (DateTime.Now - start).TotalSeconds, _FolderCounter);

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
                    (new SqlCommand(cmdtext, conn)).ExecuteNonQuery();

                    eventLog.Flush();
                }
                catch (Exception e)
                {
                    this.eventLog.Write("Exception during scan below {0}: {1}", topLevelNode.FullPath, e.Message);
                    eventLog.Buffer("Exception during scan below {0}: {1}", topLevelNode.FullPath, e.Message);
                }
                finally
                {
                    _CurrentTopLevelNode = 0;
                    eventLog.Flush();
                }
            }
        }

        private void ProcessNode(string scanPath, int level, int maxlevel, Int64 TopLevelNodeID, Int64 ParentNodeID)
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


            _FolderCounter++;
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
                owner = service.adCache.getObjectName(SID);
                isProtected = dSecurity.AreAccessRulesProtected;
            }
            catch (Exception e)
            {
                eventLog.Buffer("Failed to read ownership info for {0}: {1}", scanPath, e.Message);
            }

            if (isProtected)
                _ProtectedCounter++;

            // insert node found into nodes table (previously emptied for related toplevelfolder)
            try
            {
                nodesTable.Insert(fullPath, name, level, TopLevelNodeID, ParentNodeID, owner, isProtected, lastAccess, lastWrite, DateTime.UtcNow);
                nodeID = (Int64)getNewNodeID.ExecuteScalar();
            }
            catch (Exception e)
            {
                eventLog.Buffer("INSERTing new nodes row into DB failed: {0}", e.Message);
            }

            // analyse all access rules (explicit access rules only, no inherited access rules)
            foreach (FileSystemAccessRule fsar in dSecurity.GetAccessRules(true, false, typeof(System.Security.Principal.SecurityIdentifier)))
            {
                _EntitlementCounter++;

                string SID = fsar.IdentityReference.Value;
                string objectName = service.adCache.getObjectName(SID);
                string objectClass = service.adCache.getObjectClass(SID);
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

                Console.Write("\rLevel {0}, Folders {1}, Entitlements {2}, Protected {3}, Runtime {4}               ", level, _FolderCounter, _EntitlementCounter, _ProtectedCounter, (DateTime.Now - start).ToString());
            } // end foreach fsar

            if (level < maxlevel)
            {
                Console.Write("\rLevel {0}, Folders {1}, Entitlements {2}, Protected {3}, next level ...                    ", level, _FolderCounter, _EntitlementCounter, _ProtectedCounter);
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
                Console.Write("\rLevel {0}, Folders {1}, Entitlements {2}, Protected {3}, Runtime {4}                      ", level, _FolderCounter, _EntitlementCounter, _ProtectedCounter, (DateTime.Now - start).ToString());
                foreach (string subdirectory in subDirectories)
                    ProcessNode(subdirectory, level + 1, maxlevel, TopLevelNodeID, nodeID);
            }
        }
    }
}
