using System;
using System.Linq;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Security.AccessControl;

namespace OIMNTFS_Service
{
    public class OIMNTFSPartialScanner
    {
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
        private DateTime _Start = DateTime.Now;
        private SqlConnection conn;
        private SqlCommand getNewNodeID;
        private Int64 _CurrentTopLevelNode = 0;

        private bool runLoops = true;
        private System.Threading.Thread workerThread;

        public OIMNTFSPartialScanner(string cs, OIMNTFSService svc)
        {
            eventLog = new EventLog("OIMNTFS Partial Scanner");

            try
            {
                eventLog.Buffer("Initializing OIMNTFS Scanner instance");
                this.service = svc;

                eventLog.Buffer("Trying to open '{0}'.", cs);
                conn = new SqlConnection(cs);
                conn.Open();

                getNewNodeID = new SqlCommand("SELECT CAST(ISNULL(IDENT_CURRENT('Nodes'), 0) as bigint)", conn);

                eventLog.Buffer("Connecting data tables from database {0}...", conn.Database);

                ScanRequestsTable.Connection = conn;
                nodesTable.Connection = conn;
                entitlementsTable.Connection = conn;
                eventLog.Buffer("scanRequests, nodes and entitlements tables connected.");

                excludeNodesTable.Connection = conn;
                excludeNodesTable.Fill(excludeNodes);
                eventLog.Buffer("excludeNodes table filled.");
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
                eventLog.Flush();
            }
        }

        public void RunWorkerThread()
        {
            workerThread = new System.Threading.Thread(new System.Threading.ThreadStart(RunPartialScan));
            workerThread.Start();
        }
        public void StopWorkerThread()
        {
            runLoops = false;

            workerThread.Join();
            conn.Close();

            eventLog.Buffer("thread terminated, database connection closed");
        }

        public Int64 CurrentTopLevelNode()
        {
            return _CurrentTopLevelNode;
        }

        //
        // Private functions
        //
        void RunPartialScan()
        {
            while (runLoops)
            {
                eventLog.Buffer("Running the partial scan again.");

                eventLog.Buffer("Refreshing exclude nodes table.");
                excludeNodesTable.ClearBeforeFill = true;
                excludeNodesTable.Fill(excludeNodes);
                eventLog.Buffer("{0} exclude nodes read.", excludeNodes.Count);

                eventLog.Buffer("Reading manual scan requests from database.");
                ScanRequestsTable.ClearBeforeFill = true;
                ScanRequestsTable.Fill(scanRequests);
                eventLog.Buffer("{0} scan request lines read.", scanRequests.Count);

                foreach (OIMNTFS.ScanRequestsRow scanRequest in scanRequests)
                {
                    eventLog.Buffer("Processing request for scanning {0}.", scanRequest.FullPath);

                    _Start = DateTime.Now;
                    _FolderCounter = 0;
                    _EntitlementCounter = 0;
                    _ProtectedCounter = 0;

                    // select the closest node to requested path for scanning
                    string sql = string.Format(@"
                    SELECT TOP 1 Nodes.*, Filesystems.Depth as maxlevel FROM Nodes
                    JOIN TopLevelNodes ON Nodes.TopLevelNodeID = TopLevelNodes.ID
                    JOIN Filesystems ON TopLevelNodes.FilesystemID = Filesystems.ID
                    WHERE '{0}' LIKE Nodes.FullPath + '%' ORDER BY Level DESC", scanRequest.FullPath);

                    SqlDataAdapter dataAdapter = new SqlDataAdapter(sql, conn);
                    DataTable singleNode = new DataTable();
                    int rows = dataAdapter.Fill(singleNode);

                    eventLog.Buffer("Scanning {0} nodes.", rows);
                    foreach (DataRow node in singleNode.Rows)
                    {
                        try
                        {
                            eventLog.Buffer("BTW: full scan is working on TopLevelNodeID {0} right now.", service.continuousScanner.CurrentTopLevelNode());
                            while ((Int64)node["TopLevelNodeID"] == service.continuousScanner.CurrentTopLevelNode())
                            {
                                this.eventLog.Write("PartialScan is waiting for full scan above {0} to complete.", node["FullPath"].ToString());
                                System.Threading.Thread.Sleep(5000);
                                // busy wait - mutex on IDs not yet found :-(
                            }
                            _CurrentTopLevelNode = (Int64)node["TopLevelNodeID"];
                            // delete node and down

                            eventLog.Buffer("Deleting old scan information for {0}...", node["FullPath"]);
                            SqlCommand delnodes = conn.CreateCommand();
                            delnodes.CommandText = string.Format("DELETE FROM [OIMNTFS].[dbo].[Nodes] WHERE NodeID = {0}", node["ID"]);
                            delnodes.ExecuteNonQuery();

                            // scan from node down to level maxlevel
                            eventLog.Buffer("scanning from {0}", node["FullPath"]);
                            ProcessNode(node["FullPath"].ToString(), (int)node["Level"], (int)node["maxlevel"], (Int64)node["TopLevelNodeID"], (Int64)node["ParentNodeID"]);
                        }
                        catch (Exception e)
                        {
                            eventLog.Buffer("Exception while scanning {0}: {1}", node["FullPath"], e.Message);
                        }
                        finally
                        {
                            _CurrentTopLevelNode = 0;
                        }
                    }
                }

                eventLog.Flush();
                System.Threading.Thread.Sleep(15000);
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
