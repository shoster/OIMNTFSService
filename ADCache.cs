using System;
using System.Collections.Generic;
using System.DirectoryServices;

namespace OIMNTFS_Service
{
    class ADCache
    {
        private EventLog eventLog;
        public struct Properties
        {
            public Properties(string sid, SearchResult entry)
            {
                samAccountName = entry.Properties["samAccountName"][0].ToString();
                canonicalName = entry.Properties["canonicalName"][0].ToString();
                SID = sid;
                distinguishedName = entry.Properties["distinguishedName"][0].ToString();
                path = entry.Path;

                objectClass = entry.Properties["objectClass"][entry.Properties["objectClass"].Count - 1].ToString();
                switch (samAccountName)
                {
                    case "Administrators":
                        objectClass = "system";
                        break;
                    case "SPT_Search_filer":
                        objectClass = "system";
                        break;
                    case "SPP_Search_Filer":
                        objectClass = "system";
                        break;
                    case "GF-RD021-DA":
                        objectClass = "system";
                        break;
                    default:
                        if (samAccountName.StartsWith("SPT_Search"))
                            objectClass = "system";
                        else if (samAccountName.StartsWith("SPP_Search"))
                            objectClass = "system";
                        else if (samAccountName.StartsWith("SPP_Search"))
                            objectClass = "system";
                        else if (samAccountName.StartsWith("Domain "))
                            objectClass = "system";
                        break;
                }
            }
            public string samAccountName { get; set; }
            public string objectClass { get; set; }
            public string canonicalName { get; set; }
            public string SID { get; set; }
            public string path { get; set; }
            public string distinguishedName { get; set; }
        }
        static Dictionary<string, Properties> cache = new Dictionary<string, Properties>(200000);
        public ADCache(string path)
        {
            eventLog = new EventLog("OIMNTFS ADCache");
            eventLog.Buffer("Reading directory information.");
            try
            {
                DateTime start = DateTime.Now;
                string[] properties = new string[] { "samAccountName", "objectClass", "canonicalName", "objectSID", "distinguishedName" };
                string filter = "(|(objectClass=user)(objectClass=group))";

                eventLog.Buffer("Connecting to {0}...", path);
                DirectoryEntry directoryEntry = null;

                try
                {
                    //directoryEntry = new DirectoryEntry(path);
                    directoryEntry = new DirectoryEntry();
                    directoryEntry.RefreshCache(properties);
                }
                catch
                {
                    eventLog.Buffer("Current user context is not allowed to read from AD.");
                }

                Console.WriteLine("Reading all ad user and group objects...");
                DirectorySearcher ds = new System.DirectoryServices.DirectorySearcher(directoryEntry, filter, properties);
                ds.SearchScope = SearchScope.Subtree;
                ds.CacheResults = true;
                ds.ClientTimeout = TimeSpan.FromMinutes(120);
                ds.PageSize = 100;

                SearchResultCollection entries = ds.FindAll();
                foreach (SearchResult entry in entries)
                {
                    System.Security.Principal.SecurityIdentifier binSID = new System.Security.Principal.SecurityIdentifier((byte[])entry.Properties["objectSID"][0], 0);
                    string sid = binSID.ToString();
                    string samAccountName = entry.Properties["samAccountName"][0].ToString();

                    if (!cache.ContainsKey(sid))
                        cache.Add(sid, new Properties(sid, entry));
                }
                eventLog.Buffer("{0} objects found. Loading AD took actually {1}", cache.Count, (DateTime.Now - start).ToString());
            }
            catch (Exception e)
            {
                eventLog.Buffer("Reading AD failed: {0}", e.Message);
                //throw new Exception("Reading AD failed.");
            }
            eventLog.Flush();
        }

        public int Count()
        {
            return cache.Count;
        }

        public bool isADObject(string samAccountName)
        {
            return cache.ContainsKey(samAccountName);
        }

        public string getObjectClass(string SID)
        {
            Properties properties;

            if (cache.TryGetValue(SID, out properties))
                return properties.objectClass;
            else
                return "deleted";
        }
        public string getObjectName(string SID)
        {
            Properties properties;

            if (cache.TryGetValue(SID, out properties))
                return properties.samAccountName;
            else
                return SID;
        }

        public Properties getProperties(string objectName)
        {
            Properties properties;

            if (cache.TryGetValue(objectName, out properties))
                return properties;
            else
                return new Properties();
        }
    }
}
