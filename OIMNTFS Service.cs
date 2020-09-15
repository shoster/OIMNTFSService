using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.ServiceProcess;
using System.Timers;
using System.Runtime.InteropServices;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.DirectoryServices;
using System.Security.AccessControl;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;

namespace OIMNTFS_Service
{
    public partial class OIMNTFSService : ServiceBase
    {
        public enum ServiceState
        {
            SERVICE_STOPPED = 0x00000001,
            SERVICE_START_PENDING = 0x00000002,
            SERVICE_STOP_PENDING = 0x00000003,
            SERVICE_RUNNING = 0x00000004,
            SERVICE_CONTINUE_PENDING = 0x00000005,
            SERVICE_PAUSE_PENDING = 0x00000006,
            SERVICE_PAUSED = 0x00000007,
        };

        [StructLayout(LayoutKind.Sequential)]
        public struct ServiceStatus
        {
            public int dwServiceType;
            public ServiceState dwCurrentState;
            public int dwControlsAccepted;
            public int dwWin32ExitCode;
            public int dwServiceSpecificExitCode;
            public int dwCheckPoint;
            public int dwWaitHint;
        };

        [DllImport("advapi32.dll", SetLastError = true)]
        private static extern bool SetServiceStatus(System.IntPtr handle, ref ServiceStatus serviceStatus);

        public OIMNTFSPartialScanner scanner;
        public OIMNTFSContinuousScanner continuousScanner;
        public OIMNTFSServer server;
        public ADCache adCache;
        public NetworkDrive networkDrive;

        private EventLog eventLog;
        public OIMNTFSService()
        {
            InitializeComponent();
            eventLog = new EventLog("OIMNTFS Service");
            eventLog.Write("Service initialized.");
        }

        protected override void OnStart(string[] args)
        {
            eventLog.Buffer("Starting up service <OIMNTFS Service>");

            // Update the service state to Start Pending.
            ServiceStatus serviceStatus = new ServiceStatus();
            serviceStatus.dwCurrentState = ServiceState.SERVICE_START_PENDING;
            serviceStatus.dwWaitHint = 100000;
            SetServiceStatus(this.ServiceHandle, ref serviceStatus);
            eventLog.Buffer("SERVICE_START_PENDING");

            // Preparation: read AD
            // todo: offline AD cache
            this.adCache = new ADCache("LDAP://10.112.128.3/DC=nrwbanki,DC=de");

            // Preparation: unmap all network drives
            string[] drives = Directory.GetLogicalDrives();
            NetworkDrive networkDrive = new NetworkDrive();
            networkDrive.Persistent = true;
            networkDrive.SaveCredentials = true;
            networkDrive.Force = true;

            foreach (string drive in drives)
            {
                try
                {
                    networkDrive.LocalDrive = drive;
                    networkDrive.UnMapDrive();
                    eventLog.Buffer("Drive {0} mapping removed (net use {0} /d)", drive);
                }
                catch (Exception e)
                {
                    if (e.HResult != -2147467259)
                    {
                        eventLog.Buffer("unable to unmap {0}", drive);
                        eventLog.Buffer("Exception was: {0}", e.ToString());
                    }
                }
            }
            eventLog.Buffer("All network drives unmapped.");

            // Preparation: open database and read information
            string connectionString;

            switch (System.Environment.GetEnvironmentVariable("USERDNSDOMAIN"))
            {
                case "NRWBANKI.DE":
                    connectionString = "Data Source=10.112.133.87;Initial Catalog=oimntfs;User Id = oimntfsdbo; Password = bbGcmcZlkL8FYnsCN4j4";
                    eventLog.Buffer("Running in PROD mode.");
                    break;
                case "NRWBANK.QS":
                    connectionString = "Data Source=10.112.149.4;Initial Catalog=oimntfs;User Id = oimntfsdbo; Password = HbLjSEsgv/9ctvj2pYosOJT7UPVpid3qdJP5RPBVbG8=";
                    eventLog.Buffer("Running in QS mode.");
                    break;
                case "NRWBANK.DEV":
                    connectionString = "Data Source=10.112.139.4;Initial Catalog=oimntfs;User Id = oimntfsdbo; Password = HbLjSEsgv/9ctvj2pYosOJT7UPVpid3qdJP5RPBVbG8=";
                    eventLog.Buffer("Running in DEV mode.");
                    break;
                default:
                    eventLog.Buffer("Unknown environment in domain {0}. Exiting.", System.Environment.UserDomainName);
                    eventLog.Flush();
                    throw (new Exception("Unkown environment."));
            }

            try
            {
                scanner = new OIMNTFSPartialScanner(connectionString, this);
                continuousScanner = new OIMNTFSContinuousScanner(connectionString, this);
                eventLog.Buffer("Connection created and data read from {0}.", connectionString);

                continuousScanner.RunWorkerThread();
                scanner.RunWorkerThread();
                eventLog.Buffer("Worker threads started.");

                //server = new OIMNTFSServer(scanner, 16383);
                //eventLog.Buffer("Server started.");

                // Update the service state to Running.
                serviceStatus.dwCurrentState = ServiceState.SERVICE_RUNNING;
                SetServiceStatus(this.ServiceHandle, ref serviceStatus);
                eventLog.Buffer("SERVICE_RUNNING");

                // Set up a timer that triggers every minute.
                Timer timer = new Timer();
                timer.Interval = 60000; // 60 seconds
                timer.Elapsed += new ElapsedEventHandler(this.OnTimer);
                timer.Start();

                eventLog.Flush();
            }
            catch (Exception e)
            {
                eventLog.Buffer("Starting scanners failed: {0}", e.Message);
                eventLog.Buffer(e.StackTrace);
                eventLog.Flush();

                new Exception("Unable to start scanners", e);
            }
        }

        protected override void OnStop()
        {
            eventLog.Write("In OnStop.");

            // Update the service state to Stop Pending.
            ServiceStatus serviceStatus = new ServiceStatus();
            serviceStatus.dwCurrentState = ServiceState.SERVICE_STOP_PENDING;
            serviceStatus.dwWaitHint = 100000;
            SetServiceStatus(this.ServiceHandle, ref serviceStatus);

            eventLog.Write("Trying to stop scanner threads");
            scanner.StopWorkerThread();
            continuousScanner.StopWorkerThread();

            // Update the service state to Stopped.
            serviceStatus.dwCurrentState = ServiceState.SERVICE_STOPPED;
            SetServiceStatus(this.ServiceHandle, ref serviceStatus);
        }
        protected override void OnContinue()
        {
            eventLog.Write("In OnContinue.");
        }
        public void OnTimer(object sender, ElapsedEventArgs args)
        {
            // TODO: Insert monitoring activities here.
        }
    }
}
