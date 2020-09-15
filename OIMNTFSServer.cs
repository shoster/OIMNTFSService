using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Net;
using System.Net.Sockets;


namespace OIMNTFS_Service
{
    public class OIMNTFSServer
    {
        TcpListener listener; 
        IPAddress ipAddress = IPAddress.Any;
        int port;
        bool runLoops = true;
        EventLog eventLog;
        private OIMNTFSPartialScanner OIMNTFSScanner;

        public OIMNTFSServer(OIMNTFSPartialScanner scanner, int listentoport = 16383)
        {
            eventLog = new EventLog("OIMNTFS Server");
            OIMNTFSScanner = scanner;

            port = listentoport;
            Thread listener = new Thread(new ThreadStart(acceptConnections));
            listener.Start();
            eventLog.Write("Server started");
        }

        void acceptConnections ()
        {
            listener = new TcpListener(ipAddress, port);
            eventLog.Write("Listening on port " + port + "...");
            try
            {
                listener.Start();
                while (runLoops)
                {
                    while (!listener.Pending()) { Thread.Sleep(100); }

                    // fork server thread
                    (new Thread(new ThreadStart(processCommands))).Start();

                    Thread.Sleep(100);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Fehler bei Verbindungserkennung", ex);
            }
        }

        void sendText (Socket socket, string text)
        {
            Byte[] bytesSent = Encoding.ASCII.GetBytes(text);
            socket.Send(bytesSent, SocketFlags.None);
        }

        string receiveText(Socket socket)
        {
            Byte[] bytesReceived = new Byte[4096]; 
            int bytes = 0;
            string text = "";

            do
            {
                bytes = socket.Receive(bytesReceived, bytesReceived.Length, SocketFlags.None);
                // kovertiere die Byte Daten in einen string
                eventLog.Write("Received: {0}", Encoding.ASCII.GetString(bytesReceived, 0, bytes));
                text = text + Encoding.ASCII.GetString(bytesReceived, 0, bytes);
            } while (bytes > 0 && !text.Contains("\n"));
            
            return text;
        }

        void processCommands()
        {
            Socket socket = listener.AcceptSocket();
            eventLog.Write("Neue Client-Verbindung (" +
                        "IP: " + socket.RemoteEndPoint + ", " +
                        "Port " + ((IPEndPoint)socket.LocalEndPoint).Port.ToString() + ")");

            sendText(socket, "Nice to meet you!\r\nPlease type path to be scanned and finish with <ENTER>.\r\n\n");
            string scanPath = receiveText(socket);
            sendText(socket, string.Format("\n\rScanning folder \n\r {0} as per your request. Stand by.\r\n\n", scanPath));

            // insert into database for scanning

            try
            {
                socket.Close();
            }
            catch
            {
                eventLog.Write("Verbindung zum Client beendet");
            }
        }
    }
}
