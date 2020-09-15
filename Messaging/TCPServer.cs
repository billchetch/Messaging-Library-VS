using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using System.IO;
using System.Net.NetworkInformation;
using System.Diagnostics;

namespace Chetch.Messaging
{
    public class TCPServer : Server
    {
        private static List<IPAddress> getIP4Addresses(String hostName)
        {
            List<IPAddress> ips = new List<IPAddress>();
            IPHostEntry ipHostInfo = Dns.GetHostEntry(hostName);
            foreach (IPAddress addr in ipHostInfo.AddressList)
            {
                if (addr.AddressFamily == AddressFamily.InterNetwork)
                {
                    ips.Add(addr);
                }
            }
            return ips;
        }

        public static List<IPAddress> LanIPs
        {
            get
            {
                return getIP4Addresses(Dns.GetHostName());
            }
        }


        public static IPAddress LanIP
        {
            get
            {
                List<IPAddress> addr = LanIPs;
                return addr[0]; //use IPv4
            }
        }

        public static IPAddress LocalIP
        {
            get
            {
                List<IPAddress> addr = getIP4Addresses("localhost");
                return addr[0];            }
        }

        public static String LocalCS(int port)
        {
            return ConnectionString(LocalIP, port);
        }

        public static String LanCS(int port)
        {
            return ConnectionString(LanIP, port);
        }
        public static String ConnectionString(IPAddress ip, int port)
        {
            return String.Format("{0}:{1}", ip.ToString(), port);
        }
        public static String ConnectionString(String ip, int port)
        {
            return String.Format("{0}:{1}", ip, port);
        }
        
        protected int BasePort { get; set; } = 12000;

        public TCPServer(int basePort) : base()
        {
            BasePort = basePort;
        }


        private void HandleAddressChange(object sender, EventArgs e)
        {
            Tracing?.TraceEvent(TraceEventType.Warning, 1000, "Network address change occurred");

            //we need to test if there is a new LanIP
            var cnns = new List<Connection>(SecondaryConnections);
            cnns.Add(PrimaryConnection);
            var ipToCheck = LanIP.ToString();
            bool alreadyListening = false;
            foreach (var cnn in cnns)
            {
                var l = (TCPListener)cnn;
                if (l.IP.ToString().Equals(ipToCheck))
                {
                    alreadyListening = true;
                    Tracing?.TraceEvent(TraceEventType.Information, 1000, "Already listening on {0}", ipToCheck);
                    break;
                }
            }

            if (!alreadyListening)
            {
                Tracing?.TraceEvent(TraceEventType.Information, 1000, "Adding secondary listener for IP {0}", ipToCheck);
                var cnn = CreatePrimaryConnection(ipToCheck);
                SecondaryConnections.Add((ServerConnection)cnn);
                if (IsRunning)
                {
                    Tracing?.TraceEvent(TraceEventType.Information, 1000, "Opening secondary listener for IP {0}", ipToCheck);
                    cnn.Open();
                }
            }
        }

        public override void Start()
        {
            SecondaryConnections.Clear();

            List<IPAddress> ips = LanIPs;
            foreach (IPAddress addr in ips)
            {
                var cnn = CreatePrimaryConnection(addr.ToString());
                SecondaryConnections.Add((ServerConnection)cnn);
            }

            base.Start();

            NetworkChange.NetworkAddressChanged += new NetworkAddressChangedEventHandler(HandleAddressChange);
        }

        public override void Stop(bool waitForThreads = true)
        {
            try
            {
                base.Stop(waitForThreads);
            } finally
            {
                NetworkChange.NetworkAddressChanged -= HandleAddressChange;
            }
        }

        override public Connection CreatePrimaryConnection(String connectionString, Connection newCcnn = null)
        {
            String id = CreateNewConnectionID();
            IPAddress ip;
            if(connectionString == null)
            {
                ip = LocalIP;
            } else
            {
                ip = IPAddress.Parse(connectionString);
            }

            TCPListener listener = new TCPListener(id, ip, BasePort);

            return base.CreatePrimaryConnection(connectionString, listener);
        }

        override public Connection CreateConnection(Message request, Connection requestingCnn, Connection newCnn = null)
        {

            IPAddress ip = ((TCPListener)requestingCnn).IP;
            List<int> usedPorts = new List<int>();
            foreach (var ccnn in Connections.Values)
            {
                usedPorts.Add(((TCPListener)ccnn).Port);
            }
            usedPorts.Sort();
            int port = BasePort + 1;
            foreach(var p in usedPorts)
            {
                if(p > port)
                {
                    break;
                }
                else
                {
                    port++;
                }
            }
            
            String id = CreateNewConnectionID();
            TCPListener listener = new TCPListener(id, ip, port, -1, -1);
            return base.CreateConnection(request, requestingCnn, listener);
        }

        override protected Message CreateRequestResponse(Message request, Connection newCnn, Connection oldCnn = null)
        {
            var response = base.CreateRequestResponse(request, newCnn, oldCnn);
            TCPListener listener = newCnn != null ? (TCPListener)newCnn : (oldCnn != null ? (TCPListener)oldCnn: null);
            if (listener != null)
            {
                response.AddValue("Port", listener.Port);
                response.AddValue("IP", listener.IP.ToString());
            }
            response.DefaultEncoding = MessageEncoding.JSON;
            return response;
        }
    }
}
