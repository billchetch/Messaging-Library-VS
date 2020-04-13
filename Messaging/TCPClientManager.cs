using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using System.IO;

namespace Chetch.Messaging
{
    public class TCPClientManager : ClientManager
    {
        protected IPAddress IP;
        protected int BasePort { get; set; } = 11000;

        public TCPClientManager(IPAddress ipAddr, int basePort) : base()
        {
            if (ipAddr == null)
            {
                IPHostEntry ipHostInfo = Dns.GetHostEntry(Dns.GetHostName());
                ipAddr = ipHostInfo.AddressList[1]; //use IPv4
            }
            IP = ipAddr;
            BasePort = basePort;
        }

        public TCPClientManager(int basePort) : this(null, basePort)
        {
            //empty
        }

        protected override void InitialisePrimaryConnection()
        {
            base.InitialisePrimaryConnection();
            PrimaryConnection.ConnectionTimeout = 10000;
            PrimaryConnection.ActivityTimeout = 5000;
        }

        override public Connection CreatePrimaryConnection()
        {
            String id = CreateNewConnectionID();
            TCPClient client = new TCPClient(id, IP, BasePort);
            return client;
        }

        override public Connection CreateConnection(Message message)
        {
            int port = BasePort;
            IPAddress ipAddr = IP;
            if (message != null && message.Type == MessageType.CONNECTION_REQUEST_RESPONSE)
            {
                port = message.GetInt("Port");
                //TODO: work out how to send IPAddress info
            }
            String id = CreateNewConnectionID();
            TCPClient client = new TCPClient(id, IP, port, -1, -1);
            return client;
        }
    }
}
