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
        
        public TCPClientManager(IPAddress defaultIP, int defaultPort) : base()
        {
            if (defaultIP == null)
            {
                IPHostEntry ipHostInfo = Dns.GetHostEntry(Dns.GetHostName());
                defaultIP = ipHostInfo.AddressList[1]; //use IPv4
            }
            Servers["default"] = defaultIP + ":" + defaultPort;
        }

        public TCPClientManager(int basePort) : this(null, basePort)
        {
            //empty
        }

        protected override void InitialisePrimaryConnection(String connectionString)
        {
            base.InitialisePrimaryConnection(connectionString);
            PrimaryConnection.ConnectionTimeout = 10000;
            PrimaryConnection.ActivityTimeout = 5000;
        }

        override public Connection CreatePrimaryConnection(String connectionString)
        {
            String id = CreateNewConnectionID();
            TCPClient client = new TCPClient(id, connectionString);
            return client;
        }

        override public Connection CreateConnection(Message message)
        {
            if (message != null && message.Type == MessageType.CONNECTION_REQUEST_RESPONSE)
            {
                int port = message.GetInt("Port");
                IPAddress ip = IPAddress.Parse(message.GetString("IP"));
                String id = CreateNewConnectionID();
                TCPClient client = new TCPClient(id, ip, port, -1, -1);
                return client;
            } else
            {
                throw new Exception("TCPClientManager::CreateConnection: unable to createc connection from passed message");
            }
            
        }
    } //end TCPClientManager class
}
