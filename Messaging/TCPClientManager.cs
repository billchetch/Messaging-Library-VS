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
    public class TCPClientManager : ClientManager<TCPClient>
    {
        
        public TCPClientManager() : base()
        {
            //empty
        }

        public void AddServer(String serverName, IPAddress ip, int port)
        {
            AddServer(serverName, TCPServer.ConnectionString(ip, port));
        }
        
    } //end TCPClientManager class
}
