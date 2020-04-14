﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using System.IO;

namespace Chetch.Messaging
{
    public class TCPServer : Server
    {
        protected IPAddress IP;
        protected int BasePort { get; set; } = 11000;

        public TCPServer(IPAddress ipAddr, int basePort) : base()
        {
            if (ipAddr == null)
            {
                IPHostEntry ipHostInfo = Dns.GetHostEntry(Dns.GetHostName());
                ipAddr = ipHostInfo.AddressList[1]; //use IPv4
            }
            IP = ipAddr;
            BasePort = basePort;
        }

        protected override void InitialisePrimaryConnection()
        {
            base.InitialisePrimaryConnection();
            PrimaryConnection.ConnectionTimeout = -1;
            PrimaryConnection.ActivityTimeout = 10000;
        }

        public TCPServer(int basePort) : this(null, basePort)
        {

        }

        override public Connection CreatePrimaryConnection()
        {
            String id = CreateNewConnectionID();
            TCPListener listener = new TCPListener(id, IP, BasePort);
            return listener;
        }

        override public Connection CreateConnection(Message message)
        {
            int port = -1;
            foreach (var cnn in Connections.Values)
            {
                if (cnn.State == Connection.ConnectionState.CLOSED)
                {
                    port = ((TCPListener)cnn).Port;
                    break;
                }
            }

            if (port == -1) port = BasePort + Connections.Count + 1;

            String id = CreateNewConnectionID();
            TCPListener listener = new TCPListener(id, IP, port, -1, -1);
            return listener;
        }

        override protected Message CreateRequestResponse(Message request, Connection newCnn)
        {
            var response = base.CreateRequestResponse(request, newCnn);
            if (newCnn != null)
            {
                TCPListener listener = (TCPListener)newCnn;
                response.AddValue("Port", listener.Port);
            }
            response.DefaultEncoding = MessageEncoding.JSON;
            return response;
        }
    }
}