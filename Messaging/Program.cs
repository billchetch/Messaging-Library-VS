using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using System.IO;
using System.Web.Script;
using Chetch.Utilities;
using Chetch.Application;
using Chetch.Messaging;

namespace SocketServer
{
    
    class Program
    {

        const int CONNECTION_REQUEST_PORT = 12000;
        
        static void HandleServerError(Connection cnn, Exception e)
        {
            Console.WriteLine("HandleServerError: " + cnn.ID + " " + e.Message);
        }

        static void HandleServerMessage(Connection cnn, Message message)
        {
            Console.WriteLine("HandleServerMessage: " + cnn.ID + " " + message.ToString());
        }

        static void HandleClientError(Connection cnn, Exception e)
        {
            Console.WriteLine("HandleClientError: " + cnn.ID + " " + e.Message);
        }

        static void HandleClientMessage(Connection cnn, Message message)
        {
            Console.WriteLine("HandleClientMessage: " + cnn.ID + " " + message.ToString());
        }

        static void Main(string[] args)
        {
            TCPServer server = new TCPServer(CONNECTION_REQUEST_PORT);
            server.HandleError += HandleServerError;
            server.HandleMessage += HandleServerMessage;
            Console.WriteLine("Starting server...");
            server.Start();
            Console.WriteLine("Started server");
            
            //Console.WriteLine("Press a key to create client manager");
            //Console.ReadKey(true);
            TCPClientManager clientMgr = new TCPClientManager(CONNECTION_REQUEST_PORT);
            clientMgr.HandleError += HandleClientError;
            clientMgr.HandleMessage += HandleClientMessage;

            ClientConnection cnn1 = clientMgr.Connect();
            ClientConnection cnn2 = clientMgr.Connect();
            ClientConnection cnn3 = clientMgr.Connect();

            Console.WriteLine("Press a key to  request status");
            Console.ReadKey(true);
            cnn1.RequestServerStatus();
            cnn2.RequestServerStatus();


            Console.WriteLine("Press a key to broadcast");
            Console.ReadKey(true);
            var msg = new Message();
            msg.Type = MessageType.WARNING;
            msg.Value = "Holy crap...";
            server.Broadcast(msg);

            Console.WriteLine("Press a key to run a load test");
            Console.ReadKey(true);
            Random rnd = new Random();
            ClientConnection[] cnns = new ClientConnection[] { cnn1, cnn2, cnn3 };
            for (int i = 0; i < 50; i++)
            {
                int idx = rnd.Next(cnns.Length);
                var cnn = cnns[idx];
                cnn.RequestServerStatus();

                int delay = rnd.Next(100, 1000);
                Console.WriteLine("Delaying for " + delay);
                System.Threading.Thread.Sleep(delay);
            }
            

            Console.WriteLine("Press a key to stop server");
            Console.ReadKey(true);
            server.Stop();

            Console.WriteLine("Stopped ... press a key to end");
            Console.ReadKey(true);


            
        }
    }
}
