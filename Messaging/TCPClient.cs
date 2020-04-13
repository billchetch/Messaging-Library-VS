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
    public class TCPClient : ClientConnection
    {
        public IPAddress IP { get; set; }
        public int Port { get; set; }
        private TcpClient _client;

        public TCPClient(String cnnId, IPAddress ipAddr, int port, int cnnTimeout, int actTimeout) : base(cnnId, cnnTimeout, actTimeout)
        {
            if (ipAddr == null)
            {
                IPHostEntry ipHostInfo = Dns.GetHostEntry(Dns.GetHostName());
                ipAddr = ipHostInfo.AddressList[1];
            }
            IP = ipAddr;
            Port = port;
        }

        public TCPClient(String cnnId, IPAddress ipAddr, int port) : this(cnnId, ipAddr, port, -1, -1)
        {
            //empty
        }

        public TCPClient(IPAddress ipAddr, int port) : this(null, ipAddr, port)
        {
            //empty   
        }

        public TCPClient(int port) : this(null, port)
        {
            //empty   
        }

        override public void Close()
        {
            State = ConnectionState.CLOSING;
            //Console.WriteLine("TCPClient::Close: {0} closing", ID);
            RemainOpen = false;
            _client.Close();
            base.Close();
        }

        override protected void OnConnectionTimeout()
        {
            //Console.WriteLine("TCPClient::OnConnectionTimeout: {0} calling close", ID);
            Close();
        }
        override protected void OnActivityTimeout()
        {

        }

        override protected void Connect()
        {
            IPEndPoint remoteEP = new IPEndPoint(IP, Port);

            _client = new TcpClient();

            do
            {
                _client.Connect(remoteEP);

                State = ConnectionState.OPENED;

                Stream = _client.GetStream();
                State = ConnectionState.CONNECTED;
                try
                {
                    do
                    {
                        ReceiveMessage();
                    } while (RemainConnected);

                }
                catch (System.IO.IOException e)
                {
                    //Console.WriteLine("TCPClient::Connect: Reading exception " + e.Message + " on " + remoteEP.ToString() + " (" + ID + ")");
                    throw e;
                }
                finally
                {
                    //Console.WriteLine("TCPClient::Connect: finally block closing stream and client{0}, RemaingConnected={1}, RemainOpen={2}", ID, RemainConnected, RemainOpen);
                    if (State != ConnectionState.CLOSING)
                    {
                        Stream.Close();
                        _client.Close();
                    }
                }
            } while (RemainOpen);

            State = ConnectionState.CLOSED;
        }
    }
}
