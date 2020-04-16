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


        public TCPClient() : base()
        {
            //empty   
        }

        override public void ParseConnectionString(String connectionString)
        {
            var parts = connectionString.Split(':');
            if (parts.Length != 2) throw new Exception(String.Format("Connection string{0} is not valid.", connectionString));

            IPAddress ip = IPAddress.Parse(parts[0]);
            int port = System.Convert.ToInt32(parts[1]);
            IP = ip;
            Port = port;
        }

        override public void ParseMessage(Message message)
        {
            int port = message.GetInt("Port");
            IPAddress ip = IPAddress.Parse(message.GetString("IP"));
            IP = ip;
            Port = port;
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

        public override string ToString()
        {
            return base.ToString() + " " + IP + ":" + Port;
        }
    }//end TCPClient class
}
