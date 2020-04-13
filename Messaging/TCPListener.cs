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
    public class TCPListener : ServerConnection
    {
        public IPAddress IP { get; set; }
        public int Port { get; set; }
        private TcpListener _listener;
        private TcpClient _client;

        public TCPListener(String cnnId, IPAddress ipAddr, int port, int cnnTimeout, int actTimeout) : base(cnnId, cnnTimeout, actTimeout)
        {
            if (ipAddr == null)
            {
                IPHostEntry ipHostInfo = Dns.GetHostEntry(Dns.GetHostName());
                ipAddr = ipHostInfo.AddressList[1]; //use IPv4
            }
            IP = ipAddr;
            Port = port;
        }

        public TCPListener(String cnnId, IPAddress ipAddr, int port) : this(cnnId, ipAddr, port, -1, -1)
        {
            //empty   
        }

        public TCPListener(IPAddress ipAddr, int port) : this(null, ipAddr, port, -1, -1)
        {
            //empty   
        }

        public TCPListener(int port) : this(null, null, port, -1, -1)
        {
            //empty 
        }

        override public bool Open()
        {
            //Console.WriteLine("")
            return base.Open();
        }
        override public void Close()
        {
            //Console.WriteLine("TCPListener::Close closing {0}", ID);
            State = ConnectionState.CLOSING;
            _listener.Stop();
            _client.Close();
            base.Close();
        }

        override protected void OnConnectionTimeout()
        {
            Close();
        }
        override protected void OnActivityTimeout()
        {
            if (_client.Connected)
            {
                _client.Close();
            }
        }

        override protected void Listen()
        {
            _listener = new TcpListener(IP, Port);

            do
            {
                _listener.Start();
                try
                {
                    State = ConnectionState.OPENED;
                    _client = _listener.AcceptTcpClient();
                    State = ConnectionState.CONNECTED;

                    Stream = _client.GetStream();
                    try
                    {
                        do
                        {
                            ReceiveMessage();
                        } while (RemainConnected);

                    }
                    catch (System.IO.IOException e)
                    {
                        //Console.WriteLine("Server: Reading exception " + e.Message + " on " + IP.ToString() + ":" + Port + "(" + ID + ")");
                    }
                    finally
                    {
                        //Console.WriteLine("Server: closing stream and client on " + IP.ToString() + ":" + Port + " (" + ID + ")");
                        if (State != ConnectionState.CLOSING)
                        {
                            Stream.Close();
                            _client.Close();
                        }
                    }
                }
                catch (System.Net.Sockets.SocketException e)
                {
                    //Console.WriteLine("Server: Accept connection exception: " + e.Message);
                    throw e;
                }
                finally
                {
                    _listener.Stop();
                }
            } while (RemainOpen);

            State = ConnectionState.CLOSED;
        }
    }
}
