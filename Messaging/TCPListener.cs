using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using System.IO;
using System.Diagnostics;

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
            if (State != ConnectionState.CLOSED)
            {
                State = ConnectionState.CLOSING;
                _listener.Stop();
                _client?.Close();
            }
            base.Close();
        }

        override protected void OnConnectionTimeout()
        {
            Close();
        }

        override protected void OnActivityTimeout()
        {
            //Tracing?.TraceEvent(System.Diagnostics.TraceEventType.Warning, 3000, "Activity timeout for connection {0}", ToString());
            Close();
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
                    catch (IOException e) //some kind of underlying stream or connection error
                    {
                        Tracing?.TraceEvent(TraceEventType.Warning, 3000, "Server: Reading exception {0} {1} connection {2}", e.GetType().ToString(), e.Message, ID);
                    }
                    catch (Exception e) //other types of error
                    {
                        Tracing?.TraceEvent(TraceEventType.Warning, 3000, "Server: Reading exception {0} {1} connection {2}", e.GetType().ToString(), e.Message, ID);
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

        public override string ToString()
        {
            return base.ToString() + " " + IP + ":" + Port;
        }
    } //end TCPListener class
}
