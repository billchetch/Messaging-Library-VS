using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Diagnostics;
using Chetch.Utilities;
using Chetch.Utilities.Config;
using Chetch.Application;

namespace Chetch.Messaging
{
    abstract public class ConnectionManager
    {
        protected class ConnectionRequest
        {
            public String ID = null;
            public String Name = null;
            public bool Granted = false;
            public bool Requested = false;
            public Connection Connection = null;
            public Message Request = null;
            private Object _lockSuccessFailure = new Object();
            private bool _succeeded = false;
            public bool Succeeded
            {
                get
                {
                    return _succeeded;
                }
                set
                {
                    lock (_lockSuccessFailure)
                    {
                        if (_failed) throw new Exception("ConnecitonRequest::Succeeded: cannot set to true as Failed already set to true");
                        _succeeded = value;
                    }

                }
            }
            private bool _failed = false;
            public bool Failed
            {
                get
                {
                    return _failed;
                }
                set
                {
                    lock (_lockSuccessFailure)
                    {
                        if (_succeeded) throw new Exception("ConnecitonRequest::Failed: cannot set to true as Succeeded already set to true");
                        _failed = value;
                    }
                }
            }

            public bool Completed
            {
                get
                {
                    return Succeeded || Failed;
                }
            }

            public override string ToString()
            {
                String state = "Unknown";
                if (!Completed)
                {
                    if(Requested && !Granted)
                    {
                        state = "Requesting";
                    }
                    else if(Granted){
                        state = "Granted";
                    }
                    else
                    {
                        state = "Declined";
                    }
                } else
                {
                    state = Succeeded ? "Succeeded" : "Failed";
                }
                return String.Format("ID: {0}, Name: {1}, State: {2}", ID, Name, state);
            }
        }


        public String ID { get; internal set; }
        protected Dictionary<String, ConnectionRequest> ConnectionRequests { get; set; } = new Dictionary<String, ConnectionRequest>();

        private Connection _primaryConnection = null;
        protected Connection PrimaryConnection { get { return _primaryConnection; } }
        protected String LastConnectionString { get; set; }
        private Dictionary<String, Connection> _connections = new Dictionary<String, Connection>();
        protected Dictionary<String, Connection> Connections { get { return _connections; } }
        private int _incrementFrom = 0;
        protected int MaxConnections { get; set; } = 32;
        public int DefaultConnectionTimeout { get; set; } = -1;
        public int DefaultActivityTimeout { get; set; } = -1;
        private Object _lockConnections = new Object();

        public ConnectionManager()
        {

        }

        ~ConnectionManager()
        {
            /*if (PrimaryConnection != null)
            {
                PrimaryConnection.Close();
            }
            CloseConnections();
            */
        }

        virtual protected void InitialisePrimaryConnection(String connectionString)
        {
            if (_primaryConnection == null)
            {
                _primaryConnection = CreatePrimaryConnection(connectionString);
                _primaryConnection.Mgr = this;
            }
            else if (connectionString != null && !connectionString.Equals(LastConnectionString, StringComparison.OrdinalIgnoreCase))
            {
                if(_primaryConnection.State != Connection.ConnectionState.CLOSED)
                {
                    throw new Exception("Cannot re-initalise primary connection because there is one currently connected");
                }
                _primaryConnection = CreatePrimaryConnection(connectionString);
                _primaryConnection.Mgr = this;
            }
            LastConnectionString = connectionString;
        }

        protected void CloseConnections()
        {
            lock (_lockConnections)
            {
                var connections2close = new List<Connection>();

                foreach (var cnn in Connections.Values)
                {
                    connections2close.Add(cnn);
                }

                foreach (var cnn in connections2close)
                {
                    cnn.Close();
                }
            }
        }

        abstract public void HandleReceivedMessage(Connection cnn, Message message);
        abstract protected void HandleConnectionErrors(Connection cnn, List<Exception> exceptions);
        
        virtual public void OnConnectionClosed(Connection cnn, List<Exception> exceptions)
        {
            lock (_lockConnections)
            {
                if (_connections.ContainsKey(cnn.ID))
                {
                    _connections.Remove(cnn.ID);
                }
            }

            if (exceptions.Count > 0)
            {
                var cnnreq = GetRequest(cnn);
                if (cnnreq != null && !cnnreq.Succeeded)
                {
                    cnnreq.Failed = true;
                }
                HandleConnectionErrors(cnn, exceptions);
            }
        }
        abstract public void OnConnectionConnected(Connection cnn);
        abstract public void OnConnectionOpened(Connection cnn);

        virtual protected String CreateNewConnectionID()
        {
            _incrementFrom++;
            return ID + "-" + _incrementFrom.ToString();
        }

        abstract public Connection CreatePrimaryConnection(String connectionString);
        abstract public Connection CreateConnection(Message message);

        virtual protected ConnectionRequest AddRequest(Message request, Connection cnn = null)
        {
            var cnnreq = new ConnectionRequest();
            cnnreq.ID = request.ID;
            cnnreq.Request = request;
            cnnreq.Connection = cnn;
            ConnectionRequests[cnnreq.ID] = cnnreq;
            return cnnreq;
        }

        virtual protected ConnectionRequest GetRequest(Connection cnn)
        {
            if (cnn == null) return null;
            foreach (var v in ConnectionRequests.Values)
            {
                if (v.Connection == cnn) return v;
            }
            return null;
        }

        virtual protected ConnectionRequest GetRequest(String id)
        {
            return ConnectionRequests.ContainsKey(id) ? ConnectionRequests[id] : null;
        }
    } //end ConnectionManager class


    abstract public class Server : ConnectionManager
    {
        public static TraceSource Tracing { get; set; } = TraceSourceManager.GetInstance("Chetch.Messaging.Server");

        public MessageHandler HandleMessage = null;
        public ErrorHandler HandleError = null;

        public Server() : base()
        {
            ID = "Server-" + GetHashCode();
            Tracing.TraceEvent(TraceEventType.Information, 1000, "Created Server with ID {0}", ID);
        }

        override protected void InitialisePrimaryConnection(String connectionString)
        {
            if (PrimaryConnection == null)
            {
                Tracing.TraceEvent(TraceEventType.Information, 1000, "Initialising primary connection");
            }
            else if (connectionString != null && connectionString.Equals(LastConnectionString, StringComparison.OrdinalIgnoreCase))
            {
                Tracing.TraceEvent(TraceEventType.Information, 1000, "Re-initialising primary connection");
            }

            base.InitialisePrimaryConnection(connectionString);
            if (PrimaryConnection.Tracing == null)
            {
                PrimaryConnection.Tracing = Tracing;
            }
            PrimaryConnection.RemainOpen = true;
            PrimaryConnection.RemainConnected = false;
            PrimaryConnection.ConnectionTimeout = -1;
            PrimaryConnection.ActivityTimeout = 10000;
        }

        virtual public void Start()
        {
            if (PrimaryConnection == null || PrimaryConnection.CanOpen())
            {
                Tracing.TraceEvent(TraceEventType.Start, 1000, "Initialising primary connection");
                InitialisePrimaryConnection(null);
                PrimaryConnection.Open();
            } 
        }

        virtual public void Stop(bool waitForThreads = true)
        {
            Tracing.TraceEvent(TraceEventType.Suspend, 1000, "Stopping ... waiting for threads {0}", false);

            var message = CreateShutdownMessage();
            Broadcast(message);

            if (PrimaryConnection != null && PrimaryConnection.State != Connection.ConnectionState.CLOSED)
            {
                PrimaryConnection.Close();
            }

            List<String> threads2check = new List<String>();
            if (waitForThreads)
            {
                foreach (var cnnId in Connections.Keys)
                {
                    threads2check.Add(cnnId);
                    threads2check.Add("Monitor-" + cnnId);
                }
            }
            CloseConnections();

            while (waitForThreads)
            {
                Tracing.TraceEvent(TraceEventType.Verbose, 1000, "Checking {0} threads", threads2check.Count());

                System.Threading.Thread.Sleep(200);
                List<String> threads2remove = new List<String>();
                foreach (var xid in threads2check)
                {
                    if (ThreadExecutionManager.IsEmpty(xid))
                    {
                        threads2remove.Add(xid);
                    }
                }

                foreach (var xid in threads2remove)
                {
                    threads2check.Remove(xid);
                }
                waitForThreads = threads2check.Count > 0;
            }
        }

        protected Connection GetNamedConnection(String name)
        {
            foreach (var c in Connections.Values)
            {
                if (c.Name != null && c.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                {
                    return c;
                }
            }
            return null;
        }

        protected List<Connection> GetActiveConnections()
        {
            List<Connection> connections = new List<Connection>();
            foreach (var c in Connections.Values)
            {
                if (c.Name != null && c.IsConnected)
                {
                    connections.Add(c);
                }
            }
            return connections;
        }

        override public void OnConnectionClosed(Connection cnn, List<Exception> exceptions)
        {
            Tracing.TraceEvent(TraceEventType.Verbose, 1000, "Connection {0} closed", cnn.ToString());
            base.OnConnectionClosed(cnn, exceptions);
        }

        override public void OnConnectionConnected(Connection cnn)
        {
            Tracing.TraceEvent(TraceEventType.Verbose, 1000, "Connection {0} connected", cnn.ToString());
        }

        override public void OnConnectionOpened(Connection cnn)
        {
            Tracing.TraceEvent(TraceEventType.Verbose, 1000, "Connection {0} opened", cnn.ToString());
            if (cnn != PrimaryConnection)
            {
                var cnnreq = GetRequest(cnn);
                if (cnnreq != null)
                {
                    cnnreq.Succeeded = true;
                }
                else
                {
                    var msg = String.Format("Server::OnConnectionOpened: Cannot find coresponding connection request for connection {0}", cnn.ToString());
                    Tracing.TraceEvent(TraceEventType.Error, 1000, "Exception: {0}", msg);
                    throw new Exception(msg);
                }
            }
        }

        override protected void HandleConnectionErrors(Connection cnn, List<Exception> exceptions)
        {
            Tracing.TraceEvent(TraceEventType.Warning, 1000, "HandleConnectionErrors: connection {0} received {1} execeptions", cnn.ToString(), exceptions.Count());

            foreach (var e in exceptions)
            {
                HandleError?.Invoke(cnn, e);
            }
        }

        override public void HandleReceivedMessage(Connection cnn, Message message)
        {
            HandleMessage?.Invoke(cnn, message);

            Message response = null;
            switch (message.Type)
            {
                case MessageType.CONNECTION_REQUEST:
                    //we received a connection request
                    if (cnn == PrimaryConnection)
                    {
                        Connection newCnn = null;
                        String declined = null;
                        if (message.Sender == null) declined = "Anonymous connection not allowed";
                        if (Connections.Count + 1 >= MaxConnections) declined = "No available connections";
                        if (declined == null && GetNamedConnection(message.Sender) != null)
                        {
                            declined = "Another connection is already owned by " + message.Sender;
                        }

                        if (declined == null)
                        {
                            newCnn = CreateConnection(message);
                            if (newCnn != null)
                            {
                                Tracing.TraceEvent(TraceEventType.Verbose, 1000, "Created connection for {0}", message.Sender);

                                newCnn.Mgr = this;
                                newCnn.Tracing = Tracing;
                                newCnn.RemainConnected = true;
                                newCnn.RemainOpen = false;
                                newCnn.Name = message.Sender;
                                newCnn.ConnectionTimeout = DefaultConnectionTimeout;
                                newCnn.ActivityTimeout = DefaultActivityTimeout;
                                Connections[newCnn.ID] = newCnn;
                            }
                            else
                            {
                                declined = "Cannot create a connection";
                            }
                        }

                        //Respond
                        if (declined != null)
                        {
                            response = CreateRequestResponse(message, null);
                            response.AddValue("Declined", declined);
                            PrimaryConnection.SendMessage(response);
                            Tracing.TraceEvent(TraceEventType.Warning, 1000, "Declined connection request for {0} because {1}", message.Sender, declined);
                        }
                        else
                        {
                            ConnectionRequest cnnreq = AddRequest(message, newCnn);
                            Tracing.TraceEvent(TraceEventType.Verbose, 1000, "Connection for request {0} granted", cnnreq.ToString());
                            newCnn.Open();

                            //now we wait until the we have a successful opening
                            do
                            {
                                System.Threading.Thread.Sleep(100);
                            } while (!cnnreq.Succeeded && !cnnreq.Failed);

                            response = CreateRequestResponse(message, cnnreq.Succeeded ? newCnn : null);
                            if (cnnreq.Failed)
                            {
                                response.AddValue("Declined", "Connection request failed");
                                Tracing.TraceEvent(TraceEventType.Warning, 1000, "Connection request {0} failed", cnnreq.ToString());
                            } else
                            {
                                Tracing.TraceEvent(TraceEventType.Verbose, 1000, "Connection for request {0} opened", cnnreq.ToString());
                            }
                            PrimaryConnection.SendMessage(response);
                        }
                    }
                    else
                    {
                        var msg = String.Format("Server: received connection request not on primary connection");
                        Tracing.TraceEvent(TraceEventType.Error, 1000, "Exception: {0}", msg);
                        throw new Exception(msg);
                    }
                    break;

                case MessageType.STATUS_REQUEST:
                    response = CreateStatusResponse(message, cnn);
                    cnn.SendMessage(response);
                    break;

                default:
                    //relay messages to other clients
                    if (message.Target != null)
                    {
                        var targets = message.Target.Split(',');
                        foreach (var target in targets)
                        {
                            var tgt = target.Trim();
                            var ncnn = GetNamedConnection(tgt);
                            if (ncnn != null)
                            {
                                message.Target = tgt;
                                ncnn.SendMessage(message);
                            }
                            else
                            {
                                var emsg = CreateErrorMessage(tgt + " is not connected.", cnn);
                                cnn.SendMessage(emsg);
                            }
                        }
                    }
                    break;
            }
        }

        virtual protected Message CreateErrorMessage(String errorMsg, Connection cnn)
        {
            var message = new Message();
            message.Type = MessageType.ERROR;
            message.Value = errorMsg;
            return message;
        }

        virtual protected Message CreateRequestResponse(Message request, Connection newCnn)
        {
            var response = new Message();
            response.Type = MessageType.CONNECTION_REQUEST_RESPONSE;
            response.ResponseID = request.ID;
            if (newCnn != null)
            {
                response.Target = request.Sender;
                response.AddValue("Granted", true);
            }
            else
            {
                response.AddValue("Granted", false);
            }
            return response;
        }

        virtual protected Message CreateStatusResponse(Message request, Connection cnn)
        {
            var response = new Message();
            response.Type = MessageType.STATUS_RESPONSE;
            response.ResponseID = request.ID;
            response.Target = request.Sender;
            response.Sender = ID;
            response.AddValue("ServerID", ID);
            var acnns = GetActiveConnections();
            var activecnns = new List<String>();
            foreach(var accn in acnns)
            {
                activecnns.Add(accn.ToString());
            }
            response.AddValue("ActiveConnectionsCount", activecnns.Count);
            response.AddValue("ActiveConnections", activecnns);
            response.AddValue("MaxConnections", MaxConnections);
            return response;
        }

        virtual protected Message CreateShutdownMessage()
        {
            var response = new Message();
            response.Type = MessageType.SHUTDOWN;
            response.Value = "Server is shutting down";
            response.Sender = ID;
            return response;
        }

        virtual public void Broadcast(Message message)
        {
            IEnumerable<Connection> listeners;
            if(message.Type == MessageType.SHUTDOWN)
            {
                listeners = new List<Connection>(Connections.Values);
            } else
            {
                listeners = Connections.Values;
            }
            

            foreach (var cnn in listeners)
            {
                if (cnn.IsConnected)
                {
                    cnn.SendMessage(message);
                }
            }
        }
    } //end Server class

    /// <summary>
    /// ClientManager class
    /// </summary>
    /// <typeparam name="T"></typeparam>
    abstract public class ClientManager<T> : ConnectionManager where T : ClientConnection, new()
    {
        public static TraceSource Tracing { get; set; } = TraceSourceManager.GetInstance("Chetch.Messaging.ClientManager");

        protected Queue<ConnectionRequest> ConnectionRequestQueue = new Queue<ConnectionRequest>();
        protected Dictionary<String, String> Servers = new Dictionary<String, String>();

        public ClientManager() : base()
        {
            ID = "ClientManager-" + GetHashCode();
            Tracing.TraceEvent(TraceEventType.Information, 1000, "Created Client Manager with ID {0}", ID);
        }

        override protected void InitialisePrimaryConnection(String connectionString)
        {
            if (PrimaryConnection == null)
            {
                Tracing.TraceEvent(TraceEventType.Information, 1000, "Initialising primary connection");
            } else if(connectionString != null && connectionString.Equals(LastConnectionString, StringComparison.OrdinalIgnoreCase))
            {
                Tracing.TraceEvent(TraceEventType.Information, 1000, "Re-initialising primary connection");
            }

            base.InitialisePrimaryConnection(connectionString);
            if (PrimaryConnection.Tracing == null)
            {
                PrimaryConnection.Tracing = Tracing;
            }
            PrimaryConnection.RemainOpen = false;
            PrimaryConnection.RemainConnected = false;
            PrimaryConnection.ConnectionTimeout = 10000;
            PrimaryConnection.ActivityTimeout = 5000;
        }

        override public Connection CreatePrimaryConnection(String connectionString)
        {
            T client = new T();
            client.ID = CreateNewConnectionID();
            client.ParseConnectionString(connectionString);
            return client;
        }

        override public Connection CreateConnection(Message message)
        {
            if (message != null && message.Type == MessageType.CONNECTION_REQUEST_RESPONSE)
            {
                T client = new T();
                client.ID = CreateNewConnectionID();
                client.ParseMessage(message);
                return client;
            }
            else
            {
                throw new Exception("ClientManager::CreateConnection: unable to createc connection from passed message");
            }

        }
        override public void OnConnectionOpened(Connection cnn)
        {
            Tracing.TraceEvent(TraceEventType.Verbose, 1000, "Connection {0} opened", cnn.ToString());
        }

        override public void OnConnectionConnected(Connection cnn)
        {
            if (cnn == PrimaryConnection && ConnectionRequestQueue.Count > 0)
            {
                var cnnreq = ConnectionRequestQueue.Dequeue();
                cnnreq.Requested = true;
                PrimaryConnection.SendMessage(cnnreq.Request);

                Tracing.TraceEvent(TraceEventType.Verbose, 1000, "Requesting connection {0}", cnnreq.ToString());
            }
            else
            {
                var cnnreq = GetRequest(cnn);
                cnnreq.Succeeded = true;
            }
        }

        override protected void HandleConnectionErrors(Connection cnn, List<Exception> exceptions)
        {
            Tracing.TraceEvent(TraceEventType.Warning, 1000, "HandleConnectionErrors: connection {0} received {1} execeptions", cnn.ToString(), exceptions.Count());
        }

        override public void HandleReceivedMessage(Connection cnn, Message message)
        {
            switch (message.Type)
            {
                case MessageType.CONNECTION_REQUEST_RESPONSE:
                    if (cnn == PrimaryConnection && Connections.Count + 1 < MaxConnections)
                    {
                        ConnectionRequest cnnreq = GetRequest(message.ResponseID);
                        Tracing.TraceEvent(TraceEventType.Verbose, 1000, "Received request respponse for request {0}", cnnreq.ToString());

                        //TODO: check if indeed request is granted if
                        cnnreq.Granted = true;
                        var newCnn = CreateConnection(message);
                        if (newCnn != null)
                        {
                            newCnn.Mgr = this;
                            newCnn.Tracing = Tracing;
                            newCnn.RemainConnected = true;
                            newCnn.RemainOpen = false;
                            newCnn.ConnectionTimeout = DefaultConnectionTimeout;
                            newCnn.ActivityTimeout = DefaultActivityTimeout;
                            newCnn.Name = cnnreq.Name;
                            Connections[newCnn.ID] = newCnn;
                            cnnreq.Connection = newCnn;
                            newCnn.Open();
                            Tracing.TraceEvent(TraceEventType.Verbose, 1000, "Opening connection for request {0}", cnnreq.ToString());
                        }
                        else
                        {
                            cnnreq.Failed = true;
                        }
                    }
                    break;

                case MessageType.SHUTDOWN:
                    cnn.Close();
                    break;
            }
        }

        virtual protected Message CreateConnectionRequest(String owner)
        {
            var request = new Message();
            request.Type = MessageType.CONNECTION_REQUEST;
            request.Sender = owner;
            return request;
        }


        public void AddServer(String serverName, String connectionString)
        {
            Servers[serverName] = connectionString;
        }

        virtual public ClientConnection Connect(String name, int timeout = -1)
        {
            if (Servers.ContainsKey("default"))
            {
                return Connect(Servers["default"], name, timeout);
            } else
            {
                throw new Exception("ClientManager::Connect: No default server set");
            }
        }

        virtual public ClientConnection Connect(String connectionString, String name, int timeout = -1)
        {
            if (Servers.ContainsKey(connectionString))
            {
                connectionString = Servers[connectionString];
            }
            else
            {
                Servers[connectionString] = connectionString;
            }

            InitialisePrimaryConnection(connectionString);

            var cnnreq = AddRequest(CreateConnectionRequest(name));
            cnnreq.Name = name;
            ConnectionRequestQueue.Enqueue(cnnreq);

            if (ConnectionRequestQueue.Peek() == cnnreq)
            {
                PrimaryConnection.Open();
            }

            long started = DateTime.Now.Ticks;
            bool connected = false;
            do
            {
                System.Threading.Thread.Sleep(200);
                if (timeout > 0 && ((DateTime.Now.Ticks - started) / TimeSpan.TicksPerMillisecond) > timeout)
                {
                    var msg = String.Format("Cancelling connection request {0} due to timeout of {1}", cnnreq.ToString(), timeout);
                    Tracing.TraceEvent(TraceEventType.Error, 1000, "Connect exception: {0}", msg);
                    throw new TimeoutException(msg);
                }
                if (cnnreq.Failed)
                {
                    var msg = String.Format("Failed request: {0} ", cnnreq.ToString());
                    Tracing.TraceEvent(TraceEventType.Error, 1000, "Connect exception: {0}", msg);
                    throw new Exception(msg);
                }

                connected = cnnreq.Succeeded;
                System.Threading.Thread.Sleep(200);
                //Console.WriteLine("ClientManager::Connect connected = " + connected);

            } while (!connected);

            return (ClientConnection)cnnreq.Connection;
        }
    } //enc ClientManager class
}