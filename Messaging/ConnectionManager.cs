using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Diagnostics;
using Chetch.Utilities;
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
        }

        protected Dictionary<String, ConnectionRequest> ConnectionRequests { get; set; } = new Dictionary<String, ConnectionRequest>();

        private Connection _primaryConnection = null;
        protected Connection PrimaryConnection { get { return _primaryConnection; } }
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

        virtual protected void InitialisePrimaryConnection()
        {
            if (_primaryConnection == null)
            {
                _primaryConnection = CreatePrimaryConnection();
                _primaryConnection.Mgr = this;
            }
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
            return GetHashCode() + "-" + _incrementFrom.ToString();
        }

        abstract public Connection CreatePrimaryConnection();
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
        public MessageHandler HandleMessage = null;
        public ErrorHandler HandleError = null;

        virtual public void Start()
        {
            if (PrimaryConnection == null || PrimaryConnection.CanOpen())
            {
                InitialisePrimaryConnection();
                PrimaryConnection.RemainOpen = true;
                PrimaryConnection.RemainConnected = false;
                PrimaryConnection.Open();
            }
        }

        virtual public void Stop(bool waitForThreads = true)
        {
            var message = CreateShutdownMessage();
            Broadcast(message);

            if (PrimaryConnection != null)
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
                //Console.WriteLine("Server::Stop: Checking threads " + threads2check.Count + " threads...");
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
            base.OnConnectionClosed(cnn, exceptions);
            if (cnn == PrimaryConnection && exceptions != null && exceptions.Count > 0)
            {
                Stop();
            }
        }

        override public void OnConnectionConnected(Connection cnn)
        {
            //empty
        }

        override public void OnConnectionOpened(Connection cnn)
        {
            if (cnn != PrimaryConnection)
            {
                var cnnreq = GetRequest(cnn);
                if (cnnreq != null)
                {
                    cnnreq.Succeeded = true;
                }
                else
                {
                    throw new Exception("Server::OnConnectionOpened: Cannot find coresponding connection request for connection " + cnn.ID);
                }
            }
        }

        override protected void HandleConnectionErrors(Connection cnn, List<Exception> exceptions)
        {
            foreach (var e in exceptions)
            {
                HandleError?.Invoke(cnn, e);
            }
        }

        override public void HandleReceivedMessage(Connection cnn, Message message)
        {
            //base.HandleReceivedMessage(cnn, message);
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
                                newCnn.Mgr = this;
                                newCnn.RemainConnected = true;
                                newCnn.RemainOpen = false;
                                newCnn.Name = message.Sender;
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
                        }
                        else
                        {
                            ConnectionRequest cnnreq = AddRequest(message, newCnn);
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
                            }
                            PrimaryConnection.SendMessage(response);
                        }
                    }
                    else
                    {
                        throw new Exception("Server: received connection request not on primary connection");
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

            var acnns = GetActiveConnections();
            var activecnns = new List<String>();
            foreach(var accn in acnns)
            {
                activecnns.Add(accn.ToString());
            }
            response.AddValue("ActiveConnections", activecnns);
            return response;
        }

        virtual protected Message CreateShutdownMessage()
        {
            var response = new Message();
            response.Type = MessageType.SHUTDOWN;
            response.Value = "Server is shutting down";
            return response;
        }

        virtual public void Broadcast(Message message)
        {
            foreach (var cnn in Connections.Values)
            {
                if (cnn.IsConnected)
                {
                    cnn.SendMessage(message);
                }
            }
        }
    } //end Server class

    abstract public class ClientManager : ConnectionManager
    {
        public static TraceSource Tracing { get; set; } = new TraceSource("Chetch.Messaging.ClientManager");

        protected Queue<ConnectionRequest> ConnectionRequestQueue = new Queue<ConnectionRequest>();


        public ClientManager() : base()
        {

        }

        override protected void InitialisePrimaryConnection()
        {
            base.InitialisePrimaryConnection();
            PrimaryConnection.RemainOpen = false;
            PrimaryConnection.RemainConnected = false;
        }

        override public void OnConnectionOpened(Connection cnn)
        {
            //empty
            Tracing.TraceEvent(TraceEventType.Start, 1000, "Connection " + cnn.ID + "opened");
        }

        override public void OnConnectionConnected(Connection cnn)
        {
            if (cnn == PrimaryConnection && ConnectionRequestQueue.Count > 0)
            {
                var cnnreq = ConnectionRequestQueue.Dequeue();
                PrimaryConnection.SendMessage(cnnreq.Request);
            }
            else
            {
                var cnnreq = GetRequest(cnn);
                cnnreq.Succeeded = true;
            }
        }

        override protected void HandleConnectionErrors(Connection cnn, List<Exception> exceptions)
        {
            //empty
        }

        override public void HandleReceivedMessage(Connection cnn, Message message)
        {
            switch (message.Type)
            {
                case MessageType.CONNECTION_REQUEST_RESPONSE:
                    if (cnn == PrimaryConnection && Connections.Count + 1 < MaxConnections)
                    {
                        ConnectionRequest cnnreq = GetRequest(message.ResponseID);
                        //TODO: check if indeed request is granted if
                        cnnreq.Granted = true;
                        var newCnn = CreateConnection(message);
                        if (newCnn != null)
                        {
                            newCnn.Mgr = this;
                            newCnn.RemainConnected = true;
                            newCnn.RemainOpen = false;
                            newCnn.Name = cnnreq.Name;
                            Connections[newCnn.ID] = newCnn;
                            cnnreq.Connection = newCnn;
                            newCnn.Open();
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

        virtual public ClientConnection Connect(String name, int timeout = -1)
        {
            InitialisePrimaryConnection();

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
                    throw new TimeoutException("Timeout of " + timeout + "ms exceeded");
                }
                if (cnnreq.Failed)
                {
                    throw new Exception("Connection request failed");
                }

                connected = cnnreq.Succeeded;
                System.Threading.Thread.Sleep(200);
                //Console.WriteLine("ClientManager::Connect connected = " + connected);

            } while (!connected);

            return (ClientConnection)cnnreq.Connection;
        }
    } //enc ClientManager class
}