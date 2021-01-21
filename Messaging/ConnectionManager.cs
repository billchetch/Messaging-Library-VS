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
    public enum ErrorCode
    {
        NONE,
        CLIENT_NOT_CONNECTED,
        ILLEGAL_MESSAGE_TYPE
    }

    abstract public class ConnectionManager
    {
        protected class ConnectionRequest
        {
            public String ID = null;
            public String Name = null;
            public bool Granted = false;
            public bool Requested = false;
            public Connection Connection = null;
            public Message Request = null; //sent by the requester to the granter
            public Message Response = null; //sent by granter to the requeser 
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
                String state = "Undetermined";
                if (!Completed)
                {
                    if(Requested && !Granted)
                    {
                        state = "Requesting";
                    }
                    else if(Granted){
                        state = "Granted";
                    }
                } else
                {
                    state = Succeeded ? "Succeeded" : "Failed";
                }
                return String.Format("ID: {0}, Name: {1}, State: {2}", ID, Name, state);
            }
        }

        public class Subscriber
        {
            static public Subscriber Parse(String fromString)
            {
                var parts = fromString.Split(':');
                String clientName = parts[0];
                Subscriber sub = new Subscriber(clientName);
                List<MessageType> mts = new List<MessageType>();
                foreach(String mt in parts[1].Split(','))
                {
                    mts.Add((MessageType)Enum.Parse(typeof(MessageType), mt));
                }
                sub.AddTypes(mts);
                return sub;
            }

            public String Name { get; internal set; }
            protected List<MessageType> MessageTypes = new List<MessageType>();

            public Subscriber(String name)
            {
                Name = name;
            }

            public void AddTypes(List<MessageType> messageTypes)
            {
                foreach (var mtype in messageTypes)
                {
                    if (!MessageTypes.Contains(mtype))
                    {
                        MessageTypes.Add(mtype);
                    }
                }
            }

            public bool SubscribesTo(MessageType mtype)
            {
                return MessageTypes.Contains(mtype);
            }

            public override string ToString()
            {
                return Name + ": " + String.Join(",", MessageTypes.ToArray());
            }
        }

        public TraceSource Tracing { get; set; } = null;

        public MessageHandler HandleMessage = null;
        public ErrorHandler HandleError = null;

        public String ID { get; internal set; }
        protected Dictionary<String, ConnectionRequest> ConnectionRequests { get; set; } = new Dictionary<String, ConnectionRequest>();

        private Connection _primaryConnection = null;
        protected Connection PrimaryConnection { get { return _primaryConnection; } }
        protected String LastConnectionString { get; set; }
        private Dictionary<String, Connection> _connections = new Dictionary<String, Connection>();
        protected Dictionary<String, Connection> Connections { get { return _connections; } }
        private int _incrementFrom = 0;
        protected int MaxConnections { get; set; } = 32;
        public int DefaultConnectionTimeout { get; set; } = 25000;
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
                Tracing?.TraceEvent(TraceEventType.Information, 1000, "Initialising primary connection with connection string {0}", connectionString);

                _primaryConnection = CreatePrimaryConnection(connectionString);
            }
            else if (connectionString != null && !connectionString.Equals(LastConnectionString, StringComparison.OrdinalIgnoreCase))
            {
                Tracing?.TraceEvent(TraceEventType.Information, 1000, "Re-initialising primary connection");

                if (_primaryConnection.State != Connection.ConnectionState.CLOSED)
                {
                    throw new Exception("Cannot re-initalise primary connection because there is one currently connected");
                }
                _primaryConnection = CreatePrimaryConnection(connectionString);
            }
            LastConnectionString = connectionString;
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


        abstract public void OnConnectionClosing(Connection cnn);
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
                ConnectionRequest cnnreq = null;
                foreach(var e in exceptions)
                {
                    if(e is MessageHandlingException)
                    {
                        var m = ((MessageHandlingException)e).Message;
                        if(m != null && m.Type == MessageType.CONNECTION_REQUEST_RESPONSE)
                        {
                            cnnreq = GetRequest(m.ResponseID);
                            break;
                        }
                    }
                }

                if(cnnreq == null){
                    cnnreq = GetRequest(cnn);
                }

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

        virtual public Connection CreatePrimaryConnection(String connectionString, Connection newCnn = null)
        {
            if (newCnn != null)
            {
                newCnn.Mgr = this;
                newCnn.Tracing = Tracing;
                newCnn.SignMessage = false; //sign outgoing messages
                newCnn.ValidateMessageSignature = false; //validate signature of received messages
            }
            return newCnn;
        }

        virtual public Connection CreateConnection(Message message, Connection requestingCnn, Connection newCnn = null)
        {
            if (newCnn != null)
            {
                newCnn.Mgr = this;
                newCnn.Tracing = Tracing;
                newCnn.SignMessage = true; //sign outgoing messages
                newCnn.ValidateMessageSignature = true; //validate signature of received messages
            }
            return newCnn;
        }

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

    /// <summary>
    /// SERVER CLASS
    /// </summary>
    abstract public class Server : ConnectionManager
    {
        public enum ServerState
        {
            NOT_SET,
            STARTING,
            STARTED,
            STOPPING,
            STOPPED
        }

        public enum CommandName
        {
            NOT_SET,
            REPORT_STATUS,
            RESTART,
            REPORT_LISTENERS,
            SET_TRACE_LEVEL,
            RESTORE_TRACE_LEVEL,
            START_TRACE_TO_CLIENT,
            ECHO_TRACE_TO_CLIENT,
            STOP_TRACE_TO_CLIENT,
            CLOSE_CONNECTION
        }

        public enum NotificationEvent
        {
            NOT_SET,
            CLIENT_CONNECTED,
            CLIENT_CLOSED,
            CUSTOM
        }
        
        public class Subscription
        {
            //the client begin subscribed to
            public String ClientName { get; set; } = null;

            //the list of clients subscribing to this client
            protected Dictionary<String, Subscriber> Subscribers = new Dictionary<String, Subscriber>();

            public Subscription(String cname)
            {
                ClientName = cname;
            }

            public List<Subscriber> GetSubscribers()
            {
                return Subscribers.Values.ToList();
            }

            public List<Subscriber> GetSubscribers(MessageType mtype)
            {
                List<Subscriber> subs = new List<Subscriber>();
                foreach(var s in Subscribers.Values)
                {
                    if (s.SubscribesTo(mtype))
                    {
                        subs.Add(s);
                    }
                }
                return subs;
            }

            public Subscriber AddSubscriber(String name, List<MessageType> messageTypes)
            {
                if (!Subscribers.ContainsKey(name))
                {
                    Subscribers[name] = new Subscriber(name);
                }

                Subscribers[name].AddTypes(messageTypes);
                return Subscribers[name];
            }

            public override string ToString()
            {
                StringBuilder sb = new StringBuilder();

                String lf = Environment.NewLine;

                sb.AppendFormat("{0}: " + lf, ClientName);
                foreach(var sub in Subscribers.Values)
                {
                    sb.AppendFormat(sub.ToString() + lf);
                }
                return sb.ToString();
            }
        }

        protected MemoryStream MStream { get; set;  } = null;
        protected TraceListener TListener { get; set;  } = null;
        private ThreadExecutionState _trace2clientXS = null;
        private Connection _traceConnection = null;
        public ServerState State { get; internal set; } = ServerState.NOT_SET;
        public bool IsRunning { get { return State == ServerState.STARTED;  } }

        protected List<ServerConnection> SecondaryConnections = new List<ServerConnection>();

        protected Dictionary<String, Subscription> Subscriptions = new Dictionary<String, Subscription>();
        protected MessageType[] AllowedSubscriptions = new MessageType[] { MessageType.NOTIFICATION, MessageType.INFO, MessageType.WARNING, MessageType.ERROR, MessageType.DATA, MessageType.ALERT};

        public Server() : base()
        {
            ID = "SRV-" + GetHashCode() + "-" + (DateTime.Now.Ticks / 1000);
            Tracing?.TraceEvent(TraceEventType.Information, 1000, "Created Server with ID {0}", ID);

            //for created connections to clients.
            DefaultConnectionTimeout = 25 * 1000; //wait for this period for client to connect
            DefaultActivityTimeout = 20 * 60 * 1000; //close after this period of inactivity
        }

        override public Connection CreatePrimaryConnection(String connectionString, Connection newCnn = null)
        {
            if(newCnn != null)
            {
                newCnn.RemainOpen = true;
                newCnn.RemainConnected = false;
                newCnn.ConnectionTimeout = -1;
                newCnn.ActivityTimeout = 4000;
                newCnn.ServerID = ID;
                newCnn.ValidateMessageSignature = false;
            }
            return base.CreatePrimaryConnection(connectionString, newCnn);
        }

        virtual protected bool CanCreateConnection(Connection cnn)
        {
            if(cnn == PrimaryConnection)
            {
                return true;
            } else
            {
                foreach(var scnn in SecondaryConnections)
                {
                    if (cnn == scnn) return true;
                }
            }
            return false;
        }

        override public Connection CreateConnection(Message request, Connection requestingCnn, Connection newCnn = null)
        {
            if(newCnn != null)
            {
                newCnn.RemainConnected = true;
                newCnn.RemainOpen = false;
                newCnn.ConnectionTimeout = DefaultConnectionTimeout;
                newCnn.AuthToken = "AT:" + DateTime.Now.Ticks.ToString();

                var ato = DefaultActivityTimeout;
                if(request.HasValue("ActivityTimeout") && request.GetInt("ActivityTimeout") >= 1000)
                {
                    ato = request.GetInt("ActivityTimeout");
                }
                newCnn.ActivityTimeout = ato;
            }

            return base.CreateConnection(request, requestingCnn, newCnn);
        }

        virtual public void Start()
        {
            if(State != ServerState.NOT_SET && State != ServerState.STOPPED)
            {
                var msg = String.Format("Cannot start server with state {0}", State);
                Tracing?.TraceEvent(TraceEventType.Error, 1000, msg);
                throw new Exception(msg);
            }

            State = ServerState.STARTING;
            Tracing?.TraceEvent(TraceEventType.Information, 1000, "Starting");
            if (PrimaryConnection == null || PrimaryConnection.CanOpen())
            {
                foreach(var cnn in SecondaryConnections)
                {
                    if (!cnn.CanOpen())
                    {
                        throw new Exception(String.Format("Secondary connection {0} cannot be opened", cnn.ToString()));
                    }
                }

                Tracing?.TraceEvent(TraceEventType.Information, 1000, "Initialising primary connection");
                InitialisePrimaryConnection(null);
                PrimaryConnection.Open();

                if(SecondaryConnections.Count > 0)
                {
                    foreach (var cnn in SecondaryConnections)
                    {
                        Tracing?.TraceEvent(TraceEventType.Information, 1000, "Opening secondary connection {0}", cnn.ToString());
                        cnn.Open();
                    }
                }
            }

            State = ServerState.STARTED;
            Tracing?.TraceEvent(TraceEventType.Information, 1000, "Started");
        }

        virtual public void Stop(bool waitForThreads = true)
        {
            State = ServerState.STOPPING;
            Tracing?.TraceEvent(TraceEventType.Information, 1000, "Stopping ... waiting for threads {0}", waitForThreads);

            //broadcast a message for shutdown and wait a moment for clients to close
            var message = CreateShutdownMessage();
            Broadcast(message);
            System.Threading.Thread.Sleep(100);

            //now gather a list of threads to make sure have closed before exiting
            List<String> threads2check = new List<String>();
            if (waitForThreads)
            {
                var cid = PrimaryConnection.ID;
                threads2check.Add(cid);
                threads2check.Add("Monitor-" + cid);

                foreach (var cnn in SecondaryConnections)
                {
                    threads2check.Add(cnn.ID);
                    threads2check.Add("Monitor-" + cnn.ID);
                }

                foreach (var cnnId in Connections.Keys)
                {
                    threads2check.Add(cnnId);
                    threads2check.Add("Monitor-" + cnnId);
                }
            }

            //now close the various connections
            if (PrimaryConnection != null && PrimaryConnection.State != Connection.ConnectionState.CLOSED)
            {
                Tracing?.TraceEvent(TraceEventType.Information, 1000, "Closing primary connection {0}", PrimaryConnection.ToString());
                PrimaryConnection.Close();
            }

            foreach (var cnn in SecondaryConnections)
            {
                if (cnn.State != Connection.ConnectionState.CLOSED)
                {
                    Tracing?.TraceEvent(TraceEventType.Information, 1000, "Closing secondary connection {0}", cnn.ToString());
                    cnn.Close();
                }
            }
            CloseConnections();

            //wait a bit for the threads to terminate of their own accord
            System.Threading.Thread.Sleep(100);

            //now try and pick up any stragglers
            while (waitForThreads)
            {
                Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Checking {0} threads", threads2check.Count());

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

            //set the server state to STOPPED, trace and exit
            State = ServerState.STOPPED;
            Tracing?.TraceEvent(TraceEventType.Information, 1000, "Stopped", false);
        }

        public void Restart()
        {
            Stop();
            System.Threading.Thread.Sleep(2000);
            Start();
        }

        protected void ReadMemoryStream(Connection cnn)
        {
            Tracing?.TraceEvent(TraceEventType.Information, 1000, "Start reading memory stream from connection {0}", cnn?.ToString());

            StreamReader reader = new StreamReader(MStream);
            while (true)
            {
                if (MStream.Length > 0)
                {
                    MStream.Position = 0;
                    String line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        cnn.SendMessage(CreateTraceMessage(line, cnn));
                        System.Threading.Thread.Sleep(10);
                    }
                    MStream.SetLength(0);
                }
            }
        }

        protected Connection GetConnectionByNameOrID(String id)
        {
            if (Connections.ContainsKey(id))
            {
                return Connections[id];
            }
            var cnn = GetNamedConnection(id);
            if (cnn != null) return cnn;

            if (PrimaryConnection.ID == id) return PrimaryConnection;
            foreach(var c in SecondaryConnections)
            {
                if (c.ID == id || c.Name == id) return c;
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

        override public void OnConnectionOpened(Connection cnn)
        {
            if (cnn != PrimaryConnection && !SecondaryConnections.Contains(cnn))
            {
                var cnnreq = GetRequest(cnn);
                if (cnnreq != null)
                {
                    Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Connection {0} opened successfully from request {1}", cnn.ToString(), cnnreq.ToString());
                    cnnreq.Succeeded = true;
                }
                else
                {
                    var msg = String.Format("Server::OnConnectionOpened: Cannot find coresponding connection request for connection {0}", cnn.ToString());
                    Tracing?.TraceEvent(TraceEventType.Error, 1000, "Exception: {0}", msg);
                    throw new Exception(msg);
                }
            } else
            {
                Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "{0} Connection {1} Opened", cnn == PrimaryConnection ? "Primary" : "Secondary", cnn.ToString());

            }
        }

        protected void NotifySubscribers(String client, NotificationEvent notificationEvent)
        {
            if (!Subscriptions.ContainsKey(client))
            {
                return;
            }

            var subs = Subscriptions[client].GetSubscribers();
            if (subs.Count > 0)
            {
                Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Notifying {0} subscribers to {1} of {2}", subs.Count, client, notificationEvent.ToString());
                var msg = new Message(MessageType.NOTIFICATION);
                msg.SubType = (int)notificationEvent;
                msg.Sender = client;
                foreach (var sub in subs)
                {
                    var scnn = GetNamedConnection(sub.Name);
                    if (scnn != null && scnn.IsConnected)
                    {
                        scnn.SendMessage(msg);
                    }
                }
            }
        }

        override public void OnConnectionConnected(Connection cnn)
        {
            Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Connection {0} connected", cnn.ToString());

            if(cnn.Name != null && Subscriptions.ContainsKey(cnn.Name))
            {
                NotifySubscribers(cnn.Name, NotificationEvent.CLIENT_CONNECTED);
            }
        }

        override public void OnConnectionClosing(Connection cnn)
        {
            Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Connection {0} closing...", cnn.ToString());
        }

        override public void OnConnectionClosed(Connection cnn, List<Exception> exceptions)
        {
            Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Connection {0} closed", cnn.ToString());

            if (cnn.Name != null && Subscriptions.ContainsKey(cnn.Name))
            {
                NotifySubscribers(cnn.Name, NotificationEvent.CLIENT_CLOSED);
            }


            if (cnn == _traceConnection)
            {
                if (!_trace2clientXS.IsFinished)
                {
                    Tracing?.TraceEvent(TraceEventType.Information, 1000, "Stopping sending trace to connection {0} because connection has closed", cnn.ToString());
                    Tracing.Listeners.Remove(TListener);
                    TListener = null;
                    ThreadExecutionManager.Terminate(_trace2clientXS.ID);
                    MStream.Close();
                    MStream.Dispose();
                    MStream = null;

                }
            }

            base.OnConnectionClosed(cnn, exceptions);
        }

        override protected void HandleConnectionErrors(Connection cnn, List<Exception> exceptions)
        {
            Tracing?.TraceEvent(TraceEventType.Warning, 1000, "HandleConnectionErrors: connection {0} received {1} execeptions", cnn.ToString(), exceptions.Count());

            foreach (var e in exceptions)
            {
                HandleError?.Invoke(cnn, e);
            }
        }

        override public void HandleReceivedMessage(Connection cnn, Message message)
        {
            HandleMessage?.Invoke(cnn, message);

            Message response = null;

            //We determine first the 'target' of the message
            bool relayMessage = message.Target != null && message.Target != ID;
            if (relayMessage)
            {
                switch (message.Type)
                {
                    case MessageType.CONNECTION_REQUEST:
                        response = CreateErrorMessage(String.Format("Server {0} cannot relay messages of type {1}", ID, message.Type), ErrorCode.ILLEGAL_MESSAGE_TYPE, cnn);
                        cnn.SendMessage(response);
                        break;

                    default:
                        var targets = message.Target.Split(',');
                        foreach (var target in targets)
                        {
                            var tgt = target.Trim();
                            var ncnn = GetNamedConnection(tgt);
                            if (ncnn != null)
                            {
                                message.Target = tgt;
                                ncnn.SendMessage(message);
                                //Tracing?.TraceEvent(TraceEventType.Information, 1000, "Server {0} relaying message from {1} to {2}", ID, message.Sender, tgt);
                            }
                            else
                            {
                                response = CreateErrorMessage(String.Format("Server {0} cannot relay message ({1}) to {2} as it is not connected.", ID, message.Type, tgt), ErrorCode.CLIENT_NOT_CONNECTED, cnn);
                                response.AddValue("IntendedTarget", tgt);
                                cnn.SendMessage(response);
                            }
                        }
                        break;
                } //end switch
                return;
            }


            //SUBSCRIPTIONS: Here the message has been sent with no specific target (or with server target)
            if (AllowedSubscriptions.Contains(message.Type) && Subscriptions.ContainsKey(message.Sender))
            {
                var subscribers = Subscriptions[message.Sender].GetSubscribers(message.Type);
                foreach(var subscriber in subscribers)
                {
                    var subcnn = GetNamedConnection(subscriber.Name);
                    if(subcnn != null && subcnn.IsConnected)
                    {
                        subcnn.SendMessage(message);
                    }
                }
            }

            //SERVER MESSAGES: Here we know the message is to be handled by the Server (relayMessage = false)
            switch (message.Type)
            {
                case MessageType.CONNECTION_REQUEST:
                    //we received a connection request
                    if (CanCreateConnection(cnn))
                    {
                        Connection newCnn = null;
                        String declined = null;
                        if (message.Sender == null) declined = "Anonymous connection not allowed";
                        if (Connections.Count + 1 >= MaxConnections) declined = "No available connections";
                        Connection oldCnn = GetNamedConnection(message.Sender);
                        if (declined == null && oldCnn != null)
                        {
                            //so this is a request from a client for which there is already an exising connection
                            if(message.Signature == null)
                            {
                                declined = String.Format("Another connection (state={0}) is already owned by {1}: no signature provided", oldCnn.State, message.Sender);
                            }
                            else if(!Connection.IsValidSignature(oldCnn.AuthToken, message))
                            {
                                declined = String.Format("Another connection (state={0}) is already owned by {1}: signature {2} is not valid", oldCnn.State, message.Sender, message.Signature);
                            } else 
                            { 
                                oldCnn.Close();
                                Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Closing old connection for request {0}", message.ToString());
                            }
                            if (declined != null && !oldCnn.IsConnected) Tracing?.TraceEvent(TraceEventType.Error, 1000, "Connection request declined for {0} but existing connectio in state", message.Sender, oldCnn.State);
                        }
                    
                        if (declined == null)
                        {
                            newCnn = CreateConnection(message, cnn);
                            if (newCnn != null)
                            {
                                Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Created connection for {0}", message.Sender);
                                newCnn.Name = message.Sender;
                                newCnn.ServerID = ID;
                                Connections[newCnn.ID] = newCnn; //Here we add the connection to the list of connections
                            }
                            else
                            {
                                declined = "Cannot create a connection";
                            }
                        }

                        //Respond
                        if (declined != null)
                        {
                            response = CreateRequestResponse(message, null, oldCnn);
                            response.AddValue("Declined", declined);
                            cnn.SendMessage(response);
                            Tracing?.TraceEvent(TraceEventType.Warning, 1000, "Declined connection request for {0} because {1}", message.Sender, declined);
                        }
                        else
                        {
                            //by here the connection request is granted
                            ConnectionRequest cnnreq = AddRequest(message, newCnn);
                            Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Connection for request {0} granted", cnnreq.ToString());
                            newCnn.Open();

                            //now we wait until the we have a successful opening
                            do
                            {
                                System.Threading.Thread.Sleep(100);
                            } while (!cnnreq.Succeeded && !cnnreq.Failed);

                            response = CreateRequestResponse(message, cnnreq.Succeeded ? newCnn : null, null);
                            if (cnnreq.Failed)
                            {
                                response.AddValue("Declined", "Connection request failed");
                                Tracing?.TraceEvent(TraceEventType.Warning, 1000, "Connection request {0} failed", cnnreq.ToString());
                            }
                            else
                            {
                                Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Connection for request {0} opened", cnnreq.ToString());
                            }

                            //Finally we can remove the connection request
                            ConnectionRequests.Remove(cnnreq.ID);
                            cnn.SendMessage(response);
                            Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Sent response for connection request {0}", message.ToString());
                        }
                    }
                    else
                    {
                        var msg = String.Format("Server: received connection request not on primary connection");
                        Tracing?.TraceEvent(TraceEventType.Error, 1000, "Exception: {0}", msg);
                        throw new Exception(msg);
                    }
                    break;

                case MessageType.STATUS_REQUEST:
                    if (message.HasValue("ConnectionID"))
                    {
                        var cnnId = message.GetString("ConnectionID");
                        var c = GetConnectionByNameOrID(cnnId);
                        if(c != null)
                        {
                            Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Connection {0} requested status for connection {1}", cnn.ToString(), cnnId);
                            response = c.CreateStatusResponse(message);
                        } else
                        {
                            response = CreateErrorMessage(String.Format("Cannot find connection {0}", cnnId), cnn);
                        }
                    } else {
                        Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Connection {0} requested server status", cnn.ToString());
                        response = CreateStatusResponse(message, cnn);
                    }
                    cnn.SendMessage(response);
                    break;

                case MessageType.PING:
                    response = CreatePingResponse(message, cnn);
                    cnn.SendMessage(response);
                    break;

                case MessageType.SUBSCRIBE:
                    response = null;
                    if(cnn.Name == null || cnn.Name.Length == 0)
                    {
                        response = CreateErrorMessage("Subscription request must be made by a Named connection", cnn);
                    }
                    else if (!message.HasValue("Clients") || message.GetString("Clients") == null)
                    {
                        response = CreateErrorMessage("Subscription request does not specify any clients", cnn);
                    } else
                    {
                        var messageTypes = AllowedSubscriptions.ToList(); //TODO: make this client settable
                        var c2s = message.GetString("Clients").Split(',');
                        foreach (var c in c2s)
                        {
                            if (c == null || c.Length == 0) continue;

                            var cname = c.Trim();
                            if (!Subscriptions.ContainsKey(cname))
                            {
                                Subscriptions[cname] = new Subscription(cname);
                            } else if(cname == cnn.Name)
                            {
                                continue;
                            }

                            Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Subscription: {0} is subscribing to {1}", cnn.Name, cname);
                            Subscriber sub = Subscriptions[cname].AddSubscriber(cnn.Name, messageTypes);

                            //let the client being subscribed to know
                            var scnn = GetNamedConnection(cname);
                            if (scnn != null && scnn.IsConnected)
                            {
                                var msg = new Message(MessageType.SUBSCRIBE);
                                msg.AddValue("Subscriber", sub.ToString());
                                scnn.SendMessage(msg);
                            }
                        }

                        response = new Message(MessageType.SUBSCRIBE_RESPONSE);
                        response.Value = "Subsription successful";
                    }
                    if (response != null)
                    {
                        cnn.SendMessage(response);
                    }
                    break;

                case MessageType.UNSUBSCRIBE:
                    response = null;
                    if (cnn.Name == null || cnn.Name.Length == 0)
                    {
                        response = CreateErrorMessage("Unbsubscribe request must be made by a Named connection", cnn);
                    }
                    else if (!message.HasValue("Clients") || message.GetString("Clients") == null)
                    {
                        response = CreateErrorMessage("Unbsubscribe request does not specify any clients", cnn);
                    } else
                    {
                        var c2s = message.GetString("Clients").Split(',');
                        foreach (var c in c2s)
                        {
                            if (c == null || c.Length == 0) continue;

                            var cname = c.Trim();
                            if (Subscriptions.ContainsKey(cname))
                            {
                                Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Subscription: {0} is unsubscribing from {1}", cnn.Name, cname);
                                Subscriptions.Remove(cname);
                            }

                            //let the client being subscribed to know
                            var scnn = GetNamedConnection(cname);
                            if (scnn != null && scnn.IsConnected)
                            {
                                var msg = new Message(MessageType.UNSUBSCRIBE);
                                msg.AddValue("Subscriber", cnn.Name);
                                scnn.SendMessage(msg);
                            }
                        }
                    }
                    if (response != null)
                    {
                        cnn.SendMessage(response);
                    }
                    break;

                case MessageType.COMMAND:
                    CommandName cmd = CommandName.NOT_SET;
                    try
                    {
                        cmd = (CommandName)message.SubType;
                    } catch (Exception e)
                    {
                        response = CreateErrorMessage(String.Format("Unrecognised command {0}", message.SubType), cnn);
                        Tracing?.TraceEvent(TraceEventType.Error, 1000, e.Message);
                        cnn.SendMessage(response);
                        return;
                    }

                    switch (cmd)
                    {
                        case CommandName.RESTART:
                            ThreadExecutionManager.Execute(ID + "-Restart", this.Restart);
                            break;

                        case CommandName.REPORT_STATUS:
                            response = CreateStatusResponse(message, cnn);
                            cnn.SendMessage(response);
                            break;

                        case CommandName.REPORT_LISTENERS:
                            if (Tracing == null)
                            {
                                cnn.SendMessage(CreateErrorMessage("No Tracing Source available", cnn));
                                return;
                            }
                            response = CreateCommmandResponse(message, cnn);
                            var listenerNames = TraceSourceManager.GetListenerNames(Tracing.Name);
                            response.AddValue("Listeners", listenerNames);
                            cnn.SendMessage(response);
                            break;

                        case CommandName.SET_TRACE_LEVEL:
                            if (Tracing == null)
                            {
                                cnn.SendMessage(CreateErrorMessage("No Tracing Source available", cnn));
                                return;
                            }
                            if (message.HasValue("Listener") && message.HasValue("TraceLevel"))
                            {
                                String listenerName = message.GetString("Listener");
                                SourceLevels level = (SourceLevels)message.GetInt("TraceLevel");
                                TraceSourceManager.SetListenersTraceLevel(Tracing.Name, listenerName, level);

                                Tracing.TraceEvent(TraceEventType.Information, 1000, "Set trace level for {0} to {1}", listenerName, level);
                            } else
                            {
                                cnn.SendMessage(CreateErrorMessage("Message does not have a Listener and/or TraceLevel value", cnn));
                            }
                            break;

                        case CommandName.RESTORE_TRACE_LEVEL:
                            if (Tracing == null)
                            {
                                cnn.SendMessage(CreateErrorMessage("No Tracing Source available", cnn));
                                return;
                            }

                            if (message.HasValue("Listener"))
                            {
                                String listenerName = message.GetString("Listener");
                                TraceSourceManager.RestoreListeners(Tracing.Name, listenerName);
                                Tracing.TraceEvent(TraceEventType.Information, 1000, "Restored trace level for {0}", listenerName);
                            }
                            else
                            {
                                cnn.SendMessage(CreateErrorMessage("Message does not have a Listener value", cnn));
                            }
                            break;

                        case CommandName.START_TRACE_TO_CLIENT:
                            if (Tracing == null)
                            {
                                cnn.SendMessage(CreateErrorMessage("No Tracing Source available", cnn));
                                return;
                            }

                            if (MStream == null)
                            {
                                MStream = new MemoryStream();
                            }

                            Tracing.TraceEvent(TraceEventType.Information, 1000, "Attempting to start tracing to {0}", cnn.ToString());
                            
                            TListener = new TextWriterTraceListener(MStream);
                            Tracing.Listeners.Add(TListener);
                            _traceConnection = cnn;
                            _trace2clientXS = ThreadExecutionManager.Execute<Connection>("trace2client", this.ReadMemoryStream, cnn);

                            break;

                        case CommandName.ECHO_TRACE_TO_CLIENT:
                            if (Tracing == null)
                            {
                                cnn.SendMessage(CreateErrorMessage("No Tracing Source available", cnn));
                                return;
                            }

                            if(_traceConnection != null)
                            {
                                Tracing.TraceEvent(TraceEventType.Information, 1000, "Echoing from {0}: {1}", cnn.ToString(), message.Value);
                            }
                            break;

                        case CommandName.STOP_TRACE_TO_CLIENT:
                            if (Tracing == null)
                            {
                                cnn.SendMessage(CreateErrorMessage("No Tracing Source available", cnn));
                                return;
                            }

                            if (TListener == null || MStream == null || _trace2clientXS.IsFinished)
                            {
                                return;
                            } else
                            {
                                Tracing.Listeners.Remove(TListener);
                                ThreadExecutionManager.Terminate(_trace2clientXS.ID);
                                TListener = null;
                                MStream.Close();
                                MStream.Dispose();
                                MStream = null;
                                Tracing.TraceEvent(TraceEventType.Information, 1000, "Stopped tracing to {0}", cnn.ToString());
                            }
                            break;

                        case CommandName.CLOSE_CONNECTION:
                            if (message.HasValue("ConnectionID"))
                            {
                                String cnnId = message.GetString("ConnectionID");

                                var cnn2close = GetConnectionByNameOrID(cnnId);
                                if (cnn2close != null)
                                {
                                    Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Closing connection {0}", cnn2close.ToString());
                                    cnn2close.Close();
                                } else
                                {
                                    Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Cannot find connection {0} to close", cnnId);
                                }
                            }
                            else
                            {
                                cnn.SendMessage(CreateErrorMessage("Message does not have a Connection ID value", cnn));
                            }
                            break;
                    }
                    break;

                default:
                    //allow other messages to drop through
                    break;
            }
        }

        virtual protected Message CreateErrorMessage(String errorMsg, Connection cnn)
        {
            return CreateErrorMessage(errorMsg, ErrorCode.NONE, cnn);
        }

        virtual protected Message CreateErrorMessage(String errorMsg, ErrorCode errorCode, Connection cnn)
        {
            var message = new Message();
            message.Type = MessageType.ERROR;
            message.SubType = (int)errorCode;
            message.Value = errorMsg;
            return message;
        }

        virtual protected Message CreateTraceMessage(String msg, Connection cnn)
        {
            var message = new Message();
            message.Type = MessageType.TRACE;
            message.Value = msg;
            return message;
        }

        virtual protected Message CreateRequestResponse(Message request, Connection newCnn, Connection oldCnn = null)
        {
            var response = new Message();
            response.Type = MessageType.CONNECTION_REQUEST_RESPONSE;
            response.ResponseID = request.ID;
            response.Sender = ID;
            if (newCnn != null)
            {
                response.Target = request.Sender;
                response.AddValue("Granted", true);
                response.AddValue("AuthToken", newCnn.AuthToken);
            }
            else
            {
                response.AddValue("Granted", false);
            }

            //include useful data in response
            if (Subscriptions.ContainsKey(request.Sender))
            {
                List<String> subs = Subscriptions[request.Sender].GetSubscribers().Select(i => i.ToString()).ToList();
                response.AddValue("Subscribers", subs);
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
            response.AddValue("PrimaryConnection", PrimaryConnection.ToString());
            response.AddValue("SecondaryConnectionsCount", SecondaryConnections.Count);
            var scnns2add = new List<String>();
            foreach(var scnn in SecondaryConnections)
            {
                scnns2add.Add(scnn.ToString());
            }
            response.AddValue("SecondaryConnections", scnns2add);
            var cnns2add = new List<String>();
            foreach (var c in Connections.Values)
            {
                cnns2add.Add(c.ToString());
            }
            response.AddValue("ConnectionsCount", cnns2add.Count);
            response.AddValue("Connections", cnns2add);
            response.AddValue("MaxConnections", MaxConnections);

            response.AddValue("SubscriptionsCount", Subscriptions.Count);
            var subs = new List<String>();
            foreach(var sub in Subscriptions.Values)
            {
                subs.Add(sub.ToString());
            }
            response.AddValue("Subscriptions", subs);
            return response;
        }

        virtual protected Message CreatePingResponse(Message message, Connection cnn)
        {
            var response = new Message();
            response.ResponseID = message.ID;
            response.Type = MessageType.PING_RESPONSE;
            return response;
        }

        virtual protected Message CreateCommmandResponse(Message command, Connection cnn)
        {
            var response = new Message();
            response.Type = MessageType.COMMAND_RESPONSE;
            response.SubType = command.SubType;
            response.ResponseID = command.ID;
            response.Target = command.Sender;
            response.Sender = ID;
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
    /// <typeparam name="T">The type of client connection</typeparam>
    abstract public class ClientManager<T> : ConnectionManager where T : ClientConnection, new()
    {
        public class ServerData
        {
            public String ID; //ID given by server
            public String Name; //name according to client
            public String ConnectionString;
            
            public ServerData(String name, String cnnString)
            {
                Name = name;
                ConnectionString = cnnString;
            }
        }

        protected Queue<ConnectionRequest> ConnectionRequestQueue = new Queue<ConnectionRequest>();
        protected Dictionary<String, ServerData> Servers = new Dictionary<String, ServerData>();

        protected Queue<Connection> ReconnectionQueue = new Queue<Connection>();
        private System.Timers.Timer _keepConnectionsAlive;
        public int KeepAliveInterval { get; set; } = 30000;

        private Action<ClientConnection> _connectListener = null;

        public ClientManager(Action<ClientConnection> connectListener = null) : base()
        {
            ID = "CMGR-" + GetHashCode() + "-" + (DateTime.Now.Ticks / 1000);
            Tracing?.TraceEvent(TraceEventType.Information, 1000, "Created Client Manager with ID {0}", ID);
            _connectListener = connectListener;
        }

        override protected void InitialisePrimaryConnection(String connectionString)
        {
            if (PrimaryConnection == null)
            {
                Tracing?.TraceEvent(TraceEventType.Information, 1000, "Initialising primary connection");
            } else if(connectionString != null && connectionString.Equals(LastConnectionString, StringComparison.OrdinalIgnoreCase))
            {
                Tracing?.TraceEvent(TraceEventType.Information, 1000, "Re-initialising primary connection");
            }

            base.InitialisePrimaryConnection(connectionString);
            
            PrimaryConnection.RemainOpen = false;
            PrimaryConnection.RemainConnected = false;
            PrimaryConnection.ConnectionTimeout = 10000;
            PrimaryConnection.ActivityTimeout = 10000;
        }

        override public Connection CreatePrimaryConnection(String connectionString, Connection cnn = null)
        {
            T client = new T();
            client.ID = CreateNewConnectionID();
            client.ParseConnectionString(connectionString);
            return base.CreatePrimaryConnection(connectionString, client);
        }

        override public Connection CreateConnection(Message message, Connection requestingCnn, Connection newCnn = null)
        {
            if (message != null && message.Type == MessageType.CONNECTION_REQUEST_RESPONSE)
            {
                T client = new T();
                client.ID = CreateNewConnectionID();
                client.ParseMessage(message);
                client.ServerID = message.Sender;
                if (message.HasValue("AuthToken"))
                {
                    client.AuthToken = message.GetString("AuthToken");
                }
                client.RemainConnected = true;
                client.RemainOpen = false;
                client.ConnectionTimeout = DefaultConnectionTimeout;
                client.ActivityTimeout = DefaultActivityTimeout;

                if (message.HasValue("Subscribers"))
                {
                    List<String> subs = message.GetList<String>("Subscribers");
                    foreach(String s in subs)
                    {
                        try
                        {
                            client.AddSubscriber(Subscriber.Parse(s));
                        } catch (Exception e)
                        {
                            Tracing?.TraceEvent(TraceEventType.Error, 1000, e.Message);
                        }
                    }
                }


                return base.CreateConnection(message, requestingCnn, client);
            }
            else
            {
                throw new Exception("ClientManager::CreateConnection: unable to createc connection from passed message");
            }

        }
        override public void OnConnectionOpened(Connection cnn)
        {
            Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Connection {0} opened", cnn.ToString());
        }

        override public void OnConnectionConnected(Connection cnn)
        {
            if (cnn == PrimaryConnection && ConnectionRequestQueue.Count > 0)
            {
                var cnnreq = ConnectionRequestQueue.Dequeue();
                cnnreq.Requested = true;
                PrimaryConnection.SendMessage(cnnreq.Request);

                Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Requesting connection {0}", cnnreq.ToString());
            }
            else
            {
                var cnnreq = GetRequest(cnn);
                cnnreq.Succeeded = true;
            }
        }

        override public void OnConnectionClosing(Connection cnn)
        {
            Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Connection {0} closing...", cnn.ToString());
        }

        override public void OnConnectionClosed(Connection cnn, List<Exception> exceptions)
        {
            Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Connection {0} closed", cnn.ToString());

            //TODO: if conection is primary connection and there are still requests in the queue then open again
            //this is currently not done well ... there is a queue but the connect method is not async so the request queue
            //never populates more than 1


            base.OnConnectionClosed(cnn, exceptions);
        }

        override protected void HandleConnectionErrors(Connection cnn, List<Exception> exceptions)
        {
            Tracing?.TraceEvent(TraceEventType.Warning, 1000, "HandleConnectionErrors: connection {0} received {1} execeptions", cnn.ToString(), exceptions.Count());

            bool reconnect = false;
            foreach (var e in exceptions)
            {
                HandleError?.Invoke(cnn, e);

                if(e is MessageIOException)
                {
                    var ioe = (MessageIOException)e;
                    if (ioe.ConnectionState != Connection.ConnectionState.CLOSING && ioe.ConnectionState != Connection.ConnectionState.CLOSED)
                    {
                        Tracing?.TraceEvent(TraceEventType.Warning, 1000, "HandleConnectionErrors: MessageIOException {0} when state {1} for connection {2}", ioe.Message, ioe.ConnectionState.ToString(), cnn.ToString());
                        reconnect = true;
                    }
                }
            }

            if (reconnect)
            {
                Tracing?.TraceEvent(TraceEventType.Information, 1000, "HandleConnectionErrors: Adding connection {0} to reconnection queue", cnn.ToString());
                ReconnectionQueue.Enqueue(cnn);
                NextKeepAlive(2000);
            }
        }

        override public void HandleReceivedMessage(Connection cnn, Message message)
        {
            switch (message.Type)
            {
                case MessageType.CONNECTION_REQUEST_RESPONSE:
                    if (cnn == PrimaryConnection)
                    {
                        //get connection request and set the response
                        ConnectionRequest cnnreq = GetRequest(message.ResponseID);
                        cnnreq.Response = message;
                        Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Received request response for request {0}", cnnreq.ToString());
                        cnnreq.Granted = message.GetBool("Granted");
                        if (!cnnreq.Granted)
                        {
                            cnnreq.Failed = true;
                            return;
                        }

                        //server has granted connection so we record server ID and create a new client connection
                        cnn.ServerID = message.Sender;
                        Connection newCnn = newCnn = CreateConnection(message, cnn);
                        
                        if (newCnn == null) 
                        {
                            cnnreq.Failed = true;
                        } else if(!newCnn.IsConnected)
                        {
                            newCnn.Name = cnnreq.Name;
                            cnnreq.Connection = newCnn;
                            Connections[newCnn.ID] = newCnn; //Here we add the connection to the list of connections
                            Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Opening connection for request {0}", cnnreq.ToString());
                            newCnn.Open();
                        }
                    }
                    break;

                case MessageType.SHUTDOWN:
                    cnn.Close();
                    break;
                    
                case MessageType.PING:

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
            Servers[serverName] = new ServerData(serverName, connectionString);
        }

        public ServerData GetServer(String serverName)
        {
            return Servers.ContainsKey(serverName) ? Servers[serverName] : null;
        }

        virtual public ClientConnection Connect(String name, int timeout = -1)
        {
            if (Servers.ContainsKey("default"))
            {
                return Connect("default", name, timeout);
            } else
            {
                throw new Exception("ClientManager::Connect: No default server set");
            }
        }

        virtual public ClientConnection Connect(String connectionString, String name, int timeout = -1, String authToken = null)
        {
            if (Connections.Count == MaxConnections)
            {
                throw new Exception("Cannot create more than " + MaxConnections + " connections");
            }

            //look to see if we can directly use an existing connection
            ClientConnection cnn = (ClientConnection)GetNamedConnection(name);
            if(cnn != null)
            {
                if (cnn.IsConnected)
                {
                    return cnn;
                } else
                {
                    throw new Exception(String.Format("Cannot connect {0} because a connection already exists of state {1}", cnn.Name, cnn.State));
                }
            }

            String serverKey = connectionString;
            if (Servers.ContainsKey(connectionString))
            {
                connectionString = Servers[connectionString].ConnectionString;
            }
            else
            {
                AddServer(connectionString, connectionString);
            }

            InitialisePrimaryConnection(connectionString);

            var cnnreq = AddRequest(CreateConnectionRequest(name));
            cnnreq.Name = name;
            if(authToken != null)
            {
                //sign the request message using the old connection auth token
                cnnreq.Request.Signature = Connection.CreateSignature(authToken, cnnreq.Request.Sender);
            }
            ConnectionRequestQueue.Enqueue(cnnreq);
            Tracing?.TraceEvent(TraceEventType.Verbose, 1000, "Requesting connection @ {0} using request {1}", connectionString, cnnreq.ToString());

            if (ConnectionRequestQueue.Peek() == cnnreq)
            {
                PrimaryConnection.Open();
            }

            long started = DateTime.Now.Ticks;
            bool connected = false;
            bool waited2long = false;
            try
            { 
                do
                {
                    System.Threading.Thread.Sleep(200);
                    long elapsed = ((DateTime.Now.Ticks - started) / TimeSpan.TicksPerMillisecond);
                    if (timeout > 0 && elapsed > timeout)
                    {
                        var msg = String.Format("Cancelling connection request {0} due to timeout of {1}", cnnreq.ToString(), timeout);
                        throw new TimeoutException(msg);
                    }
                    if (cnnreq.Failed)
                    {
                        var msg = String.Format("Failed request: {0} ", cnnreq.ToString());
                        throw new Exception(msg);
                    }

                    if(elapsed > 20000 && !waited2long)
                    {
                        Tracing?.TraceEvent(TraceEventType.Warning, 1000, "Already waiting to connect for {0} seconds!", elapsed/1000);
                        waited2long = true;
                    }

                    connected = cnnreq.Succeeded;
                    System.Threading.Thread.Sleep(200);
                    //Console.WriteLine("ClientManager::Connect connected = " + connected);

                } while (!connected);
            } catch (Exception e)
            {
                //
                if(ConnectionRequestQueue.Count > 0 && ConnectionRequestQueue.Peek() == cnnreq)
                {
                    ConnectionRequestQueue.Dequeue();
                }
                Tracing?.TraceEvent(TraceEventType.Error, 1000, "Connect exception: {0}", e.Message);
                throw e;
            }
            finally
            {
                ConnectionRequests.Remove(cnnreq.ID);
            }

            //here the connection is successful so we update the server with ServerID) and create a timer
            Tracing?.TraceEvent(TraceEventType.Information, 1000, "Connection request {0} successful", cnnreq);

            Servers[serverKey].ID = cnnreq.Response.Sender;

            NextKeepAlive(KeepAliveInterval, true);
            
            if(_connectListener != null)
            {
                _connectListener((ClientConnection)cnnreq.Connection);
            }
            
            return (ClientConnection)cnnreq.Connection;
        }

        //Reconnecting involves
        virtual public void Reconnect(Connection cnn, int timeout = -1)
        {
            if (cnn.IsConnected)
            {
                throw new Exception(String.Format("Cannot reconnect {0} because it is already connected", cnn.ToString()));
            }
            

            String connectionString = null;
            foreach(var server in Servers.Values)
            {
                if(server.ID == cnn.ServerID)
                {
                    connectionString = server.ConnectionString;
                    break;
                }
            }

            if(connectionString == null)
            {
                throw new Exception(String.Format("Cannot find connection string for server connection {0} with ServerID {1}", cnn.ToString(), cnn.ServerID));
            }

            //here the connection is ready to re-connect and we have the original connection string
            Connect(connectionString, cnn.Name, timeout, cnn.AuthToken);
        }

        private void NextKeepAlive(int interval, bool createTimer = false)
        {
            if (_keepConnectionsAlive == null)
            {
                if (!createTimer)
                {
                    return;
                }
                _keepConnectionsAlive = new System.Timers.Timer();
                _keepConnectionsAlive.Elapsed += KeepConnectionsAlive;
                Tracing?.TraceEvent(TraceEventType.Information, 1000, "Created keep alive timer");
            }
            else
            {
                _keepConnectionsAlive.Stop();
            }

            _keepConnectionsAlive.Interval = interval;
            _keepConnectionsAlive.AutoReset = false;
            _keepConnectionsAlive.Start();
        }

        virtual public void KeepConnectionsAlive(Object source, System.Timers.ElapsedEventArgs ea)
        {
            foreach(var cnn in Connections.Values)
            {
                var ccnn = ((ClientConnection)cnn);
                if (ccnn.IsConnected)
                {
                    ccnn.SendPing();
                }
            }
            
            var nextInterval = KeepAliveInterval;
            if(ReconnectionQueue.Count > 0)
            {
                nextInterval = 2000;
                try
                {
                    var cnn = ReconnectionQueue.Peek();
                    Tracing?.TraceEvent(TraceEventType.Information, 1000, "Attempting to reconnect {0}", cnn.ToString());
                    Reconnect(cnn, 10000);
                    ReconnectionQueue.Dequeue();
                } catch (Exception e)
                {
                    Tracing?.TraceEvent(TraceEventType.Error, 1000, e.Message);
                }
            }

            //set off timer again
            NextKeepAlive(nextInterval);
        }
    } //enc ClientManager class
}