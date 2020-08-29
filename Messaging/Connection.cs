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
    public delegate void MessageHandler(Connection cnn, Message message);
    public delegate void MessageModifier(Connection cnn, Message message);
    public delegate void ErrorHandler(Connection cnn, Exception e);

    abstract public class Connection
    {
        public enum ConnectionState
        {
            NOT_SET,
            OPENING,
            OPENED,
            CONNECTED,
            RECEIVING,
            SENDING,
            CLOSING,
            CLOSED
        }

        public String ID { get; internal set; }
        public String Name { get; internal set; }
        public int ConnectionTimeout { get; set; }
        public int ActivityTimeout { get; set; }
        public Dictionary<ConnectionState, long> States { get; internal set; } = new Dictionary<ConnectionState, long>();
        private ConnectionState _state;
        private Object _stateLock = new Object();
        public ConnectionState State
        {
            get
            {
                lock (_stateLock)
                {
                    return _state;
                }
            }
            set
            {
                if (value != _state)
                {
                    ConnectionState oldState;
                    lock (_stateLock)
                    {
                        oldState = _state;
                        _state = value;
                        States[value] = DateTime.Now.Ticks;
                    }
                    //Tracing?.TraceEvent(TraceEventType.Verbose, 2000, "Connection::State change: {0} from {1} to {2}", ToString(), oldState, _state);

                    if (_state == ConnectionState.CLOSING)
                    {
                        OnClosing();
                    }
                    if (_state == ConnectionState.CLOSED)
                    {
                        OnClose(_startXS == null ? null : _startXS.Exceptions);
                    }
                    if (_state == ConnectionState.CONNECTED && oldState == ConnectionState.OPENED)
                    {
                        OnConnect();
                    }
                    if (_state == ConnectionState.OPENED)
                    {
                        OnOpen();
                    }
                }
            }//end set
        }
        public bool IsConnected
        {
            get
            {
                bool connected = false;
                lock (_stateLock)
                {
                    connected = _state == ConnectionState.CONNECTED || _state == ConnectionState.RECEIVING || _state == ConnectionState.SENDING;
                }
                return connected;
            }
        }
        public ConnectionManager Mgr { get; set; } = null;
        protected Stream Stream { get; set; } = null;
        private ThreadExecutionState _startXS = null;
        public bool RemainConnected { get; set; } = false; //Can be used in 'listening' loop to set whether connection waits for additional messages or not
        public bool RemainOpen { get; set; } = false; //Can be used in 'listening' loop to set whether connection re-opens or not
        public String ServerID { get; set; }
        public String AuthToken { get; set; }
        public bool ValidateMessageSignature { get; set; } = true; //validate siganture of incoming messages
        public bool SignMessage { get; set; } = true; //sign outgoing messages

        public MessageEncoding MEncoding { get; set; } = MessageEncoding.JSON;
        public long MessagesReceived { get; internal set; } = 0;
        public long GarbageReceived { get; internal set; } = 0;
        public long MessagesSent { get; internal set; } = 0;
        public long LastMessageSentOn { get; internal set; } = -1;
        
        public TraceSource Tracing { get; set; } = null;


        public Connection()
        {

        }

        public Connection(String cnnId, int cnnTimeout, int actTimeout)
        {
            ID = cnnId;
            ConnectionTimeout = cnnTimeout;
            ActivityTimeout = actTimeout;
        }

        public Connection(int cnnTimeout, int actTimeout) : this(null, cnnTimeout, actTimeout)
        {

        }

        public void Reset()
        {
            if (State == ConnectionState.CLOSED && (_startXS == null || _startXS.IsFinished))
            {
                State = ConnectionState.NOT_SET;
            } else
            {
                throw new Exception(String.Format("Cannot reset connection {0} because in wrong state ({1}) or execution thread is not finished.", ToString(), State));
            }
        }

        protected void Start(Action action, int retryAttempts = 3)
        {
            String monitorId = "Monitor-" + ID;
            int attempts = 0;
            while (true)
            {
                if (ThreadExecutionManager.IsEmpty(ID) && ThreadExecutionManager.IsEmpty(monitorId))
                {
                    _startXS = ThreadExecutionManager.Execute(ID, action);
                    if (_startXS == null) throw new Exception("Connection::Start: Unable to create thread for connection " + ID);
                    ThreadExecutionManager.Execute(monitorId, this.Monitor);
                    Tracing?.TraceEvent(TraceEventType.Verbose, 2000, "Connection::Start: Created execution thread {0} and monitor thrad{1}", ID, monitorId);

                    break;
                }
                else
                {
                    if (++attempts > retryAttempts)
                    {
                        var sxs = ThreadExecutionManager.GetExecutionState(ID);
                        var mxs = ThreadExecutionManager.GetExecutionState(monitorId);
                        throw new Exception("Thread ID " + ID + " has state " + sxs.State + " and " + monitorId + " has state " + mxs.State);
                    }
                    System.Threading.Thread.Sleep(200);
                }
            }
        }

        protected long Elapsed(ConnectionState state)
        {
            return DateTime.Now.Ticks - States[state];
        }

        private bool TimedOut(int timeout, ConnectionState state)
        {
            return (timeout > 0) && State == state && (Elapsed(state) > (timeout * TimeSpan.TicksPerMillisecond));
        }

        protected void Monitor()
        {
            while (true)
            {
                System.Threading.Thread.Sleep(500); //TODO: should make this value settable
                //Console.WriteLine("Monitoring connection " + ID + " has state " + State);
                if (_startXS != null && _startXS.IsFinished)
                {
                    Tracing?.TraceEvent(TraceEventType.Warning, 2000, "Connection::Monitor: Exeuction thread  for {0} is of state {1} but connection is of state {2} so setting connection stateo to closed.", ToString(), _startXS.State, State);
                    State = ConnectionState.CLOSED;
                }

                if (TimedOut(ConnectionTimeout, ConnectionState.OPENED))
                {
                    Tracing?.TraceEvent(TraceEventType.Warning, 2000, "Connection::Monitor: Connection timeout {0} for {1}", ConnectionTimeout, ToString());
                    OnConnectionTimeout();
                }

                if (TimedOut(ActivityTimeout, ConnectionState.CONNECTED))
                {
                    Tracing?.TraceEvent(TraceEventType.Warning, 2000, "Connection::Monitor: Activity timeout {0} for {1}", ActivityTimeout, ToString());
                    OnActivityTimeout();
                }
                if (State == ConnectionState.CLOSED)
                {
                    if (_startXS != null)
                    {
                        if (_startXS.IsFinished)
                        {
                            Tracing?.TraceEvent(TraceEventType.Information, 2000, "Connection::Monitor: Exeuction thread is of state closed but is not finished so terminating.");
                            ThreadExecutionManager.Terminate(_startXS.ID);
                        } else if(_startXS.Exceptions.Count > 0)
                        {
                            foreach (var ex in _startXS.Exceptions)
                            {
                                Tracing?.TraceEvent(TraceEventType.Error, 2000, "Connection::Monitor: Exeuction thread is of state closed with exception {0}", ex.Message);
                            }
                        }
                    }
                    break;
                }
            }
        }

        virtual public bool Open()
        {
            if (!CanOpen()) return false;

            Tracing?.TraceEvent(TraceEventType.Verbose, 2000, "Connection::Open: {0} opening", ID);

            State = ConnectionState.OPENING;
            Stream = null; //
            return true;
        }

        virtual public void Close()
        {
            if(State == ConnectionState.CLOSED)
            {
                Tracing?.TraceEvent(TraceEventType.Warning, 2000, "Connection::Close: {0} already of state CLOSED", ToString());
                return;
            }

            
            State = ConnectionState.CLOSING;
            Tracing?.TraceEvent(TraceEventType.Verbose, 2000, "Connection::Close: {0} started closing", ToString());

            if (Stream != null)
            {
                try
                {
                    Stream.Close();
                    Stream.Dispose();
                }
                catch (System.ObjectDisposedException e)
                {
                    //sometimes the 'Open' thread will dispose of this and then a managing thread will try and close
                    //resulting in an ObjectDisposedException... which we ignore since the important thing is the Stream
                    //has been closed
                    Tracing?.TraceEvent(TraceEventType.Error, 2000, "Connection::Close: exception {0} for connectino ", e.Message, ToString());
                }
            }

            State = ConnectionState.CLOSED;
        }

        abstract protected void OnConnectionTimeout();
        abstract protected void OnActivityTimeout();
        virtual protected void OnClosing()
        {
            if (Mgr != null)
            {
                Mgr.OnConnectionClosing(this);
            }
        }
        virtual protected void OnClose(List<Exception> exceptions)
        {
            if (Mgr != null)
            {
                Mgr.OnConnectionClosed(this, exceptions);
            }
        }

        virtual protected void OnConnect()
        {
            if (Mgr != null)
            {
                Mgr.OnConnectionConnected(this);
            }
        }

        virtual protected void OnOpen()
        {
            if (Mgr != null)
            {
                Mgr.OnConnectionOpened(this);
            }
        }


        virtual protected void HandleReceivedMessage(Message message)
        {
            if (Mgr != null)
            {
                try
                {
                    Mgr.HandleReceivedMessage(this, message);
                } catch (MessageHandlingException e)
                {
                    throw e;
                }
                catch (Exception e)
                {
                    throw new MessageHandlingException(message, e);
                }
            }
        }

        virtual public bool CanOpen()
        {
            return State == ConnectionState.NOT_SET || State == ConnectionState.CLOSED;
        }

        virtual public bool CanSend()
        {
            return Stream != null && (State == ConnectionState.CONNECTED || State == ConnectionState.RECEIVING || State == ConnectionState.SENDING);
        }

        virtual public bool CanReceive()
        {
            return Stream != null && (State == ConnectionState.CONNECTED || State == ConnectionState.RECEIVING || State == ConnectionState.SENDING);
        }

        virtual protected String Read()
        {
            byte[] buffer = new byte[1024];

            StringBuilder serializedMessage = new StringBuilder();
            int bytesRead = 0;

            // Incoming message may be larger than the buffer size.
            do
            {
                bytesRead = Stream.Read(buffer, 0, buffer.Length);
                if (bytesRead == 0) return null;
                State = ConnectionState.RECEIVING;

                serializedMessage.AppendFormat("{0}", Encoding.UTF8.GetString(buffer, 0, bytesRead));
            }
            while (bytesRead == buffer.Length);

            return serializedMessage.ToString();
        }

        virtual protected void Write(String data)
        {
            State = ConnectionState.SENDING;

            byte[] msg = Encoding.UTF8.GetBytes(data);
            Stream.Write(msg, 0, msg.Length);
            Stream.Flush();
        }

        virtual protected String CreateSignature(String sender = null)
        {
            if(AuthToken == null || AuthToken.Length == 0)
            {
                throw new Exception("Cannot creat signature without AuthToken");
            }
            return (sender == null ? Name : sender) + "-" + AuthToken;
        }

        virtual protected bool IsValidSignature(Message message)
        {
            var vsig = CreateSignature(message.Sender);
            return vsig == message.Signature;
        }

        protected void ReceiveMessage()
        {
            if (!CanReceive())
            {
                throw new MessageIOException(String.Format("Cannot receive message in state {0}", State));
            }

            String data = null;
            data = Read();
            if (data == null)
            {
                throw new MessageIOException(this, String.Format("Connection::ReceiveMessage: Returned null from Connection::Read {0}", ID));
            }
            
            //This will block thread while waiting for a message
            if (data != null && data.Length > 0)
            {


                Message message = null;
                try
                {
                    message = Message.Deserialize(data, MEncoding);
                } catch (ArgumentException)
                {
                    Tracing?.TraceEvent(TraceEventType.Verbose, 2000, "Connection::ReceiveMessage: connection {0} encountered garbage {1}", ToString(), data);
                    GarbageReceived++;
                }
                if (message != null)
                {
                    if (ValidateMessageSignature && !IsValidSignature(message))
                    {
                        throw new MessageHandlingException(String.Format("Message signature {0} is not valid", message.Signature), message);
                    }

                    MessagesReceived++;
                    HandleReceivedMessage(message);
                }
            }
        }

        virtual public void SendMessage(Message message)
        {
            if(!CanSend())
            {
                throw new MessageIOException(String.Format("Cannot send messge in state {0}", State));
            }

            //introduce some 'throtttling' so messages don't bunch up on the stream and
            //arrive at the other end as more than one per 'read' as this causes deserialization issues
            if(LastMessageSentOn > 0 && ((DateTime.Now.Ticks - LastMessageSentOn) < 1000))
            {
                System.Threading.Thread.Sleep(1);
            }
            
            try
            {
                if (SignMessage)
                {
                    message.Signature = CreateSignature(message.Sender); 
                }

                String serialized = message.Serialize(MEncoding);
                Write(serialized);
                LastMessageSentOn = DateTime.Now.Ticks;
                MessagesSent++;
            }
            catch (Exception e)
            {
                //Console.WriteLine("Connection::SendMessage: " + e.Message);
                throw e;
            }
            
        }

        public Message CreateResponse(Message message, MessageType mtype)
        {
            var response = new Message(mtype);
            response.ResponseID = message.ID;
            response.Target = message.Sender;

            return response;
        }

        virtual public Message CreateStatusResponse(Message request)
        {
            var m = CreateResponse(request, MessageType.STATUS_RESPONSE);
            m.AddValue("ConnectionID", ID);
            m.AddValue("Name", Name);
            m.AddValue("State", State.ToString());
            m.AddValue("ActivityTimeout", ActivityTimeout);
            m.AddValue("ConnectionTimeout", ConnectionTimeout);
            m.AddValue("RemainConnected", RemainConnected);
            m.AddValue("RemainOpen", RemainOpen);
            m.AddValue("MessageEncoding", MEncoding.ToString());
            m.AddValue("MessagesReceived", MessagesReceived);
            m.AddValue("GarbageReceived", GarbageReceived);
            m.AddValue("MessagesSent", MessagesSent);
            return m;
        }

        public override string ToString()
        {
            return ID + " " + Name + " " + State;
        }
    } //end Connection cloass

    abstract public class ServerConnection : Connection
    {
        public ServerConnection(String cnnId, int cnnTimeout, int actTimeout) : base(cnnId, cnnTimeout, actTimeout)
        {

        }

        abstract protected void Listen();

        override public bool Open()
        {
            if (base.Open())
            {
                Start(this.Listen);
                return true;
            }
            else
            {
                return false;
            }
        }

        override public void SendMessage(Message message)
        {
            if (Name != null && message.Target == null)
            {
                //if this connection has a name then set the Target to the name for clarity
                //this is done because the Name is normally set to the requester of the connection (i.e. client)
                //See ConnectionManager -> Server and how it creates and initialises connections to clients
                message.Target = Name;
            }

            if(message.Sender == null)
            {
                message.Sender = ServerID;
            }
            base.SendMessage(message);
        }
    } //end server conneciton class


    abstract public class ClientConnection : Connection
    {
        public enum ClientContext
        {
            NOT_SET,
            SERVICE,
            CONTROLLER
        }

        public MessageHandler HandleMessage = null;
        public MessageModifier ModifyMessage = null;
        public ErrorHandler HandleError = null;

        protected Dictionary<String, ConnectionManager.Subscriber> Subscribers = new Dictionary<string, ConnectionManager.Subscriber>();

        private bool _tracing2Client = false;

        public bool AlwaysConnect { get; set; } = true;

        public ClientContext Context { get; set; } = ClientContext.NOT_SET;

        public ClientConnection() : base()
        {
            RemainOpen = false;
        }

        public abstract void ParseConnectionString(String connectionString);
        public abstract void ParseMessage(Message message);

        abstract protected void Connect();

        override public bool Open()
        {
            if (base.Open())
            {
                Start(this.Connect);
                return true;
            }
            else
            {
                return false;
            }
        }

        override protected void OnClosing()
        {
            base.OnClosing();
            if (_tracing2Client)
            {
                _tracing2Client = false;
                StopTracingToClient();
            }
        }

        override protected void OnClose(List<Exception> exceptions)
        {
            base.OnClose(exceptions);
            if(HandleError != null)
            {
                foreach(var e in exceptions)
                {
                    HandleError.Invoke(this, e);
                }
            }
            
        }

        public void AddSubscriber(ConnectionManager.Subscriber sub)
        {
            if (!Subscribers.ContainsKey(sub.Name)) Subscribers[sub.Name] = sub;
        }

        public void RemoveSubscriber(String name)
        {
            if (Subscribers.ContainsKey(name)) Subscribers.Remove(name);
        }

        private void HandleMessageDelegateWrapper(Message message)
        {
            HandleMessage?.Invoke(this, message);
        }

        override protected void HandleReceivedMessage(Message message)
        {
            base.HandleReceivedMessage(message);

            Message response = null;
            switch (message.Type)
            {
                case MessageType.TRACE:
                    _tracing2Client = true;
                    HandleMessage?.Invoke(this, message);
                    break;

                case MessageType.PING:
                    response = CreatePingResponse(message);
                    SendMessage(response);
                    break;

                case MessageType.STATUS_REQUEST:
                    response = CreateStatusResponse(message);
                    SendMessage(response);
                    break;

                case MessageType.SUBSCRIBE:
                    if (message.HasValue("Subscriber"))
                    {
                        try
                        {
                            AddSubscriber(ConnectionManager.Subscriber.Parse(message.GetString("Subscriber")));
                        }
                        catch (Exception e)
                        {
                            Tracing?.TraceEvent(TraceEventType.Error, 2000, e.Message);
                        }
                    }
                    break;

                case MessageType.UNSUBSCRIBE:
                    if (message.HasValue("Subscriber"))
                    {
                        RemoveSubscriber(message.GetString("Subscriber"));
                    }
                    break;

                default:
                    int priorSize = ThreadExecutionManager.MaxQueueSize;
                    ThreadExecutionManager.MaxQueueSize = 256;
                    ThreadExecutionManager.Execute<Message>("HandleMessage-" + ID, HandleMessageDelegateWrapper, message);
                    ThreadExecutionManager.MaxQueueSize = priorSize;
                    break;
            }
        }

        override public void SendMessage(Message message)
        {
            if (Name != null && message.Sender == null)
            {
                message.Sender = Name;
            }

            if(message.Type == MessageType.COMMAND && message.SubType == (int)Server.CommandName.STOP_TRACE_TO_CLIENT)
            {
                _tracing2Client = false;
            }

            ModifyMessage?.Invoke(this, message);

            base.SendMessage(message);
        }

        public Message SendPing(String target = null)
        {
            var msg = new Message();
            msg.Type = MessageType.PING;
            if (target != null)
            {
                msg.Target = target;
            }
            SendMessage(msg);
            return msg;
        }

        /*
         *Server directed messages 
         */

        public Message PingServer()
        {
            return SendPing(null);
        }

        public Message SendServerCommand(Server.CommandName cmd, params Object[] cmdParams)
        {
            return SendServerCommand(cmd, cmdParams.ToList());
        }

        public Message SendServerCommand(Server.CommandName cmd, List<Object> cmdParams)
        {
            var command = new Message();
            command.Type = MessageType.COMMAND;
            command.SubType = (int)cmd;
            command.Target = ServerID;

            //now add in extra params
            switch (cmd)
            {
                case Server.CommandName.SET_TRACE_LEVEL:
                    if(cmdParams.Count < 2)
                    {
                        throw new Exception("Please supply a listener name and trace level");
                    }

                    command.AddValue("Listener", cmdParams[0].ToString());
                    command.AddValue("TraceLevel", (int)cmdParams[1]);
                    break;

                case Server.CommandName.RESTORE_TRACE_LEVEL:
                    if (cmdParams.Count < 1)
                    {
                        throw new Exception("Please supply a listener name");
                    }

                    command.AddValue("Listener", cmdParams[0].ToString());
                    break;

                case Server.CommandName.ECHO_TRACE_TO_CLIENT:
                    if (cmdParams.Count < 1)
                    {
                        throw new Exception("Please supply something to echo");
                    }

                    command.Value = (String)cmdParams[0];
                    break;

                case Server.CommandName.CLOSE_CONNECTION:
                    if (cmdParams.Count < 1)
                    {
                        throw new Exception("Please supply connection ID");
                    }
                    command.AddValue("ConnectionID", cmdParams[0].ToString());
                    break;
            }
            
            SendMessage(command);

            return command;
        }

        public Message RequestServerStatus()
        {
            if (!IsConnected) throw new Exception("Connection::RequestStatus: cannot request because not connected");

            var request = new Message();
            request.Type = MessageType.STATUS_REQUEST;
            SendMessage(request);

            return request;
        }

        public Message RequestServerConnectionStatus(String cnnId = null)
        {
            if (!IsConnected) throw new Exception("Connection::RequestStatus: cannot request because not connected");

            var request = new Message();
            request.Type = MessageType.STATUS_REQUEST;
            request.AddValue("ConnectionID", cnnId == null ? Name : cnnId);
            SendMessage(request);

            return request;
        }

        public Message RequestClientConnectionStatus(String target = null)
        {
            var request = new Message();
            request.Type = MessageType.STATUS_REQUEST;
            request.Target = target == null ? Name : target;
            SendMessage(request);

            return request;
        }

        public Message SetListenerTraceLevel(String listenerName, SourceLevels level)
        {
            return SendServerCommand(Server.CommandName.SET_TRACE_LEVEL, listenerName, level);
        }

        public Message SetListenerTraceLevel(String listenerName, String level)
        {
            SourceLevels l = (SourceLevels)Enum.Parse(typeof(SourceLevels), level, true);
            return SetListenerTraceLevel(listenerName, l);
        }

        public Message RestoreListenerTraceLevel(String listenerName)
        {
            return SendServerCommand(Server.CommandName.RESTORE_TRACE_LEVEL, listenerName);
        }

        public Message StartTracingToClient()
        {
            return SendServerCommand(Server.CommandName.START_TRACE_TO_CLIENT);
        }


        public Message EchoTracingToClient(String toEcho)
        {
            return SendServerCommand(Server.CommandName.ECHO_TRACE_TO_CLIENT, toEcho);
        }

        public Message StopTracingToClient()
        {
            return SendServerCommand(Server.CommandName.STOP_TRACE_TO_CLIENT);
        }

        public Message CloseServerConnection(String cnnId)
        {
            return SendServerCommand(Server.CommandName.CLOSE_CONNECTION, cnnId);
        }

        public Message Subscribe(String clients)
        {
            var msg = new Message();
            msg.Type = MessageType.SUBSCRIBE;
            msg.Value = "Subscription request from " + Name;
            msg.AddValue("Clients", clients);
            SendMessage(msg);

            return msg;
        }

        public Message Unsubscribe(String clients)
        {
            var msg = new Message();
            msg.Type = MessageType.UNSUBSCRIBE;
            msg.Value = "Unsubscibe request from " + Name;
            msg.AddValue("Clients", clients);
            SendMessage(msg);

            return msg;
        }

        //Notify Subscribers
        public bool CanNotify(MessageType messageType)
        {
            if (Subscribers.Count == 0) return false;
            bool canNotify = false;
            foreach (var sub in Subscribers.Values)
            {
                if (sub.SubscribesTo(messageType))
                {
                    canNotify = true;
                    break;
                }
            }
            return canNotify;
        }

        public bool Notify(Message message)
        {
            if (!CanNotify(message.Type)) return false;

            if(message.Type == MessageType.NOT_SET)
            {
                message.Type = MessageType.NOTIFICATION;
            }
            message.Sender = Name; //we have to set the Sender to the client name to ensure that subscribers get the message
            message.Target = ServerID;
            SendMessage(message);
            return true;
        }

        public Message Notify(String msg, MessageType messageType = MessageType.NOTIFICATION)
        {
            if (!CanNotify(messageType)) return null;

            var message = new Message();
            message.Type = messageType;
            message.Value = msg;
            Notify(message);
            return message;
        }

        /*
         * Client directed messages
         */

        public void SendMessage(String target, Message message)
        {
            message.Target = target;
            SendMessage(message);
        }

        public Message SendMessage(String target, String message, MessageType type = MessageType.INFO)
        {
            var msg = new Message();
            msg.Type = type;
            msg.Value = message;
            SendMessage(target, msg);

            return msg;
        }

        public Message SendCommand(String target, String command, List<Object> args = null)
        {
            var msg = new Message();
            msg.Type = MessageType.COMMAND;
            msg.Value = command;
            msg.AddValue("Arguments", args);
            SendMessage(target, msg);

            return msg;
        }

        public Message PingClient(String target)
        {
            return SendPing(target);
        }

        //Message creationg functions
        override public Message CreateStatusResponse(Message request)
        {
            var m = base.CreateStatusResponse(request);
            m.AddValue("Context", Context.ToString());
            m.AddValue("Subscribers", Subscribers.Select(i => i.ToString()));
            return m;
        }

        virtual public Message CreatePingResponse(Message request)
        {
            var m = new Message();
            m.Type = MessageType.PING_RESPONSE;
            m.ResponseID = request.ID;
            m.Target = request.Sender;
            return m;
        }

    } //end Client connection class
}