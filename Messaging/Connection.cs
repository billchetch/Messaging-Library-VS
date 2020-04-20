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
        public bool RemainConnected { get; set; } = false;
        public bool RemainOpen { get; set; } = false;
        public String ServerID { get; set; }


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
                    Tracing?.TraceEvent(TraceEventType.Warning, 2000, "Connection::Monitor: Exeuction thread is of state {0} but connection is of state {1} so setting connection stateo to closed.", _startXS.State, State);
                    State = ConnectionState.CLOSED;
                }

                if (TimedOut(ConnectionTimeout, ConnectionState.OPENED))
                {
                    OnConnectionTimeout();
                }

                if (TimedOut(ActivityTimeout, ConnectionState.CONNECTED))
                {
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
                Tracing?.TraceEvent(TraceEventType.Warning, 2000, "Connection::Close: {0} already of state CLOSED", ID);
                return;
            }

            Tracing?.TraceEvent(TraceEventType.Verbose, 2000, "Connection::Close: {0} started closing", ID);

            State = ConnectionState.CLOSING;

            if (Stream != null)
            {
                try
                {
                    Stream.Close();
                    Stream.Dispose();
                }
                catch (System.ObjectDisposedException)
                {
                    //sometimes the 'Open' thread will dispose of this and then a managing thread will try and close
                    //resulting in an ObjectDisposedException... which we ignore since the important thing is the Stream
                    //has been closed
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
                    var x = new MessageHandlingException(message, e);
                    throw x;
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
        }

        protected void ReceiveMessage()
        {
            ConnectionState oldState = State;
            String data = null;
            try
            {
                data = Read();
                if (data == null)
                {
                    throw new System.IO.IOException("Connection::ReceiveMessage: Returned null from Connection::Read " + ID + " " + GetType().ToString());
                }
            }
            finally
            {
                State = oldState;
            }

            //This will block thread while waiting for a message
            if (data != null && data.Length > 0)
            {
                var message = Message.Deserialize(data);
                HandleReceivedMessage(message);
            }
        }

        virtual public void SendMessage(Message message)
        {
            ConnectionState oldState = State;
            try
            {
                String serialized = message.Serialize();
                Write(serialized);
            }
            catch (Exception e)
            {
                //Console.WriteLine("Connection::SendMessage: " + e.Message);
                throw e;
            }
            finally
            {
                State = oldState;
            }
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

        public MessageHandler HandleMessage = null;
        public ErrorHandler HandleError = null;

        protected Dictionary<String, Message> Subscribers = new Dictionary<string, Message>();
        protected Dictionary<String, Message> Subscriptions = new Dictionary<string, Message>();

        private bool _tracing2Client = false;

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

        override protected void HandleReceivedMessage(Message message)
        {
            base.HandleReceivedMessage(message);
            switch (message.Type)
            {
                case MessageType.SUBSCRIBE:
                    if (message.Sender != null && !Subscribers.ContainsKey(message.Sender))
                    {
                        Subscribers[message.Sender] = message;
                    }
                    break;

                case MessageType.UNSUBSCRIBE:
                    if (message.Sender != null && Subscribers.ContainsKey(message.Sender))
                    {
                        Subscribers.Remove(message.Sender);
                    }
                    break;

                case MessageType.TRACE:
                    _tracing2Client = true;
                    HandleMessage?.Invoke(this, message);
                    break;

               //TODO: handle error messages...
                default:
                    HandleMessage?.Invoke(this, message);
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

            base.SendMessage(message);
        }

        /*
         *Server directed messages 
         */
        public void SendServerCommand(Server.CommandName cmd, params Object[] cmdParams)
        {
            var command = new Message();
            command.Type = MessageType.COMMAND;
            command.SubType = (int)cmd;
            command.Target = ServerID;

            //now add in extra params
            switch (cmd)
            {
                case Server.CommandName.SET_TRACE_LEVEL:
                    if(cmdParams.Length < 2)
                    {
                        throw new Exception("Please supply a listener name and trace level");
                    }

                    command.AddValue("Listener", cmdParams[0].ToString());
                    command.AddValue("TraceLevel", (int)cmdParams[1]);
                    break;

                case Server.CommandName.RESTORE_TRACE_LEVEL:
                    if (cmdParams.Length < 1)
                    {
                        throw new Exception("Please supply a listener name");
                    }

                    command.AddValue("Listener", cmdParams[0].ToString());
                    break;

                case Server.CommandName.ECHO_TRACE_TO_CLIENT:
                    if (cmdParams.Length < 1)
                    {
                        throw new Exception("Please supply something to echo");
                    }

                    command.Value = (String)cmdParams[0];
                    break;
            }


            SendMessage(command);
        }

        public void RequestServerStatus()
        {
            if (!IsConnected) throw new Exception("Connection::RequestStatus: cannot request because not connected");

            var request = new Message();
            request.Type = MessageType.STATUS_REQUEST;
            SendMessage(request);
        }

        public void SetListenerTraceLevel(String listenerName, SourceLevels level)
        {
            SendServerCommand(Server.CommandName.SET_TRACE_LEVEL, listenerName, level);
        }

        public void SetListenerTraceLevel(String listenerName, String level)
        {
            SourceLevels l = (SourceLevels)Enum.Parse(typeof(SourceLevels), level, true);
            SetListenerTraceLevel(listenerName, l);
        }

        public void RestoreListenerTraceLevel(String listenerName)
        {
            SendServerCommand(Server.CommandName.RESTORE_TRACE_LEVEL, listenerName);
        }

        public void StartTracingToClient()
        {
            SendServerCommand(Server.CommandName.START_TRACE_TO_CLIENT);
        }


        public void EchoTracingToClient(String toEcho)
        {
            SendServerCommand(Server.CommandName.ECHO_TRACE_TO_CLIENT, toEcho);
        }

        public void StopTracingToClient()
        {
            SendServerCommand(Server.CommandName.STOP_TRACE_TO_CLIENT);
        }


        /*
         * Client directed messages
         */

        public void SendMessage(String target, Message message)
        {
            message.Target = target;
            SendMessage(message);
        }

        public void SendMessage(String target, String message, MessageType type = MessageType.INFO)
        {
            var msg = new Message();
            msg.Type = type;
            msg.Value = message;
            SendMessage(target, msg);
        }

        public void SendCommand(String target, String command, List<Object> args = null)
        {
            var msg = new Message();
            msg.Type = MessageType.COMMAND;
            msg.Value = command;
            msg.AddValue("Arguments", args);
            SendMessage(target, msg);
        }

        virtual public void Subscribe(String target)
        {
            var msg = new Message();
            msg.Type = MessageType.SUBSCRIBE;
            msg.Value = "Subscription request from " + Name;
            msg.Target = target;
            var targets = target.Split(',');
            foreach (var tgt in targets)
            {
                Subscriptions[tgt.Trim()] = msg;
            }
            SendMessage(target, msg);
        }

        public void Unsubscribe(String target)
        {
            var targets = target.Split(',');
            foreach (var tgt in targets)
            {
                Subscriptions.Remove(tgt.Trim());
            }

            SendMessage(target, "Unsubscribing", MessageType.UNSUBSCRIBE);
        }

        public void Notify(Message message)
        {
            if (Subscribers.Count > 0)
            {
                String targets = String.Join(",", Subscribers.Keys.ToArray());
                SendMessage(targets, message);
            }
        }

        public void Notify(String message, MessageType type = MessageType.INFO)
        {
            var msg = new Message();
            msg.Type = type;
            msg.Value = message;
            Notify(msg);
        }

    } //end Client connection class
}