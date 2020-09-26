using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chetch.Messaging
{

    public delegate void MatchedHandler(MessageFilter messageFilter, Message message);

    /// <summary>
    /// Set filter criteria, provide an on Matched handler and then assign HandleMessage to a Client.HandleMessage event to receive filtered messages
    /// </summary>
    public class MessageFilter
    {
        public String Sender { get; internal set; }
        private List<MessageType> _types = null;
        private List<String> _requiredKeys = new List<string>();
        private List<Object> _requiredVals = new List<object>();

        public event MatchedHandler HandleMatched;
        
        public MessageFilter(String sender, MessageType[] types, String requiredKeys, params Object[] requiredVals)
        {
            Sender = sender;
            _types = new List<MessageType>(types);
            if (requiredKeys != null)
            {
                String[] splitted = requiredKeys.Split(',');
                foreach (String s in splitted)
                {
                    _requiredKeys.Add(s.Trim());
                }
            }

            if(requiredVals != null && requiredVals.Length > 0)
            {
                _requiredVals.AddRange(requiredVals);
            }
        }

        public MessageFilter(String sender, MessageType type, String requiredKeys, params Object[] requiredVals) :
            this(sender, new MessageType[] { type }, requiredKeys, requiredVals)
        { }


        public MessageFilter(String sender)
        {
            Sender = sender;
        }

        public MessageFilter(String sender, MessageType[] types) : this(sender, types, null) { }

        public MessageFilter(MessageType[] types) : this(null, types, null) { }

        public MessageFilter(String sender, MessageType type) : this(sender, type, null) { }

        public MessageFilter(MessageType type) : this(null, type, null) { }
        

        public void HandleMessage(Connection cnn, Message message)
        {
            if (Matches(message))
            {
                OnMatched(message);    
            }
        }

        virtual protected bool Matches(Message message)
        {
            bool matched = true;
            if (Sender != null && message.Sender != null)
            {
                matched = Sender.Equals(message.Sender);
                if (!matched) return false;
            }

            if (_types != null && _types.Count > 0)
            {
                matched = _types.Contains(message.Type);
                if (!matched) return false;
            }

            if(_requiredKeys.Count > 0)
            {
                foreach(String k in _requiredKeys)
                {
                    if (!message.HasValue(k)) return false;
                }
            }

            if (_requiredVals.Count == _requiredKeys.Count)
            {
                for(int i = 0; i < _requiredVals.Count; i++)
                {
                    String k = _requiredKeys[i];
                    Object v = _requiredVals[i];
                    if (message.GetValue(k) != v) return false;
                }
            }

            return matched;
        }

        virtual protected void OnMatched(Message message)
        {
            HandleMatched?.Invoke(this, message);
        }
    }
}
