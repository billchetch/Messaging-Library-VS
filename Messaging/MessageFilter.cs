﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chetch.Messaging
{
    /// <summary>
    /// Set filter criteria, provide an on Matched handler and then assign HandleMessage to a Client.HandleMessage event to receive filtered messages
    /// </summary>
    public class MessageFilter
    {
        public String Sender { get; internal set; }
        private List<MessageType> _types = null;
        private Action<MessageFilter, Message> _onMatched;

        public bool HasMatchedListener { get { return _onMatched != null; } }

        public MessageFilter(String sender, MessageType type, Action<MessageFilter, Message> onMatched)
        {
            Sender = sender;
            _types = new List<MessageType>();
            _types.Add(type);
            _onMatched = onMatched;
        }

        public MessageFilter(String sender, MessageType[] types, Action<MessageFilter, Message> onMatched)
        {
            Sender = sender;
            _types = new List<MessageType>(types);
            _onMatched = onMatched;
        }

        public MessageFilter(MessageType[] types, Action<MessageFilter, Message> onMatched) : this(null, types, onMatched) { }

        public MessageFilter(MessageType type, Action<MessageFilter, Message> onMatched) : this(null, type, onMatched) { }

        public MessageFilter(String sender, Action<MessageFilter, Message> onMatched)
        {
            Sender = sender;
            _onMatched = onMatched;
        }

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

            return matched;
        }

        virtual protected void OnMatched(Message message)
        {
            _onMatched?.Invoke(this, message);
        }
    }
}