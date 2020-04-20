using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chetch.Messaging
{
    public class MessageHandlingException : Exception
    {
        public Message Message { get; set; }

        public MessageHandlingException(String msg, Message message, Exception e) : base(msg, e)
        {
            Message = message;
        }

        public MessageHandlingException(String msg, Message message) : base(msg)
        {
            Message = message;
        }

        public MessageHandlingException(Message message) : base("no exception message supplied")
        {
            Message = message;
        }

        public MessageHandlingException(Message message, Exception e) : base(e.Message, e)
        {
            Message = message;
        }
    }
}
