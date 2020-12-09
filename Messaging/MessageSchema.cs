using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chetch.Messaging
{
    /// <summary>
    /// Use this class to define message values (both populating and retreiving)...
    /// The idea is to derive a domain specific schema from this class to use in populating and retreiving data in a consistent and structured form.
    /// </summary>
    public class MessageSchema
    {
        static public T CreateSchema<T>(MessageType messageType, String target) where T : MessageSchema, new()
        {
            Message msg = new Message(messageType);
            msg.Target = target;
            T t = new T();
            t.Message = msg;

            return t;
        }

        public Message Message { get; set; }


        public MessageSchema() { }

        public MessageSchema(Message message)
        {
            Message = message;
        }

        //TODO: add conforms chect
        virtual public bool ConformsToSchema(Message message)
        {
            return true;
        }

        public bool MessageConforms()
        {
            return ConformsToSchema(Message);
        }

        public void AssertMessageConforms()
        {
            AssertConformsToSchema(Message);
        }

        public void AssertConformsToSchema(Message message)
        {
            if (!ConformsToSchema(message))
            {
                throw new Exception(String.Format("Message (type={0}) from {1} does not conform to schema {2}", message.Type, message.Sender, this.GetType().FullName));
            }
        }
    }
}
