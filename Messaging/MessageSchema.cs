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
        protected Message Message;

        public MessageSchema(Message message)
        {
            Message = message;
        }

        //TODO: add conforms chect
        virtual public bool ConformsToSchema()
        {
            return true;
        }

        public void AssertConformsToSchema()
        {
            if (!ConformsToSchema())
            {
                throw new Exception(String.Format("Message (type={0}) from {1} does not conform to schema {2}", Message.Type, Message.Sender, this.GetType().FullName));
            }
        }
    }
}
