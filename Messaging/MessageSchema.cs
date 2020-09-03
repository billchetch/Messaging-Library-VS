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
        public Message Message { get; set; }

        public MessageSchema() { }
    }
}
