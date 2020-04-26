using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chetch.Messaging
{
    public class MessageIOException : Exception
    {
        public Connection.ConnectionState ConnectionState { get; internal set; } = Connection.ConnectionState.NOT_SET;
        public String ConnectionName { get; internal set; } = null;

        public MessageIOException(String message) : base(message)
        {

        }

        public MessageIOException(Connection cnn, String message) : this(message)
        {
            ConnectionState = cnn.State;
            ConnectionName = cnn.Name;
        }
    }
}
