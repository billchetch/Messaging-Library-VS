using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chetch.Messaging
{
    public class MessageIOException : Exception
    {
        public MessageIOException(String message) : base(message)
        {

        }
    }
}
