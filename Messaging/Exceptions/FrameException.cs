using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chetch.Messaging.Exceptions
{
    public class FrameException : Exception
    {
        public Frame.FrameError Error { get; internal set; }


        public FrameException(Frame.FrameError error)
        {
            Error = error;
        }

        public FrameException(Frame.FrameError error, String message) : base(message)
        {
            Error = error;
        }
    }
}
