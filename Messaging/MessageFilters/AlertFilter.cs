using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chetch.Messaging.MessageFilters
{
    public class AlertFilter : MessageFilter
    {
        public AlertFilter(String sender, String requiredKeys, params Object[] requiredVals) : 
            base(sender, MessageType.ALERT, requiredKeys, requiredVals)
        { }
    }
}
