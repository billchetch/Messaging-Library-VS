using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chetch.Utilities;

namespace Chetch.Messaging
{
    public static class Frame
    {
        public enum Checksum
        {
            NONE = 0,
            SIMPLE = 1,
        }

        public static byte[] Create(MessageEncoding encoding, byte[] payload, Checksum checksum = Checksum.NONE)
        {
            if (payload == null || payload.Length == 0)
            {
                throw new Exception("Cannot create a frame without a payload");
            }

            //calculate the number of bytes required for the payload
            int n = 0;
            int p = payload.Length;
            while (p != 0)
            {
                p >>= 8;
                n++;
            }


            //create a list for the frame
            List<byte> frame = new List<byte>();
            
            //first byte is the encoding
            byte firstByte = (byte)encoding;
            frame.Add(firstByte);

            //second byte is the number of bytes required to specify payload length and the number of bytes for checksum length 
            byte secondByte = (byte)((n << 4) + checksum);
            frame.Add(secondByte);
            byte[] temp = Chetch.Utilities.Convert.ToBytes(payload.Length);
            frame.AddRange(temp);

            //now we add the payload
            frame.AddRange(payload);

            //finally we add the checksum
            switch (checksum)
            {
                case Checksum.SIMPLE:
                    frame.Add(CheckSum.SimpleAddition(frame.ToArray()));
                    break;
            }

            return frame.ToArray();
        }
    }
}
