using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chetch.Utilities;
using Chetch.Messaging.Exceptions;

namespace Chetch.Messaging
{
    public class Frame
    {
        public enum FrameSchema
        {
            SMALL_NO_CHECKSUM = 1,      //FrameSchema = 1 byte, Encoding = 1 byte, Payload size = 1 byte, Payload = max 255 bytes
            SMALL_SIMPLE_CHECKSUM,      //FrameSchema = 1 byte, Encoding = 1 byte, Payload size = 1 byte, Payload = max 255 bytes, Checksum = 1 byte
            MEDIUM_NO_CHECKSUM,         //FrameSchema = 1 byte, Encoding = 1 byte, Payload size = 2 bytes, Payload = max 32K bytes
            MEDIUM_SIMPLE_CHECKSUM      //FrameSchema = 1 byte, Encoding = 1 byte, Payload size = 2 bytes, Payload = max 32K bytes, Checksum = 4 bytes
        }

        public enum FrameError
        {
            NO_ERROR = 0,
            NO_DIMENSIONS,
            NO_HEADER,
            NO_PAYLOAD,
            INCOMPLETE_DATA,
            NON_VALID_SCHEMA,
            NON_VALID_ENCODING,
            CHECKSUM_FAILED,
            ADD_TIMEOUT
        }

        public class FrameDimensions
        {
            public int Schema { get; internal set; } //nb this is the dimension NOT the schema value
            public int Encoding { get; internal set; } //nb this is the dimension NOT the encoding value
            public int PayloadSize { get; internal set; }
            public int Checksum { get; internal set; }
            public int Payload { get; set; } = -1;

            public FrameDimensions(FrameSchema schema)
            {
                Schema = 1; //nb this is the dimension NOT the schema value
                Encoding = 1; //nb this is the dimension NOT the encoding value
                switch (schema)
                {
                    case FrameSchema.SMALL_NO_CHECKSUM:
                    case FrameSchema.SMALL_SIMPLE_CHECKSUM:
                        PayloadSize = 1;
                        Checksum = schema == FrameSchema.SMALL_SIMPLE_CHECKSUM ? 1 : 0;
                        break;

                    case FrameSchema.MEDIUM_NO_CHECKSUM:
                    case FrameSchema.MEDIUM_SIMPLE_CHECKSUM:
                        PayloadSize = 2;
                        Checksum = schema == FrameSchema.MEDIUM_SIMPLE_CHECKSUM ? 1 : 0;
                        break;
                }
            }

            public int SchemaIndex
            {
                get
                {
                    return 0;
                }

            }
            public int EncodingIndex
            {
                get
                {
                    return SchemaIndex + Schema;
                }
            }
            public int PayloadSizeIndex
            {
                get
                {
                    return EncodingIndex + Encoding;
                }
            }
            public int PayloadIndex
            {
                get
                {
                    return PayloadSizeIndex + PayloadSize;
                }
            }
            public int ChecksumIndex
            {
                get
                {
                    if (Checksum <= 0) throw new Exception("There is no checksum for this frame schema");
                    if (Payload <= 0) throw new Exception("Payload dimension has no value");
                    return PayloadIndex + Payload;
                }
            }

            public int Size
            {
                get
                {
                    if (Payload <= 0) return -1;
                    return Schema + Encoding + PayloadSize + Payload + Checksum;
                }
            }
        }


        public FrameSchema Schema { 
            get 
            {
                return (FrameSchema)_bytes[0];
            } 
            internal set 
            {
                setByteAt((byte)value, 0);
            } 
        }

        public FrameDimensions Dimensions { get; internal set; }
        public MessageEncoding Encoding { 
            get
            {
                return (MessageEncoding)_bytes[1];
            } 
            set 
            { 
                setByteAt((byte)value, 1);
                
            } 
        }

        public byte[] Payload
        {
            get
            {
                if (Dimensions.Payload > 0 && _bytes.Count >= Dimensions.PayloadIndex + Dimensions.Payload)
                {
                    return _bytes.GetRange(Dimensions.PayloadIndex, Dimensions.Payload).ToArray();
                } else
                {
                    return null;
                }
            }

            set
            {
                byte encoding = (byte)Encoding;
                byte schema = (byte)Schema;

                _bytes.Clear();
                _bytes.Add(schema);
                _bytes.Add(encoding);
                byte[] payloadSize = Utilities.Convert.ToBytes(value.Length, Dimensions.PayloadSize);
                _bytes.AddRange(payloadSize);
                for (int i = 0; i < value.Length; i++)
                {
                    _bytes.Add(value[i]);
                }
                Dimensions.Payload = value.Length;
            }
        }

        private List<byte> _bytes = new List<byte>();
        private int _addPosition = 0;

        public bool Complete { get; internal set; } = false;
        

        public Frame(FrameSchema schema)
        {
            Schema = schema;
            Dimensions = new FrameDimensions(schema);
        }

        public Frame(FrameSchema schema, MessageEncoding encoding) : this(schema)
        {
            Encoding = encoding;
        }

        private void setByteAt(byte b, int idx)
        {
            if(idx < _bytes.Count)
            {
                _bytes[idx] = b;
            } else
            {
                for(int i = _bytes.Count; i <= idx; i++)
                {
                    _bytes.Add(0);
                }
                _bytes[idx] = b;
            }
        }

        public bool Add(byte b)
        {
            if (Complete)
            {
                throw new Exception("Frame already complete");
            }

            if (_addPosition == 0)
            {
                if(b != (byte)Schema)
                {
                    throw new FrameException(FrameError.NON_VALID_SCHEMA, String.Format("{0} is not a valid schema", b));
                }
            }
            else if (_addPosition == Dimensions.PayloadIndex)
            {
                Encoding = (MessageEncoding)_bytes[1];
                Dimensions.Payload = GetInt(Dimensions.PayloadSizeIndex, Dimensions.PayloadSize);
                if(Dimensions.Payload <= 0)throw new FrameException(FrameError.INCOMPLETE_DATA, "Payload dimensions must be 1 or more");
            }

            setByteAt(b, _addPosition);
            _addPosition++;
            Complete = Dimensions.Payload > 0 && _addPosition == Dimensions.Size;

            return Complete;
        }

        public bool Add(List<byte> bytes)
        {
            bool complete = false;
            foreach(byte b in bytes)
            {
                complete = Add(b);
            }
            return complete;
        }

        public void Reset()
        {
            Complete = false;
            _addPosition = 0;
            FrameSchema schema = Schema;
            byte encoding = _bytes.Count > 1 ? _bytes[1] : (byte)0;
            _bytes.Clear();
            Schema = schema;
            if (encoding > 0) Encoding = (MessageEncoding)encoding;   
        }

        public void Validate()
        {
            if (!Complete)
            {
                throw new FrameException(Frame.FrameError.INCOMPLETE_DATA);
            }
            if (Payload == null || Dimensions.Payload == 0)
            {
                throw new FrameException( Frame.FrameError.NO_PAYLOAD);
            }
            if (_bytes.Count < Dimensions.PayloadIndex)
            {
                throw new FrameException(Frame.FrameError.NO_HEADER);
            }
            
            //confirm checksum
            if (Dimensions.Checksum > 0)
            {
                switch (Schema)
                {
                    case Frame.FrameSchema.SMALL_SIMPLE_CHECKSUM:
                        byte csum = 0;
                        csum = CheckSum.SimpleAddition(_bytes.GetRange(0, Dimensions.ChecksumIndex).ToArray());
                        if (_bytes[Dimensions.ChecksumIndex] != csum)
                        {
                            String msg = String.Format("Supplied checksum {0} != {1} calculated checksum");
                            throw new FrameException(Frame.FrameError.CHECKSUM_FAILED, msg);
                        }
                        break;
                }
            }
        }

        public List<byte> GetBytes(bool addChecksum = true)
        {
            if (Dimensions.Payload <= 0) throw new FrameException(FrameError.NO_PAYLOAD);

            if (addChecksum && Dimensions.Checksum > 0)
            {
                switch (Schema)
                {
                    case Frame.FrameSchema.SMALL_SIMPLE_CHECKSUM:
                        byte csum = CheckSum.SimpleAddition(_bytes.GetRange(0, Dimensions.ChecksumIndex).ToArray());
                        setByteAt(csum, Dimensions.ChecksumIndex);
                        break;
                }
            }

            return _bytes;
        }


        public byte GetByte(int index)
        {
            return _bytes[index];
        }

        public int GetInt(int index, int count)
        {
            byte[] bytes = _bytes.GetRange(index, count).ToArray();
            return Chetch.Utilities.Convert.ToInt(bytes);
        }
    }
}
