﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Xml.Serialization;
using System.Xml;
using System.IO;
using System.Linq;
using Chetch.Utilities;

namespace Chetch.Messaging
{
    public enum MessageType
    {
        NOT_SET,
        REGISTER_LISTENER,
        CUSTOM,
        INFO,
        WARNING,
        ERROR,
        PING,
        PING_RESPONSE,
        STATUS_REQUEST,
        STATUS_RESPONSE,
        COMMAND,
        ERROR_TEST,
        ECHO,
        ECHO_RESPONSE,
        CONFIGURE,
        CONFIGURE_RESPONSE,
        RESET,
        INITIALISE,
        DATA,
        CONNECTION_REQUEST,
        CONNECTION_REQUEST_RESPONSE,
        SHUTDOWN,
        SUBSCRIBE,
        UNSUBSCRIBE,
        COMMAND_RESPONSE,
        TRACE,
        NOTIFICATION,
        SUBSCRIBE_RESPONSE,
        INITIALISE_RESPONSE,
        ALERT,
        FINALISE,
    }

    public enum MessageEncoding
    {
        SYSTEM_DEFINED = 1, //the particulars are decided by the system being implemented (e.g. a single byte command)
        XML,
        QUERY_STRING,
        POSITONAL,
        BYTES_ARRAY,
        JSON
    }

    public class Utf8StringWriter : StringWriter
    {
        public override Encoding Encoding => Encoding.UTF8;
    }

    [Serializable]
    public class MessageValue
    {
        public String Key;
        public Object Value;
    }

    [Serializable]
    public class Message
    {
        public const String JSON_DATETIME_FORMAT = "yyyy'-'MM'-'dd HH':'mm':'ss zzzz";

        public String ID;
        public String Target; //to help routing to the correct place at the receive end
        public String ResponseID; //normally the ID of the message that was sent requesting a response (e.g. Ping and Ping Response)
        public String Sender; //who sent the message
        public MessageType Type;
        public int SubType;
        public String Signature; //a way to test whether this message is valid or not

        public List<MessageValue> Values = new List<MessageValue>();
        public String Value
        {
            get
            {
                return Values.Count > 0 && HasValue("Value") ? GetString("Value") : null;
            }
            set
            {
                AddValue("Value", value);
            }
        }
        public MessageEncoding DefaultEncoding { get; set; } = MessageEncoding.JSON;


        public Message()
        {
            ID = CreateID();
            Type = MessageType.NOT_SET;
        }

        public Message(MessageType type = MessageType.NOT_SET)
        {
            ID = CreateID();
            Type = type;
        }

        public Message(MessageType type, String target)
        {
            ID = CreateID();
            Type = type;
            Target = target;
        }

        public Message(String message, int subType = 0, MessageType type = MessageType.NOT_SET)
        {
            ID = CreateID();
            Value = message;
            SubType = subType;
            Type = type;
        }

        public Message(String message, MessageType type = MessageType.NOT_SET) : this(message, 0, type){}

        public Message(Message message)
        {
            Target = message.Target;
            ResponseID = message.ResponseID;
            Type = message.Type;
            SubType = message.SubType;
            Sender = message.Sender;
            Value = message.Value;

            foreach(var mv in message.Values)
            {
                AddValue(mv.Key, mv.Value);
            }
        }

        private String CreateID()
        {
            return System.Diagnostics.Process.GetCurrentProcess().Id.ToString() + "-" + this.GetHashCode() + "-" + DateTime.Now.ToString("yyyyMMddHHmmssffff");
        }

        public void AddValue(String key, Object value)
        {
            var key2cmp = key.ToLower();
            foreach (var v in Values)
            {
                if (v.Key.ToLower() == key2cmp)
                {
                    v.Value = value;
                    return;
                }
            }

            //if here then there is no existing value
            var mv = new MessageValue();
            mv.Key = key;
            mv.Value = value;
            Values.Add(mv);
        }

        public void AddValues(Dictionary<String, Object> vals)
        {
            foreach (var entry in vals)
            {
                AddValue(entry.Key, entry.Value);
            }
        }

        public bool HasValue()
        {
            return HasValue("Value");
        }

        public bool HasValue(String key)
        {
            if (key == null || key.Length == 0)
            {
                throw new ArgumentNullException();
            }

            var key2cmp = key.ToLower();
            foreach (var v in Values)
            {
                if (v.Key.ToLower() == key2cmp)
                {
                    return true;
                }
            }
            return false;
        }

        public bool HasValues(params String[] keys)
        {
            foreach (var key in keys)
            {
                if (!HasValue(key)) return false;
            }
            return true;
        }

        public Object GetValue(String key)
        {
            if (key == null || key.Length == 0) return null;

            var key2cmp = key.ToLower();
            foreach (var v in Values)
            {
                if (v.Key.ToLower() == key2cmp)
                {
                    return v.Value;
                }
            }
            throw new Exception("No value found for key " + key);
        }

        public String GetString(String key)
        {
            return (String)GetValue(key);
        }

        public int GetInt(String key)
        {
            return System.Convert.ToInt32(GetValue(key));
        }

        public long GetLong(String key)
        {
            return System.Convert.ToInt64(GetValue(key));
        }

        public double GetDouble(String key)
        {
            return System.Convert.ToDouble(GetValue(key));
        }

        public byte GetByte(String key)
        {
            return (byte)GetInt(key);
        }

        public bool GetBool(String key)
        {
            var v = GetValue(key);
            if (v is bool) return (bool)v;
            if(v is String) return System.Convert.ToBoolean(GetString(key));
            return GetInt(key) != 0;
        }

        public T GetEnum<T>(String key) where T : struct
        {
            var v = GetValue(key);
            return (T)Enum.Parse(typeof(T), v.ToString());
        }

        public DateTime GetDateTime(String key)
        {
            String dts = GetString(key);
            if (dts == null || dts == String.Empty)
            {
                return default(DateTime);
            }
            else
            {
                return DateTime.Parse(dts, System.Globalization.CultureInfo.InvariantCulture);
            }
            
        }

        public List<T> GetList<T>(String key)
        {
            Object v = GetValue(key);
            if (v is System.Collections.ArrayList)
            {
                var al = (System.Collections.ArrayList)v;
                return al.Cast<T>().ToList();
            }

            throw new Exception("Cannot convert to List as value is of type " + v.GetType().ToString());
        }

        //Due to JSON serialization we only support dictionaries that have string keys
        public Dictionary<String, T> GetDictionary<T>(String key)
        {
            Dictionary<String, T> dictionary = new Dictionary<String, T>();
            var temp = (Dictionary<String, Object>)GetValue(key);
            Type ttype = typeof(T);
            foreach (var kv in temp)
            {
                if (ttype.IsEnum)
                {
                    dictionary[kv.Key] = (T)Enum.Parse(typeof(T), kv.Value.ToString());
                }
                else if(ttype == typeof(String))
                {
                    String s = kv.Value.ToString();
                    dictionary[kv.Key] = (T)(Object)s;
                }
                {
                    dictionary[kv.Key] = (T)kv.Value;
                }
            }
            return dictionary;
        }

        public Dictionary<String, Object> GetDictionary(String key)
        {
            return (Dictionary<String, Object>)GetValue(key);
        }

        public void Clear()
        {
            Values.Clear();
        }


        virtual public String GetQueryString(Dictionary<String, Object> vals)
        {
            vals.Add("ID", ID);
            vals.Add("ResponseID", ResponseID);
            vals.Add("Target", Target);
            vals.Add("Sender", Sender);
            vals.Add("Type", Type);
            vals.Add("SubType", SubType);
            vals.Add("Signature", Signature);
            foreach (var mv in Values)
            {
                vals.Add(mv.Key, mv.Value);
            }
            return Utilities.Convert.ToQueryString(vals);
        }

        virtual public void AddBytes(List<byte> bytes)
        {
            bytes.Add((byte)Type);
        }

        virtual public String GetXML()
        {
            XmlWriterSettings settings = new XmlWriterSettings();
            settings.Indent = false;
            settings.NewLineHandling = NewLineHandling.None;

            String xmlStr;
            using (StringWriter stringWriter = new Utf8StringWriter())
            {
                using (XmlWriter xmlWriter = XmlWriter.Create(stringWriter, settings))
                {
                    XmlSerializer serializer = new XmlSerializer(this.GetType());
                    serializer.Serialize(xmlWriter, this); //, namespaces);
                    xmlStr = stringWriter.ToString();
                    xmlWriter.Close();
                }

                stringWriter.Close();
            }
            return xmlStr;
        }

        virtual public String GetJSON(Dictionary<String, Object> vals)
        {
            vals.Add("ID", ID);
            vals.Add("ResponseID", ResponseID);
            vals.Add("Target", Target);
            vals.Add("Sender", Sender);
            vals.Add("Type", Type);
            vals.Add("SubType", SubType);
            vals.Add("Signature", Signature);
            foreach (var mv in Values)
            {
                if (mv.Value is DateTime)
                {
                    vals.Add(mv.Key, ((DateTime)mv.Value).ToString(JSON_DATETIME_FORMAT));
                }
                else
                {
                    vals.Add(mv.Key, mv.Value);
                }
            }

            var jsonSerializer = new System.Web.Script.Serialization.JavaScriptSerializer();
            return jsonSerializer.Serialize(vals);
        }

        public void Serialize(StreamWriter stream)
        {
            var xmlStr = GetXML();
            stream.WriteLine(xmlStr);
        }

        public String Serialize(MessageEncoding encoding = MessageEncoding.JSON)
        {
            String serialized = null;
            switch (encoding)
            {
                case MessageEncoding.XML:
                    serialized = GetXML();
                    break;

                case MessageEncoding.JSON:
                    serialized = GetJSON(new Dictionary<String, Object>());
                    break;

                case MessageEncoding.QUERY_STRING:
                    serialized = GetQueryString(new Dictionary<String, Object>());
                    break;

                case MessageEncoding.BYTES_ARRAY:
                    var bytes = new List<byte>();
                    AddBytes(bytes);
                    serialized = Utilities.Convert.ToString(bytes.ToArray());
                    break;

                default:
                    throw new Exception("Unable to serialize encoding " + encoding);
            }

            return serialized;

        }

        virtual public String Serialize()
        {
            return Serialize(DefaultEncoding);
        }

        public static List<String> Split(String serialized, MessageEncoding encoding)
        {
            List<String> split = new List<String>();
            switch (encoding)
            {
                case MessageEncoding.JSON:
                    if (serialized.IndexOf("}{") >= 0) {
                        serialized = "}" + serialized + "{";
                        var parts = serialized.Split(new String[] { "}{" }, StringSplitOptions.RemoveEmptyEntries);
                        foreach(var part in parts)
                        {
                            split.Add("{" + part + "}");
                        }
                    } else
                    {
                        split.Add(serialized);
                    }
                    break;

                default:
                    throw new NotImplementedException();
            }
            return split;
        }

        public static T Deserialize<T>(String s, MessageEncoding encoding = MessageEncoding.XML) where T : Message, new()
        {
            T t;
            switch (encoding)
            {
                case MessageEncoding.XML:
                    byte[] byteArray = Encoding.UTF8.GetBytes(s);
                    var stream = new MemoryStream(byteArray);
                    var writer = new StreamWriter(stream);
                    writer.Write(s);
                    writer.Flush();
                    stream.Position = 0;

                    var serializer = new XmlSerializer(typeof(T));
                    t = (T)serializer.Deserialize(stream);
                    break;

                case MessageEncoding.JSON:
                    t = new T();
                    break;

                case MessageEncoding.QUERY_STRING:
                    t = new T();
                    break;

                case MessageEncoding.BYTES_ARRAY:
                    t = new T();
                    break;

                default:
                    throw new Exception("Unrecongnised encoding " + encoding);
            }

            if (t != null)
            {
                t.OnDeserialize(s, encoding);
            }
            return t;
        }

        //if no type required for deserializing then no need to default to XML as no class type data is parsed for JSON
        public static Message Deserialize(String s, MessageEncoding encoding = MessageEncoding.JSON)
        {
            return Deserialize<Message>(s, encoding);
        }

        virtual public void OnDeserialize(String s, MessageEncoding encoding)
        {
            Dictionary<String, Object> vals;
            switch (encoding)
            {
                case MessageEncoding.XML:
                    break;

                case MessageEncoding.JSON:
                    var jsonSerializer = new System.Web.Script.Serialization.JavaScriptSerializer();
                    vals = jsonSerializer.Deserialize<Dictionary<String, Object>>(s);
                    AssignValue<String>(ref ID, "ID", vals);
                    AssignValue<String>(ref ResponseID, "ResponseID", vals);
                    AssignValue<String>(ref Target, "Target", vals);
                    AssignValue<String>(ref Sender, "Sender", vals);
                    AssignValue<MessageType>(ref Type, "Type", vals);
                    AssignValue<int>(ref SubType, "SubType", vals);
                    AssignValue<String>(ref Signature, "Signature", vals);
                    AddValues(vals);
                    break;

                case MessageEncoding.QUERY_STRING:
                    vals = Utilities.Convert.ParseQueryString(s);
                    AssignValue<String>(ref ID, "ID", vals);
                    AssignValue<String>(ref ResponseID, "ResponseID", vals);
                    AssignValue<String>(ref Target, "Target", vals);
                    AssignValue<String>(ref Sender, "Sender", vals);
                    AssignValue<MessageType>(ref Type, "Type", vals);
                    AssignValue<int>(ref SubType, "SubType", vals);
                    AssignValue<String>(ref Signature, "Signature", vals);
                    AddValues(vals);
                    break;

                case MessageEncoding.BYTES_ARRAY:
                    break;

                default:
                    throw new Exception("Unrecongnised encoding " + encoding);
            }
        }

        public static void AssignValue<T>(ref T p, String key, Dictionary<String, Object> vals)
        {
            if (vals.ContainsKey(key))
            {
                if (p is MessageType)
                {
                    p = (T)(Object)Int32.Parse(vals[key].ToString());
                    vals.Remove(key);
                }
                else
                {
                    Utilities.Convert.AssignValue<T>(ref p, key, vals, true);
                }
            }
        }

        virtual public String ToStringHeader()
        {
            String lf = Environment.NewLine;
            String s = "ID: " + ID + lf;
            s += "Target: " + Target + lf;
            s += "Response ID: " + ResponseID + lf;
            s += "Sender: " + Sender + lf;
            s += "Type: " + Type + lf;
            s += "Sub Type: " + SubType + lf;
            s += "Signature: " + Signature;
            return s;
        }


        private String _nullOrEmpty(Object o)
        {
            if(o == null)
            {
                return "[null]";
            } else if(o.ToString() == String.Empty)
            {
                return "[empty]";
            } else
            {
                return o.ToString();
            }
        }

        virtual public String ToStringValues(bool expandLists = false)
        {
            String lf = Environment.NewLine;
            String s = "Values: " + lf;
            var jsonSerializer = new System.Web.Script.Serialization.JavaScriptSerializer();
            
            foreach (var v in Values)
            {
                if (v.Value is System.Collections.IList && expandLists)
                {
                    s += v.Key + ":" + lf;
                    foreach (var itm in (System.Collections.IList)v.Value)
                    {
                        s += " - " + _nullOrEmpty(itm) + lf;
                    }
                } else if (v.Value != null && v.Value.GetType().IsGenericType && v.Value.GetType().GetGenericTypeDefinition() == typeof(Dictionary<,>))
                {
                    s += v.Key + ":" + lf;

                    //We use json serializer here to avoid type issues as we only want strings (e.g. for display)
                    var serialized = jsonSerializer.Serialize(v.Value);
                    var d = jsonSerializer.Deserialize<Dictionary<String, String>>(serialized);
                    foreach(var kv in d)
                    {
                        s += " - " + kv.Key + " = " + _nullOrEmpty(kv.Value) + lf;
                    }
                } else
                { 
                    s += v.Key + " = " + _nullOrEmpty(v.Value) + lf;
                }
            }

            return s;
        }

        override public String ToString()
        {
            String lf = Environment.NewLine;
            String s = ToStringHeader();
            s += lf + ToStringValues(false);
            return s;
        }
    }
}