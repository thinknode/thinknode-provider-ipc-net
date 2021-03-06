﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using MsgPack;
using MsgPack.Serialization;

/// <summary>
/// The Thinknode namespace includes a Provider class which may be inherited to create
/// a Thinknode app.
/// </summary>
namespace Thinknode
{
    /// <summary>
    /// Built-in datetime serializer.
    /// </summary>
    public class DateTimeSerializer : MessagePackSerializer<DateTime>
    {
        public DateTimeSerializer(SerializationContext ownerContext) : base(ownerContext) {}

        /// <summary>
        /// Defines the serialization for the Datetime.
        /// </summary>
        /// <param name="packer">Packer.</param>
        /// <param name="objectTree">Object tree.</param>
        protected override void PackToCore(Packer packer, DateTime objectTree)
        {
            DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            double ticks = (TimeZoneInfo.ConvertTimeToUtc(objectTree) - epoch).TotalMilliseconds;
            byte[] bytes;
            try
            {
                sbyte val = Convert.ToSByte(ticks);
                bytes = BitConverter.GetBytes(val);
            }
            catch (OverflowException)
            {
                try
                {
                    short val = Convert.ToInt16(ticks);
                    bytes = BitConverter.GetBytes(val);
                }
                catch (OverflowException)
                {
                    try
                    {
                        int val = Convert.ToInt32(ticks);
                        bytes = BitConverter.GetBytes(val);
                    }
                    catch (OverflowException)
                    {
                        long val = Convert.ToInt64(ticks);
                        bytes = BitConverter.GetBytes(val);
                    }
                }
            }
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }
            packer.PackExtendedTypeValue(1, bytes);
        }

        /// <summary>
        /// Defines the deserialization for the Datetime.
        /// </summary>
        /// <returns>The from core.</returns>
        /// <param name="unpacker">Unpacker.</param>
        protected override DateTime UnpackFromCore(Unpacker unpacker)
        {
            DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            MessagePackObject obj = unpacker.LastReadData;
            MessagePackExtendedTypeObject ext = obj.AsMessagePackExtendedTypeObject();
            byte[] bytes = ext.GetBody();
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }
            double val;
            if (bytes.Length == 1)
            {
                val = Convert.ToDouble(bytes[0]);
            }
            else if (bytes.Length == 2)
            {
                val = Convert.ToDouble(BitConverter.ToInt16(bytes, 0));
            }
            else if (bytes.Length == 4)
            {
                val = Convert.ToDouble(BitConverter.ToInt32(bytes, 0));
            }
            else
            {
                val = Convert.ToDouble(BitConverter.ToInt64(bytes, 0));
            }
            return epoch.AddMilliseconds(val);
        }
    }

    public delegate void ProgressDelegate(float prog, string message);
    public delegate void FailureDelegate(string code, string message);

    /// <summary>
    /// An abstract class which may be inherited to create a Thinknode app. The provider handles
    /// IPC between itself and the calculation supervisor. A provider will create a socket connection
    /// with the supervisor, register itself as a provider, and begin listening for messages.
    /// </summary>
    abstract public class Provider
    {
        public SerializationContext context;

        private Client client;

        /// <summary>
        /// Initializes a new instance of the <see cref="Thinknode.Provider"/> class. Requires that
        /// the environment variables THINKNODE_HOST, THINKNODE_PORT, and THINKNODE_PID be set.
        /// </summary>
        public Provider() {
            String host = Environment.GetEnvironmentVariable("THINKNODE_HOST");
            int port = Int32.Parse(Environment.GetEnvironmentVariable("THINKNODE_PORT"));
            String pid = Environment.GetEnvironmentVariable("THINKNODE_PID");

            // Construct client.
            client = new Client(this, host, port, pid);

            // Initialize context.
            context = new SerializationContext();
            context.SerializationMethod = SerializationMethod.Map;

            // Add DateTime Serializer
            context.Serializers.RegisterOverride(new DateTimeSerializer(context));
        }

        /// <summary>
        /// Starts the provider by first creating a socket connection, then registering itself as
        /// a provider, and finally, listening for new messages.
        /// </summary>
        public void Start()
        {
            client.Connect();
            client.Register();
            client.Loop();
        }

        /// <summary>
        /// A socket client for interacting with the calculation supervisor.
        /// </summary>
        private class Client
        {
            private Provider provider;

            private static CancellationTokenSource cancelSource;
            private static ManualResetEvent connectDone = new ManualResetEvent(false);
            private static ManualResetEvent sendDone = new ManualResetEvent(false);

            private String host;
            private int port;
            private String pid;

            private Socket sock;

            private enum Action { Register, Function, Progress, Result, Failure, Ping, Pong };

            /// <summary>
            /// Initializes a new instance of the <see cref="Thinknode.Client"/> class by creating
            /// a new client socket.
            /// </summary>
            /// <param name="provider">The provider that owns this client.</param>
            /// <param name="host">The host address of the Thinknode calculation supervisor.</param>
            /// <param name="port">The port number of the Thinknode calculation supervisor.</param>
            /// <param name="pid">The "process id" for the calculation.</param>
            public Client(Provider provider, String host, int port, String pid)
            {
                this.provider = provider;
                this.host = host;
                this.port = port;
                this.pid = pid;

                // Create client socket.
                sock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            }

            /// <summary>
            /// Initiates a connection with the calculation supervisor.
            /// </summary>
            public void Connect()
            {
                // Begin and wait for connection.
                Console.Write("Connecting...");
                sock.BeginConnect(host, port, new AsyncCallback(ConnectCallback), sock);
                connectDone.WaitOne();
                Console.WriteLine("done");
            }

            /// <summary>
            /// Starts the receive message loop.
            /// </summary>
            public void Loop()
            {
                Console.WriteLine("Receiving messages...");
                while (true)
                {
                    HandleMessage();
                }
            }

            /// <summary>
            /// Registers this calculation provider instance with the calculation supervisor.
            /// </summary>
            public void Register()
            {
                Console.Write("Registering...");
                byte[] header = ConstructHeader(1, Action.Register, 34);
                byte[] protocol = { 0, 0 };
                byte[] pid = Encoding.UTF8.GetBytes(this.pid);
                byte[] message = header.Concat(protocol).Concat(pid).ToArray();
                Send(message);
                Console.WriteLine("done");
            }

            /// <summary>
            /// The callback called once the connection has been made.
            /// </summary>
            /// <param name="res">The asynchronous result.</param>
            private static void ConnectCallback(IAsyncResult res)
            {
                // Retrieve the socket from the state object.
                Socket sock = (Socket) res.AsyncState;

                // Complete the connection.
                sock.EndConnect(res);

                // Signal that the connection has been made.
                connectDone.Set();
            }

            /// <summary>
            /// Gets a message header.
            /// </summary>
            /// <param name="version">The IPC protocol version.</param>
            /// <param name="action">The message type (or action).</param>
            /// <param name="length">The length of the message to follow.</param>
            private static byte[] ConstructHeader(int version, Action action, uint length)
            {
                byte[] header = new byte[8];
                // Set version field.
                if (version == 1) {
                    Buffer.SetByte(header, 0, 0);
                } else {
                    throw new System.ArgumentException("Only version 1 is supported", "version");
                }

                // Set reserved byte.
                Buffer.SetByte(header, 1, 0);

                // Set action field.
                byte code = EncodeAction(action);
                Buffer.SetByte(header, 2, code);

                // Set reserved byte.
                Buffer.SetByte(header, 3, 0);

                // Set length.
                byte[] lengthBytes = BitConverter.GetBytes(length);
                if (BitConverter.IsLittleEndian)
                {
                    Array.Reverse(lengthBytes);
                }
                Buffer.BlockCopy(lengthBytes, 0, header, 4, 4);

                return header;
            }

            /// <summary>
            /// Encodes the action to a byte.
            /// </summary>
            /// <returns>The action encoded as a byte.</returns>
            /// <param name="action">The Action enum.</param>
            private static byte EncodeAction(Action action)
            {
                switch (action)
                {
                    case Action.Register:
                        return 0;
                    case Action.Function:
                        return 1;
                    case Action.Progress:
                        return 2;
                    case Action.Result:
                        return 3;
                    case Action.Failure:
                        return 4;
                    case Action.Ping:
                        return 5;
                    case Action.Pong:
                        return 6;
                    default:
                        throw new ArgumentException("Cannot encode.", "action");
                }
            }

            /// <summary>
            /// Decodes the action to an Action enum.
            /// </summary>
            /// <returns>The action decode to an Action enum.</returns>
            /// <param name="action">The action code.</param>
            private static Action DecodeAction(byte action)
            {
                switch (action)
                {
                    case 0:
                        return Action.Register;
                    case 1:
                        return Action.Function;
                    case 2:
                        return Action.Progress;
                    case 3:
                        return Action.Result;
                    case 4:
                        return Action.Failure;
                    case 5:
                        return Action.Ping;
                    case 6:
                        return Action.Pong;
                    default:
                        throw new ArgumentException("Cannot decode.", "action");
                }
            }

            /// <summary>
            /// The callback called once the data has been sent.
            /// </summary>
            /// <param name="res">The asynchronous result.</param>
            private static void SendCallback(IAsyncResult res)
            {
                // Retrieve the socket from the state object.
                AysnchronousMessage msg = (AysnchronousMessage) res.AsyncState;

                // Complete the send operation.
                int bytesSent = msg.sock.EndSend(res);
                // Send any remaing data
                int offset = bytesSent;
                int length = msg.data.Length;
                int remaining = length - offset;
                while (remaining > 0)
                {
                    int additional = msg.sock.Send(msg.data, offset, remaining, 0);
                    offset += additional;
                    remaining -= additional;
                }

                // Signal that the message has been sent.
                sendDone.Set();
            }

            /// <summary>
            /// Converts the next 4 bytes of the byte array at the given index to a uint32.
            /// </summary>
            /// <returns>The converted uint32.</returns>
            /// <param name="buf">The byte array containing the uint32.</param>
            /// <param name="index">The index at which to find the uint32.</param>
            private static uint ToUInt32(byte[] buf, int index)
            {
                byte[] data = new byte[4];
                Buffer.BlockCopy(buf, index, data, 0, 4);
                if (BitConverter.IsLittleEndian)
                {
                    Array.Reverse(data);
                }
                return BitConverter.ToUInt32(data, 0);
            }

            /// <summary>
            /// Converts the next 2 bytes of the byte array at the given index to a uint16.
            /// </summary>
            /// <returns>The converted uint16.</returns>
            /// <param name="buf">The byte array containing the uint32.</param>
            /// <param name="index">The index at which to find the uint32.</param>
            private static ushort ToUInt16(byte[] buf, int index)
            {
                byte[] data = new byte[2];
                Buffer.BlockCopy(buf, index, data, 0, 2);
                if (BitConverter.IsLittleEndian)
                {
                    Array.Reverse(data);
                }
                return BitConverter.ToUInt16(data, 0);
            }

            /// <summary>
            /// Handles aggregate exceptions from the provider.
            /// </summary>
            /// <param name="ex">The exception encountered by the provider.</param>
            private void HandleAggregateException(AggregateException ae)
            {
                Console.WriteLine("Encountered failure...");
                Exception ex = ((TargetInvocationException)ae.InnerException).InnerException;
                HandleException(ex);
            }

            /// <summary>
            /// Handles the exception from the provider.
            /// </summary>
            /// <param name="ex">The exception.</param>
            private void HandleException(Exception ex)
            {
                string code = ex.GetType().Name;
                string message = ex.Message;
                HandleFailure(code, message);
            }

            /// <summary>
            /// Handles failure whether reported by the provider or as uncaught exceptions.
            /// </summary>
            /// <param name="code">The code string.</param>
            /// <param name="message">The message string.</param>
            private void HandleFailure(string code, string message)
            {
                Console.WriteLine("Reporting failure...");
                // Trim code and message if necessary.
                if (code.Length > 255)
                {
                    code = code.Substring(0, 255);
                }
                if (message.Length > 65535)
                {
                    message = message.Substring(0, 65535);
                }

                // Construct byte arrays for pieces of failure request body.
                byte[] codeLength = { Convert.ToByte(code.Length) };
                byte[] messageLength = BitConverter.GetBytes(Convert.ToUInt16(message.Length));
                byte[] codeBytes = Encoding.UTF8.GetBytes(code);
                byte[] messageBytes = Encoding.UTF8.GetBytes(message);

                // Reverse bytes if we are on little endian system.
                if (BitConverter.IsLittleEndian)
                {
                    Array.Reverse(messageLength);
                }

                // Compute the total body length.
                UInt32 bodyLength = Convert.ToUInt32(1 + code.Length + 2 + message.Length);

                // Construct the header and body. Send both together.
                byte[] header = ConstructHeader(1, Action.Failure, bodyLength);
                byte[] body = codeLength.Concat(codeBytes).Concat(messageLength).Concat(messageBytes).ToArray();
                Send(header.Concat(body).ToArray());
                Console.WriteLine("Reported failure...");

                // Cancel
                cancelSource.Cancel();
            }

            /// <summary>
            /// Handles a function request.
            /// </summary>
            /// <param name="message">The message body.</param>
            private void HandleFunction(object message)
            {
                try {
                    byte[] data = (byte[])message;

                    // Receive the length of the function name.
                    int nameLength = Convert.ToInt32(Buffer.GetByte(data, 0));

                    // Receive the name of the function.
                    String name = Encoding.UTF8.GetString(data, 1, nameLength);

                    // Receive the number of arguments.
                    ushort argCount = ToUInt16(data, 1 + nameLength);

                    // Lookup the method info.
                    MethodInfo methodInfo = provider.GetType().GetMethods(BindingFlags.Static | BindingFlags.DeclaredOnly | BindingFlags.Public)
                        .Where(m => m.Name == name)
                        .Where(m =>
                            {
                                ParameterInfo[] p = m.GetParameters();
                                // If the number of parameters is less than the arg count, return false.
                                if (p.Length == argCount)
                                {
                                    return true;
                                }
                                else if (p.Length == argCount + 1)
                                {
                                    string n = p.Last().ParameterType.FullName;
                                    if (n == "Thinknode.ProgressDelegate" || n == "Thinknode.FailureDelegate")
                                    {
                                        return true;
                                    }
                                    else
                                    {
                                        return false;
                                    }
                                }
                                else if (p.Length == argCount + 2)
                                {
                                    string n1 = p[p.Length - 2].ParameterType.FullName;
                                    string n2 = p[p.Length - 1].ParameterType.FullName;
                                    if ((n1 == "Thinknode.ProgressDelegate" && n2 == "Thinknode.FailureDelegate") ||
                                        (n1 == "Thinknode.FailureDelegate" && n2 == "Thinknode.ProgressDelegate"))
                                    {
                                        return true;
                                    }
                                    else
                                    {
                                        return false;
                                    }
                                }
                                else
                                {
                                    return false;
                                }
                            })
                        .First();
                    if (methodInfo == null)
                    {
                        throw new InvalidOperationException(String.Format("Public static method {0} not found.", name));
                    }

                    // Lookup parameters.
                    ParameterInfo[] parameters = methodInfo.GetParameters();

                    // Decode arguments.
                    object[] args = new object[parameters.Length];
                    int offset = 1 + nameLength + 2;
                    ushort i;
                    for (i = 0; i < argCount; ++i)
                    {
                        uint argLength = ToUInt32(data, offset);
                        offset += 4;
                        byte[] arg = new byte[argLength];
                        Buffer.BlockCopy(data, offset, arg, 0, (int)argLength);
                        offset += (int)argLength;
                        var serializer = MessagePackSerializer.Get(parameters[i].ParameterType, provider.context);
                        args[i] = serializer.UnpackSingleObject(arg);
                    }

                    // Optionally add the progress and failure delegates.
                    string delName;
                    for (; i < parameters.Length; ++i)
                    {
                        delName = parameters[i].ParameterType.FullName;
                        if (delName == "Thinknode.ProgressDelegate")
                        {
                            ProgressDelegate progDel = HandleProgress;
                            args[i] = progDel;
                        } else {
                            FailureDelegate failDel = HandleFailure;
                            args[i] = failDel;
                        }
                    }

                    // Invoke function with arguments from function request message.
                    var returnSerializer = MessagePackSerializer.Get(methodInfo.ReturnType, provider.context);
                    byte[] result = returnSerializer.PackSingleObject(methodInfo.Invoke(null, args));

                    // Send result.
                    uint length = Convert.ToUInt32(result.LongLength);
                    byte[] header = ConstructHeader(1, Action.Result, length);
                    Send(header.Concat(result).ToArray());
                    Console.WriteLine("Completed function...");

                    // Reset source.
                    cancelSource = null;
                }
                catch (Exception ex)
                {
                    HandleException(ex);
                }
            }

            /// <summary>
            /// Handles the message from the calculation supervisor.
            /// </summary>
            private void HandleMessage()
            {
                // Receive the header data.
                byte[] header = Receive(8);
                Action action = DecodeAction(Buffer.GetByte(header, 2));
                uint length = ToUInt32(header, 4);

                // Receive the message body.
                byte[] message = Receive(length);
                switch (action)
                {
                    case Action.Function:
                        {
                            Console.WriteLine("Received function message...");
                            // Start a new thread to handle the function.
                            cancelSource = new CancellationTokenSource();
                            Task task = new Task(() => HandleFunction(message), cancelSource.Token);
                            task.ContinueWith((t) => HandleAggregateException(t.Exception), TaskContinuationOptions.OnlyOnFaulted);
                            task.Start();
                            break;
                        }
                    case Action.Ping:
                        {
                            Console.WriteLine("Received ping message...");
                            // Start a new thread to handle the pong response.
                            Task task = new Task(() => HandlePing(message));
                            task.Start();
                            break;
                        }
                    default:
                        {
                            String msg = String.Format("{0} request not supported.", action.ToString());
                            throw new InvalidOperationException(msg);
                        }
                }
            }

            /// <summary>
            /// Handles the ping message by responding with a "pong".
            /// </summary>
            /// <param name="body">The ping message body to be used in the pong response.</param>
            private void HandlePing(byte[] body)
            {
                byte[] header = ConstructHeader(1, Action.Pong, 32);
                Send(header.Concat(body).ToArray());
                Console.WriteLine("Responded to ping message...");
            }

            /// <summary>
            /// Handles calculation progress.
            /// </summary>
            /// <param name="prog">A floating point number between 0 and 1 representing the calculation's progress.</param>
            /// <param name="message">The message string.</param>
            private void HandleProgress(float prog, string message)
            {
                Console.WriteLine("Reporting progress...");
                // Trim message if necessary.
                if (message.Length > 65535)
                {
                    message = message.Substring(0, 65535);
                }

                // Construct byte arrays for pieces of failure request body.
                byte[] progressBytes = BitConverter.GetBytes(prog);
                byte[] messageLength = BitConverter.GetBytes(Convert.ToUInt16(message.Length));
                byte[] messageBytes = Encoding.UTF8.GetBytes(message);

                // Reverse bytes if we are on little endian system.
                if (BitConverter.IsLittleEndian)
                {
                    Array.Reverse(progressBytes);
                    Array.Reverse(messageLength);
                }

                // Compute the total body length.
                UInt32 bodyLength = Convert.ToUInt32(4 + 2 + message.Length);

                // Construct the header and body. Send both together.
                byte[] header = ConstructHeader(1, Action.Progress, bodyLength);
                byte[] body = progressBytes.Concat(messageLength).Concat(messageBytes).ToArray();
                Send(header.Concat(body).ToArray());
                Console.WriteLine("Reported progress...");
            }

            /// <summary>
            /// Receive the specified number of bytes.
            /// </summary>
            /// <param name="length">The number of bytes to receive.</param>
            private byte[] Receive(uint length)
            {
                // Receive 'length' number of bytes.
                uint remaining = length;
                byte[] data = new byte[0];
                do
                {
                    int read = 0, offset = 0;
                    uint num = Math.Min(remaining, 2147483647);
                    byte[] temp = new byte[num];
                    while (read != num)
                    {
                        int additional = sock.Receive(temp, offset, (int)num - read, 0);
                        read += additional;
                        offset += additional;
                    }
                    data = data.Concat(temp).ToArray();
                    remaining -= num;
                } while (remaining > 0);
                return data;
            }

            /// <summary>
            /// Sends data as bytes over the socket.
            /// </summary>
            /// <param name="data">The data to send.</param>
            private void Send(byte[] data)
            {
                if (cancelSource != null && cancelSource.IsCancellationRequested)
                {
                    return;
                }
                // Begin sending the data to the remote device.
                AysnchronousMessage msg = new AysnchronousMessage(sock, data);
                sock.BeginSend(data, 0, data.Length, 0, new AsyncCallback(SendCallback), msg);
                sendDone.WaitOne();
                sendDone.Reset();
            }

            /// <summary>
            /// An aysnchronous message.
            /// </summary>
            private class AysnchronousMessage
            {
                public Socket sock;
                public byte[] data;

                /// <summary>
                /// Initializes a new instance of the <see cref="Thinknode.Provider+Client+AysnchronousMessage"/> class.
                /// </summary>
                /// <param name="sock">A client socket.</param>
                /// <param name="data">The message content as a byte array.</param>
                public AysnchronousMessage(Socket sock, byte[] data)
                {
                    this.sock = sock;
                    this.data = data;
                }
            }
        }
    }
}

