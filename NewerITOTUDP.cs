using ModemAPI;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;

string[] config = File.ReadAllLines("config.txt");
if (config[0] != "client")
{
    Console.WriteLine("Client config parsing error: terminating");
    Thread.Sleep(5000);
    throw new FormatException("config line 1 not \"client\"");
}
string websocketProviderUrl = config[1];
string destination = config[2];
string port = config[3];
string verbosity = config[4];
string bigpings = config[5];
string virtualOrCiocil = config[6];
string wiretype = config[7];
bool verbose = verbosity == "verbose";
// one shared instance instead of allocating a new Address for every packet
Address destinationAddress = new(destination);

IModem modem;
if (virtualOrCiocil == "virtual")
{
    VirtualModem modem_ = new(websocketProviderUrl, new(), rawWs:wiretype == "websock");
    modem_.OnReconnectWebsocket += delegate ()
    {
        Console.WriteLine("Raw Websocket is reconnecting...");
    };
    modem_.OnSuccessReconnectWebsocket += delegate ()
    {
        Console.WriteLine("Raw Websocket has reconnected successfully!");
    };
    modem = modem_;
}
else if (virtualOrCiocil == "ciocil")
{
    L1Types wt = L1Types.L1_SERIAL;
    if (wiretype == "tcp")
    {
        wt = L1Types.L1_INET_TCP;
    }
    else if (wiretype == "serport")
    {
        wt = L1Types.L1_SERIAL;
    }
    LowLatencyPhysicalModem modem_ = new(websocketProviderUrl.Split(' ')[0], int.Parse(websocketProviderUrl.Split(' ')[1]), wt, true);
    modem_.AttachCRCMismatchEvent(delegate ()
    {
        Console.WriteLine("CRC-32 mismatched on the Level Two of the tetronet stack!");
    });
    modem_.InternalErrorHappened += delegate (Exception error, ErrorEmitter errorEmitter)
    {
        Console.WriteLine($"Physical error sent by {errorEmitter}: {error}");
    };
    modem = modem_;
}
else
{
    throw new FormatException("you must use virtual for socket.io connection or ciocil for low latency serial connections");
}
modem.Dial();
Console.Write("Connecting");
while (!modem.IsModemConnected)
{
    Console.Write(".");
    Thread.Sleep(10);
}
Console.WriteLine("OK");
Console.WriteLine("Connected to the Tetronet with address " + (modem.LocalModemAddress ?? throw new NullAddressException()).AddressValue);

// All transmissions go through a single sender task: VirtualModem's raw websocket
// does not allow concurrent SendAsync calls (it reconnects on that error), and
// LowLevelTransmit blocks the caller until the frame is sent.
TetronetSender sender = new(modem, verbose);

// create the udp server
UdpClient udpServer = new(int.Parse(port));
// Making anormous buffer size for this UDP client.
udpServer.Client.ReceiveBufferSize = 16 * 1024 * 1024; // 16 Mbytes
udpServer.Client.SendBufferSize = 16 * 1024 * 1024;
if (OperatingSystem.IsWindows())
{
    // Without this an ICMP "port unreachable" from any one client makes ReceiveAsync throw
    // ConnectionReset on the shared server socket, stalling every other client.
    const int SIO_UDP_CONNRESET = -1744830452;
    udpServer.Client.IOControl(SIO_UDP_CONNRESET, [0, 0, 0, 0], null);
}
Console.WriteLine($"UDP server listening on port {port}");

// store active UDP clients by remote endpoint and by connectionId
ConcurrentDictionary<IPEndPoint, UdpClientInfo> activeConnections = new();
ConcurrentDictionary<uint, UdpClientInfo> connectionsById = new();

ConcurrentDictionary<uint, long> pendingPings = []; // ping connection ID -> timestamp

void RemoveConnection(UdpClientInfo clientInfo)
{
    activeConnections.TryRemove(clientInfo.RemoteEndPoint, out _);
    connectionsById.TryRemove(clientInfo.ConnectionId, out _);
}

// handle single ping
if (bigpings == "singleping")
{
    Console.WriteLine($"Ping sent with Connection ID of 500000000");
    sender.SendControl([0x00], destinationAddress, "ping", 500000000);
}

// Ping task
_ = Task.Run(async delegate ()
{
    uint currentPingConid = 0;
    while (true)
    {
        try
        {
            // Transmit different data in a ping packet to prevent network from caching (which btw violates the tetronet spec) the request
            if (bigpings == "bigping")
            {
                byte[] pingData = new byte[Random.Shared.Next(25000)];
                Random.Shared.NextBytes(pingData);
                Console.WriteLine($"Ping sent with Connection ID of {currentPingConid}");
                pendingPings[currentPingConid] = DateTime.Now.Ticks;
                sender.SendControl(pingData, destinationAddress, "ping", currentPingConid);
            }
            else if (bigpings == "ping")
            {
                Console.WriteLine($"Ping sent with Connection ID of {currentPingConid}");
                pendingPings[currentPingConid] = DateTime.Now.Ticks;
                sender.SendControl([0x00], destinationAddress, "ping", currentPingConid);
            }
            else
            {
                return;
            }
            currentPingConid++;
        }
        catch
        {
            Console.WriteLine("Failed to transmit ping: refused");
        }
        await Task.Delay(Random.Shared.Next(100, 10000));
    }
});

// incoming from tetronet data processor
// (runs on the modem's reader thread, so it must not block)
modem.AttachReceiveEventNoUnfragment(delegate (Packet data, Action k)
{
    switch (data.QueryType)
    {
        case "udp":
            if (data.Transmitter.AddressValue != destination) return;
            if (connectionsById.TryGetValue(data.ConnectionID, out UdpClientInfo? clientInfo))
            {
                // send dgram to the correct client
                byte[] dataBytes = data.DataBytes;
                try
                {
                    udpServer.Client.SendTo(dataBytes, SocketFlags.None, clientInfo.RemoteEndPoint);
                    if (verbose) Console.WriteLine($"Sent {dataBytes.Length} bytes to UDP client {clientInfo.RemoteEndPoint} for connection {data.ConnectionID}");
                }
                catch (Exception ex)
                {
                    if (verbose) Console.WriteLine($"Error sending UDP to client: {ex.Message}");
                    RemoveConnection(clientInfo);
                }
            }
            else
            {
                Console.WriteLine($"Client wasn't found: {data.ConnectionID}");
            }
            break;
        case "udp_heartbeat":
            if (verbose && data.Transmitter.AddressValue == destination && connectionsById.TryGetValue(data.ConnectionID, out UdpClientInfo? hbInfo))
            {
                Console.WriteLine($"Heartbeat for connection {data.ConnectionID} - {hbInfo.RemoteEndPoint}");
            }
            break;
        case "udp_reset":
            if (data.Transmitter.AddressValue != destination) return;
            if (connectionsById.TryGetValue(data.ConnectionID, out UdpClientInfo? resetInfo))
            {
                Console.WriteLine($"Received UDP_RESET for connection {data.ConnectionID}");
                RemoveConnection(resetInfo);
            }
            else
            {
                Console.WriteLine($"Failed to reset for connection {data.ConnectionID}");
            }
            break;
        case "pong":
            if (pendingPings.TryRemove(data.ConnectionID, out long pingTimestamp))
            {
                Console.WriteLine($"Pong received with Connection ID of {data.ConnectionID} in {(DateTime.Now.Ticks - pingTimestamp) / 10000m}ms");
            }
            break;
    }
});

// start UDP data processor
_ = Task.Run(async () =>
{
    while (true)
    {
        UdpReceiveResult result;
        try
        {
            // read dgram
            result = await udpServer.ReceiveAsync();
        }
        catch (SocketException ex)
        {
            if (verbose) Console.WriteLine($"UDP receive error: {ex.Message}");
            continue;
        }
        catch (Exception ex)
        {
            if (verbose) Console.WriteLine($"UDP receive error: {ex.Message}");
            await Task.Delay(100);
            continue;
        }
        byte[] receivedData = result.Buffer;
        IPEndPoint remoteEndPoint = result.RemoteEndPoint;

        // check if there's already a connection for that guy
        if (!activeConnections.TryGetValue(remoteEndPoint, out UdpClientInfo? clientInfo))
        {
            uint connectionId;
            do
            {
                connectionId = (uint)Random.Shared.NextInt64(uint.MaxValue);
                clientInfo = new UdpClientInfo(connectionId, remoteEndPoint);
            }
            while (!connectionsById.TryAdd(connectionId, clientInfo));
            activeConnections[remoteEndPoint] = clientInfo;

            Console.WriteLine($"NEW UDP CLIENT! Assigned ID: {connectionId} from {remoteEndPoint}");

            byte[] connectData = System.Text.Encoding.UTF8.GetBytes($"UDP connection from {remoteEndPoint}");
            sender.SendControl(connectData, destinationAddress, "udp_connect", connectionId);
            Console.WriteLine($"UDP connect sent for connection {connectionId}");
        }

        // forward to the tnet
        if (verbose) Console.WriteLine($"Received UDP data from {remoteEndPoint}: {receivedData.Length} bytes");
        sender.SendData(receivedData, destinationAddress, "udp", clientInfo.ConnectionId);
    }
});

// background monitoring for the active connections
if (!verbose)
{
    await Task.Delay(Timeout.Infinite);
}
while (true)
{
    await Task.Delay(1000);
    Console.WriteLine($"Active UDP clients: {activeConnections.Count}, queued to Tetronet: {sender.QueuedData}, dropped: {sender.DroppedData}");

    foreach (var client in activeConnections.Values)
    {
        Console.WriteLine($"  - Connection {client.ConnectionId}: {client.RemoteEndPoint}");
    }
}

sealed class UdpClientInfo(uint connectionId, IPEndPoint remoteEndPoint)
{
    public uint ConnectionId { get; } = connectionId;
    public IPEndPoint RemoteEndPoint { get; } = remoteEndPoint;
}

/// <summary>
/// Serializes every transmission into the modem from one background task.
/// Control packets (connect/reset/ping) are never dropped and go out first;
/// UDP data goes through a bounded queue and the oldest datagrams are dropped
/// when the Tetronet link can't keep up, so latency doesn't grow without limit.
/// </summary>
sealed class TetronetSender
{
    private readonly record struct Outgoing(byte[] Data, Address Address, string QueryType, uint ConnectionId);

    private readonly IModem modem;
    private readonly bool verbose;
    private readonly Channel<Outgoing> control = Channel.CreateUnbounded<Outgoing>(new() { SingleReader = true });
    private readonly Channel<Outgoing> data;
    // One permit per queued item. Waiting on both channels' WaitToReadAsync instead would
    // cancel the previous waiter of the single-reader channel on every packet
    // (OperationCanceledException each time).
    private readonly SemaphoreSlim itemsAvailable = new(0);
    private long droppedData;

    public long DroppedData => Interlocked.Read(ref droppedData);
    public int QueuedData => data.Reader.Count;

    public TetronetSender(IModem modem, bool verbose, int dataCapacity = 4096)
    {
        this.modem = modem;
        this.verbose = verbose;
        data = Channel.CreateBounded<Outgoing>(new BoundedChannelOptions(dataCapacity)
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.DropOldest
        }, delegate (Outgoing _) { Interlocked.Increment(ref droppedData); });
        Task.Factory.StartNew(Run, TaskCreationOptions.LongRunning);
    }

    public void SendControl(byte[] bytes, Address address, string queryType, uint connectionId)
    {
        if (control.Writer.TryWrite(new(bytes, address, queryType, connectionId))) itemsAvailable.Release();
    }

    public void SendData(byte[] bytes, Address address, string queryType, uint connectionId)
    {
        // with DropOldest a dropped item leaves a spare permit; Run just finds nothing and waits again
        if (data.Writer.TryWrite(new(bytes, address, queryType, connectionId))) itemsAvailable.Release();
    }

    private void Run()
    {
        while (true)
        {
            itemsAvailable.Wait();
            if (!control.Reader.TryRead(out Outgoing item) && !data.Reader.TryRead(out item))
            {
                continue;
            }
            try
            {
                modem.LowLevelTransmit(item.Data, item.Address, item.QueryType, item.ConnectionId);
                if (verbose && item.QueryType == "udp") Console.WriteLine($"Transmitted {item.Data.Length} bytes to Tetronet (conn {item.ConnectionId})");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Transmit error ({item.QueryType}, conn {item.ConnectionId}): {ex.Message}");
            }
        }
    }
}
