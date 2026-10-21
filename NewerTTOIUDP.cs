using ModemAPI;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;

Console.WriteLine("UDP-Bridge v1.0 (Optimized for Tetronet)");

string[] config = File.ReadAllLines("config.txt");

Address? wantedAddress = null;
if (File.Exists("ci_address.txt"))
{
    wantedAddress = new(File.ReadAllText("ci_address.txt"));
}
if (config[0] != "server")
{
    Console.WriteLine("Server config parsing error: terminating");
    Thread.Sleep(5000);
    throw new FormatException("config line 1 not \"server\"");
}
string wsProvider = config[1];
// parsed once instead of on every udp_connect
IPAddress targetAddress = IPAddress.Parse(config[2]);
int targetPort = int.Parse(config[3]);
bool verbose = config[4] == "verbose";
IModem modem;
if (config[5] == "virtual")
{
    modem = new VirtualModem(wsProvider, new(wantedAddress ?? new(), true, null), rawWs:true);
}
else if (config[5] == "ciocil")
{
    LowLatencyPhysicalModem mdm = new(wsProvider.Split(' ')[0], int.Parse(wsProvider.Split(' ')[1]), L1Types.L1_SERIAL, true);
    mdm.AttachCRCMismatchEvent(delegate ()
    {
        Console.WriteLine("[WARNING]: CRC-32 Mismatched on the Level One");
    });
    mdm.L1DroppedData += delegate (L1DropBytesReasons reason, int count)
    {
        Console.WriteLine($"[WARNING]: Level One dropped {count} bytes, reason is {reason}");
    };
    modem = mdm;
}
else
{
    throw new FormatException("sixth line of the config must be \"virtual\" for tetronet over socket.io or \"ciocil\" for tetronet over serial (LL-CIoCIL-mini)");
}
Console.WriteLine("[EVENT]: Preparing to connect");
modem.Dial();
Console.Write("[PROCESS]: Connecting");
while (!modem.IsModemConnected)
{
    Console.Write(".");
    Thread.Sleep(10);
}
Console.WriteLine("OK");
Console.WriteLine("[INFO]: Connected to tetronet with address " + (modem.LocalModemAddress ?? throw new NullAddressException()).AddressValue);
File.WriteAllText("ci_address.txt", modem.LocalModemAddress.AddressValue);

// All transmissions go through a single sender task: VirtualModem's raw websocket
// does not allow concurrent SendAsync calls (it reconnects on that error), and every
// UDP reader loop below would otherwise call LowLevelTransmit in parallel.
TetronetSender sender = new(modem, verbose);

// ConnectionID -> UDP session (socket + tetronet address of the peer)
ConcurrentDictionary<uint, UdpSession> udpSessions = [];

modem.AttachReceiveEventNoUnfragment(delegate (Packet received, Action k)
{
    // runs on the modem's reader thread, so nothing here may block
    if (verbose)
    {
        Console.WriteLine($"[INFO]: Type: {received.QueryType}");
        Console.WriteLine($"        Length: {received.DataBytes.Length}");
        Console.WriteLine($"        ConnectionID: {received.ConnectionID}");
    }

    switch (received.QueryType)
    {
        case "udp":
            if (udpSessions.TryGetValue(received.ConnectionID, out UdpSession? session))
            {
                try
                {
                    session.Client.Client.Send(received.DataBytes, SocketFlags.None);
                    if (verbose) Console.WriteLine($"[EVENT]: Sent UDP datagram of {received.DataBytes.Length} bytes for connection {received.ConnectionID}");
                }
                catch (Exception ex)
                {
                    if (verbose) Console.WriteLine($"[ERROR]: Sending UDP datagram failed: {ex.Message}");
                }
            }
            else if (verbose)
            {
                Console.WriteLine($"[WARNING]: No UDP client for connection {received.ConnectionID}");
            }
            break;
        case "ping":
            // Transmit different data in a ping packet to prevent network from caching the request
            if (received.DataBytes.Length < 100)
            {
                sender.SendControl([0x00], received.Transmitter, "pong", received.ConnectionID);
            }
            else
            {
                byte[] pingData = new byte[Random.Shared.Next(30000)];
                Random.Shared.NextBytes(pingData);
                sender.SendControl(pingData, received.Transmitter, "pong", received.ConnectionID);
            }
            Console.WriteLine($"[EVENT]: Response for client's ping packet (ping connection id is {received.ConnectionID}, TTL is {received.RoutersToPass})");
            break;
        case "udp_connect":
            Console.WriteLine("[EVENT]: Received udp_connect packet for connection " + received.ConnectionID);
            if (udpSessions.ContainsKey(received.ConnectionID))
            {
                Console.WriteLine("[ERROR]: Cannot create UDP client, connection already exists: " + received.ConnectionID);
                break;
            }
            UdpClient? udpClient = null;
            try
            {
                Console.WriteLine("[INFO]: Creating UDP client: " + received.ConnectionID);
                Console.WriteLine($"[INFO]: Target IP:Port = {targetAddress}:{targetPort}");

                udpClient = new UdpClient(targetAddress.AddressFamily);
                udpClient.Connect(targetAddress, targetPort);
                UdpSession newSession = new(received.ConnectionID, udpClient, received.Transmitter);
                if (!udpSessions.TryAdd(received.ConnectionID, newSession))
                {
                    udpClient.Dispose();
                    Console.WriteLine("[ERROR]: Cannot create UDP client, connection already exists: " + received.ConnectionID);
                    break;
                }

                // start async reader
                _ = Task.Run(() => ReadFromUdpAsync(newSession));

                Console.WriteLine($"[SUCCESS]: Created the client! Now there are {udpSessions.Count} clients active");
            }
            catch (Exception e)
            {
                udpClient?.Dispose();
                Console.WriteLine($"[ERROR]: UDP client creation failed: {e}: {e.Message}");
            }
            break;
        case "udp_reset":
            if (udpSessions.TryGetValue(received.ConnectionID, out UdpSession? resetSession))
            {
                // the peer asked for it, so don't echo udp_reset back
                CleanupUdpConnection(resetSession, sendReset: false);
                Console.WriteLine("[INFO]: UDP client was closed (UDP RESET)");
            }
            else
            {
                if (verbose) Console.WriteLine($"[WARNING]: No UDP client for connection {received.ConnectionID}, telling sender to reset");
                sender.SendControl([0x00], received.Transmitter, "udp_reset", received.ConnectionID);
            }
            break;
    }
});

// async reading
async Task ReadFromUdpAsync(UdpSession session)
{
    try
    {
        while (true)
        {
            UdpReceiveResult result;
            try
            {
                // read dgram
                result = await session.Client.ReceiveAsync();
            }
            catch (SocketException ex) when (ex.SocketErrorCode == SocketError.TimedOut)
            {
                continue;
            }
            catch (ObjectDisposedException)
            {
                // closed by udp_reset or the dead connection cleanup
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR]: Reading from UDP {session.ConnectionId} failed: {ex.Message}");
                break;
            }

            // sometimes there can be a 0-length dgram
            if (result.Buffer.Length == 0) continue;

            // UDP->Tetronet forward process (Buffer is already a fresh array, no copy needed)
            sender.SendData(result.Buffer, session.PeerAddress, "udp", session.ConnectionId);
            if (verbose) Console.WriteLine($"[EVENT]: Queued UDP datagram of {result.Buffer.Length} bytes from {session.ConnectionId} to Tetronet");
        }
    }
    finally
    {
        CleanupUdpConnection(session, sendReset: true);
    }
}

void CleanupUdpConnection(UdpSession session, bool sendReset)
{
    // Reader loop, timer and udp_reset can all race here; only the first one proceeds
    if (!udpSessions.TryRemove(new KeyValuePair<uint, UdpSession>(session.ConnectionId, session))) return;

    try { session.Client.Close(); } catch { }

    Console.WriteLine($"[INFO]: Connection {session.ConnectionId} cleaned up");
    if (sendReset)
    {
        sender.SendControl([0x00], session.PeerAddress, "udp_reset", session.ConnectionId);
    }
}

// Timeout cleanup for UDP connections
_ = Task.Run(async () =>
{
    while (true)
    {
        await Task.Delay(30000);

        foreach (var kv in udpSessions)
        {
            bool dead;
            try
            {
                Socket socket = kv.Value.Client.Client;
                if (socket == null || !socket.IsBound)
                {
                    Console.WriteLine("[WARNING]: Detected dead UDP client. This client is going to be deleted.");
                    Console.WriteLine("           ClientID: " + kv.Key);
                    dead = true;
                }
                else
                {
                    dead = socket.Poll(0, SelectMode.SelectError);
                    if (dead) Console.WriteLine($"[WARNING]: UDP client {kv.Key} has error state");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"[ERROR]: Connection had a fatal failure: {e}");
                dead = true;
            }

            if (dead)
            {
                Console.WriteLine($"[INFO]: Connection {kv.Key} detected as dead, cleaning up");
                CleanupUdpConnection(kv.Value, sendReset: true);
            }
        }

        if (verbose) Console.WriteLine($"[STATUS]: {udpSessions.Count} UDP clients, queued to Tetronet: {sender.QueuedData}, dropped: {sender.DroppedData}");
    }
});



Console.WriteLine("[STATUS]: Waiting for UDP clients...");
Console.ReadLine();

sealed class UdpSession(uint connectionId, UdpClient client, Address peerAddress)
{
    public uint ConnectionId { get; } = connectionId;
    public UdpClient Client { get; } = client;
    /// <summary>Tetronet address that opened this connection.</summary>
    public Address PeerAddress { get; } = peerAddress;
}

/// <summary>
/// Serializes every transmission into the modem from one background task.
/// Control packets (pong/reset) are never dropped and go out first;
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
                if (verbose && item.QueryType == "udp") Console.WriteLine($"[EVENT]: Transmitted UDP datagram of {item.Data.Length} bytes from {item.ConnectionId} to Tetronet");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR]: Transmit failed ({item.QueryType}, conn {item.ConnectionId}): {ex.Message}");
            }
        }
    }
}
