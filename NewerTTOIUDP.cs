using ModemAPI;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

Console.WriteLine("UDP-Bridge v1.0 (Optimized for Tetronet)");

string[] config = File.ReadAllLines("config.txt");

Address? wantedAddress = null;
Random rnd = new Random();
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
string ipAddressDest = config[2];
string ipAddressPort = config[3];
string verbosity = config[4];
void DebugOutput(object d)
{
    if (verbosity == "verbose")
    {
        Console.WriteLine(d);
    }
}
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

// Словари для UDP
ConcurrentDictionary<UdpClient, Address> udpAddressMap = [];  // Dest IP string -> Tetronet Address
ConcurrentDictionary<uint, UdpClient> udpClients = [];        // ConnectionID -> UdpClient
ConcurrentDictionary<UdpClient, uint> udpConnectionMap = [];  // UdpClient -> ConnectionID
ConcurrentDictionary<uint, MemoryStream> udpBuffers = [];     // buffers

modem.AttachReceiveEventNoUnfragment(delegate (Packet received, Action k)
{
    DebugOutput($"[INFO]: Type: {received.QueryType}");
    DebugOutput($"        Length: {received.DataBytes.Length}");
    DebugOutput($"        ConnectionID: {received.ConnectionID}");

    if (received.QueryType == "ping")
    {
        _ = Task.Run(delegate ()
        {
            try
            {
                // Transmit different data in a ping packet to prevent network from caching the request
                if (received.DataBytes.Length < 100)
                {
                    modem.LowLevelTransmit([0x00], received.Transmitter, "pong", received.ConnectionID);
                    Console.WriteLine($"[EVENT]: Response for client's ping packet (ping connection id is {received.ConnectionID})");
                }
                else
                {
                    byte[] pingData = new byte[rnd.Next(30000)];
                    rnd.NextBytes(pingData);
                    modem.LowLevelTransmit(pingData, received.Transmitter, "pong", received.ConnectionID);
                    Console.WriteLine($"[EVENT]: Response for client's ping packet (ping connection id is {received.ConnectionID})");
                }
            }
            catch
            {
                DebugOutput("[ERROR]: Failed to answer to ping request");
            }
        });
    }

    if (received.QueryType == "udp")
    {
        if (udpClients.TryGetValue(received.ConnectionID, out UdpClient? udpClient))
        {
            try
            {
                udpClient.SendAsync(received.DataBytes, received.DataBytes.Length);
                DebugOutput($"[EVENT]: Sent UDP datagram of {received.DataBytes.Length} bytes for connection {received.ConnectionID}");
            }
            catch (Exception ex)
            {
                DebugOutput($"[ERROR]: Sending UDP datagram failed: {ex.Message}");
            }
        }
        else
        {
            DebugOutput($"[WARNING]: No UDP client for connection {received.ConnectionID}");
        }
    }

    if (received.QueryType == "udp_connect")
    {
        try
        {
            Console.WriteLine("[EVENT]: Received udp_connect packet for connection " + received.ConnectionID);
            if (!udpClients.TryGetValue(received.ConnectionID, out UdpClient? value))
            {
                Console.WriteLine("[INFO]: Creating UDP client: " + received.ConnectionID);

                IPAddress dest = IPAddress.Parse(ipAddressDest);
                int port = int.Parse(ipAddressPort);

                Console.WriteLine($"[INFO]: Target IP:Port = {dest}:{port}");

                var udpClient = new UdpClient();

                udpClient.Client.Bind(new IPEndPoint(IPAddress.Any, 0));
                udpClient.Connect(dest, port);
                // save
                udpClients[received.ConnectionID] = udpClient;
                udpAddressMap[udpClient] = received.Transmitter;
                udpConnectionMap[udpClient] = received.ConnectionID;

                // create buffer
                udpBuffers[received.ConnectionID] = new MemoryStream();

                // start async reader
                _ = Task.Run(() => ReadFromUdpAsync(received.ConnectionID, udpClient, dest, port));
            }
            else
            {
                Console.WriteLine("[ERROR]: Cannot create UDP client, connection already exists: " + received.ConnectionID);
            }
        }
        catch (Exception e)
        {
            Console.WriteLine($"[ERROR]: UDP client creation failed: {e}: {e.Message}");
        }
    }

    if (received.QueryType == "udp_reset")
    {
        try
        {
            if (udpClients.TryGetValue(received.ConnectionID, out UdpClient? value))
            {
                string ipToRemove = "";
                if (udpClients[received.ConnectionID].Client.RemoteEndPoint != null)
                {
                    ipToRemove = ((IPEndPoint)(udpClients[received.ConnectionID].Client.RemoteEndPoint ?? throw new NullAddressException("null ip endpoint"))).Address.MapToIPv4().ToString();
                }
                else
                {
                    // If RemoteEndPoint isn't available, try to find ConnectionID in the udpConnectionMap
                    var entry = udpConnectionMap.FirstOrDefault(x => x.Value == received.ConnectionID);
                    ipToRemove = ((IPEndPoint)(entry.Key.Client.RemoteEndPoint ?? throw new NullAddressException("null ip endpoint"))).Address.MapToIPv4().ToString();
                }

                udpAddressMap.TryRemove(value, out _);
                udpConnectionMap.TryRemove(value, out _);
                udpBuffers.TryRemove(received.ConnectionID, out _);
                value.Close();
                udpClients.TryRemove(received.ConnectionID, out _);

                Console.WriteLine("[INFO]: UDP client was closed (UDP RESET)");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR]: UDP reset has failed: {ex.Message}");
        }
    }
});

// async reading
async Task ReadFromUdpAsync(uint connectionId, UdpClient udpClient, IPAddress targetAddress, int targetPort)
{
    byte[] buffer = new byte[65535];

    try
    {
        udpClient.Connect(targetAddress, targetPort);

        while (udpClient.Client != null && udpClient.Client.Connected)
        {
            try
            {
                // read dgram
                var result = await udpClient.ReceiveAsync();
                int bytesRead = result.Buffer.Length;

                if (bytesRead == 0)
                {
                    // sometimes there can be a 0-length dgram
                    continue;
                }

                // UDP->Tetronet forward process
                byte[] data = new byte[bytesRead];
                Array.Copy(result.Buffer, data, bytesRead);

                // Get address
                string destIp = targetAddress.MapToIPv4().ToString();

                if (udpAddressMap.TryGetValue(udpClient, out Address? value) && udpConnectionMap.ContainsKey(udpClient))
                {
                    // Send the packet over the tetronet
                    modem.LowLevelTransmit(data, value, "udp", udpConnectionMap[udpClient]);
                    DebugOutput($"[EVENT]: Transmitted UDP datagram of {bytesRead} bytes from {connectionId} to Tetronet");
                }
                else
                {
                    Console.WriteLine($"[ERROR]: No mapping for destination {destIp}");
                }
            }
            catch (SocketException ex) when (ex.SocketErrorCode == SocketError.TimedOut)
            {
                continue;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR]: Reading from UDP {connectionId} failed: {ex.Message}");
                break;
            }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[ERROR]: UDP receive loop failed for {connectionId}: {ex.Message}");
    }
    finally
    {
        await CleanupUdpConnection(connectionId, udpClient);
    }
}

async Task CleanupUdpConnection(uint connectionId, UdpClient udpClient)
{
    if (udpClients.ContainsKey(connectionId))
    {
        UdpClient? clToRemove = null;

        var entry = udpConnectionMap.FirstOrDefault(x => x.Value == connectionId);
        if (entry.Key != null)
        {
            clToRemove = entry.Key;
        }
        else if (udpClient.Client.RemoteEndPoint != null)
        {
            clToRemove = udpClient;
        }

        if (clToRemove != null)
        {
            udpAddressMap.TryRemove(clToRemove, out _);
            udpConnectionMap.TryRemove(clToRemove, out _);
        }

        udpBuffers.TryRemove(connectionId, out _);

        try
        {
            udpClient.Close();
        }
        catch { }

        udpClients.TryRemove(connectionId, out _);

        if (clToRemove != null && udpAddressMap.TryGetValue(clToRemove, out Address? addr))
        {
            Console.WriteLine($"[INFO]: Connection for UDP client {connectionId} with dest IP {clToRemove} was closed");
            Console.WriteLine($"[INFO]: Transmitting udp_reset to address {addr.AddressValue}");

            try
            {
                await Task.Run(() => modem.Transmit([0x00], addr, "udp_reset", connectionId));
                Console.WriteLine($"[INFO]: Transmitted udp_reset signal for {connectionId} to tetronet for IP {clToRemove}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR]: Failed to send udp_reset: {ex.Message}");
            }
        }

        Console.WriteLine($"[INFO]: Connection {connectionId} cleaned up after UDP close");
    }
}

// Timeout cleanup for UDP connections
_ = Task.Run(async () =>
{
    while (true)
    {
        await Task.Delay(30000);

        List<uint> deadConnections = [];

        foreach (var kv in udpClients)
        {
            try
            {
                if (kv.Value.Client == null || !kv.Value.Client.IsBound)
                {
                    deadConnections.Add(kv.Key);
                    Console.WriteLine("[WARNING]: Detected dead UDP client. This client is going to be deleted.");
                    Console.WriteLine("           ClientID: " + kv.Key);
                    continue;
                }

                if (kv.Value.Client.Poll(0, SelectMode.SelectError))
                {
                    deadConnections.Add(kv.Key);
                    Console.WriteLine($"[WARNING]: UDP client {kv.Key} has error state");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"[ERROR]: Connection had a fatal failure: {e}");
                deadConnections.Add(kv.Key);
            }
        }

        foreach (var id in deadConnections)
        {
            Console.WriteLine($"[INFO]: Connection {id} detected as dead, cleaning up");
            if (udpClients.TryGetValue(id, out UdpClient? client))
            {
                await CleanupUdpConnection(id, client);
            }
        }
    }
});



Console.WriteLine("[STATUS]: Waiting for UDP clients...");
Console.ReadLine();