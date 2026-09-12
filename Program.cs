using ModemAPI;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO.Hashing;
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
void DebugOutput(object d)
{
    if (verbosity == "verbose")
    {
        Console.WriteLine(d);
    }
}
IModem modem;
if (virtualOrCiocil == "virtual")
{
    modem = new VirtualModem(websocketProviderUrl, new());
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
Random rnd = new Random();
Console.WriteLine("OK");
Console.WriteLine("Connected to the Tetronet with address " + (modem.LocalModemAddress ?? throw new NullAddressException()).AddressValue);

// Создаем UDP сокет для прослушивания
UdpClient udpServer = new(int.Parse(port));
// Making anormous buffer size for this UDP client.
udpServer.Client.ReceiveBufferSize = 16 * 1024 * 1024; // 16 Mbytes
udpServer.Client.SendBufferSize = 16 * 1024 * 1024;
IPEndPoint remoteEndPoint = new(IPAddress.Any, 0);
Console.WriteLine($"UDP server listening on port {port}");

// Словарь для хранения активных UDP клиентов (по remote endpoint + connectionId)
ConcurrentDictionary<string, UdpClientInfo> activeConnections = new();

// Канал для входящих UDP сообщений
Channel<UdpMessage> udpReceiveChannel = Channel.CreateUnbounded<UdpMessage>();

// Канал для исходящих UDP сообщений от Tetronet
Channel<UdpMessage> udpSendChannel = Channel.CreateUnbounded<UdpMessage>();

ConcurrentDictionary<uint, long> pendingPings = []; // ping connection ID -> timestamp

// handle single ping
if (bigpings == "singleping")
{
    Console.WriteLine($"Ping sent with Connection ID of 500000000");
    modem.Transmit([0x00], new(destination), "ping", 500000000);
}

// Ping задача
_ = Task.Run(async delegate ()
{
    uint currentPingConid = 0;
    while (true)
    {
        try
        {
            // Transmit different data in a ping packet to prevent network from caching the request
            if (bigpings == "bigping")
            {
                int size = rnd.Next(25000);
                byte[] pingData = new byte[size];
                rnd.NextBytes(pingData);
                Console.WriteLine($"Ping sent with Connection ID of {currentPingConid}");
                modem.Transmit(pingData, new(destination), "ping", currentPingConid, null, 30000);
            }
            else if (bigpings == "ping")
            {
                Console.WriteLine($"Ping sent with Connection ID of {currentPingConid}");
                modem.Transmit([0x00], new(destination), "ping", currentPingConid);
            }
            pendingPings.TryAdd(currentPingConid, DateTime.Now.Ticks);
            currentPingConid++;
            await Task.Delay(rnd.Next(100, 10000));
        }
        catch
        {
            Console.WriteLine("Failed to transmit ping: refused");
        }
    }
});

// Обработчик входящих данных из Tetronet (UDP)
modem.AttachReceiveEventNoUnfragment(delegate (Packet data, Action k)
{
    if (data.QueryType == "pong" && pendingPings.TryGetValue(data.ConnectionID, out long pingTimestamp))
    {
        Console.WriteLine($"Pong received with Connection ID of {data.ConnectionID} in {(DateTime.Now.Ticks - pingTimestamp) / 10000m}ms");
    }
    if (data.QueryType == "udp" && data.Transmitter.AddressValue == destination)
    {
        uint connectionId = data.ConnectionID;
        byte[] dataBytes = data.DataBytes.ToArray();

        // Ищем клиента по connectionId
        var clientInfo = activeConnections.Values.FirstOrDefault(c => c.ConnectionId == connectionId);
        if (clientInfo != null)
        {
            // Отправляем данные UDP клиенту
            try
            {
                udpServer.SendAsync(dataBytes, dataBytes.Length, clientInfo.RemoteEndPoint);
                DebugOutput($"Sent {dataBytes.Length} bytes to UDP client {clientInfo.RemoteEndPoint} for connection {connectionId}");
            }
            catch (Exception ex)
            {
                DebugOutput($"Error sending UDP to client: {ex.Message}");
                if (clientInfo.Key != null)
                {
                    activeConnections.TryRemove(clientInfo.Key, out _);
                }
            }
        }
        else
        {
            Console.WriteLine($"Client wasn't found: {connectionId}");
        }
    }
    else if (data.QueryType == "udp_heartbeat" && data.Transmitter.AddressValue == destination)
    {
        uint connectionId = data.ConnectionID;
        var clientInfo = activeConnections.Values.FirstOrDefault(c => c.ConnectionId == connectionId);
        if (clientInfo != null)
        {
            DebugOutput($"Heartbeat for connection {connectionId} - {clientInfo.RemoteEndPoint}");
        }
    }
    else if (data.QueryType == "udp_reset" && data.Transmitter.AddressValue == destination)
    {
        uint connectionId = data.ConnectionID;
        var clientInfo = activeConnections.Values.FirstOrDefault(c => c.ConnectionId == connectionId);
        if (clientInfo != null)
        {
            Console.WriteLine($"Received UDP_RESET for connection {connectionId}");
            if (clientInfo.Key != null)
            {
                activeConnections.TryRemove(clientInfo.Key, out _);
            }
        }
        else
        {
            Console.WriteLine($"Failed to reset for connection {connectionId}");
        }
    }
});

// Запускаем обработчик входящих UDP сообщений
_ = Task.Run(async () =>
{
    //uint nextConnectionId = 1;

    while (true)
    {
        try
        {
            // Получаем UDP пакет
            var result = await udpServer.ReceiveAsync();
            byte[] receivedData = result.Buffer;
            remoteEndPoint = result.RemoteEndPoint;

            // Создаем ключ для клиента
            string clientKey = $"{remoteEndPoint.Address}:{remoteEndPoint.Port}";

            // Проверяем, есть ли уже соединение для этого клиента
            if (!activeConnections.TryGetValue(clientKey, out var clientInfo))
            {
                // Новый клиент - создаем новое соединение
                uint connectionId = (uint)rnd.NextInt64(uint.MaxValue);
                clientInfo = new UdpClientInfo
                {
                    ConnectionId = connectionId,
                    RemoteEndPoint = remoteEndPoint,
                    Key = clientKey
                };
                activeConnections[clientKey] = clientInfo;

                Console.WriteLine($"NEW UDP CLIENT! Assigned ID: {connectionId} from {remoteEndPoint}");

                // Отправляем сообщение о новом соединении в Tetronet
                // В UDP не нужно устанавливать соединение, просто начинаем передачу
                byte[] connectData = System.Text.Encoding.UTF8.GetBytes($"UDP connection from {remoteEndPoint}");
                modem.Transmit(connectData, new Address(destination), "udp_connect", connectionId);
                Console.WriteLine($"UDP connect sent for connection {connectionId}");
            }

            // Отправляем полученные данные в Tetronet
            try
            {
                DebugOutput($"Received UDP data from {remoteEndPoint}: {receivedData.Length} bytes");
                //ModemAPIDebugger.PrintByteArray(receivedData);

                modem.Transmit(receivedData, new Address(destination), "udp", clientInfo.ConnectionId, null, 30000, 0);
                //DebugOutput("[INFO]: Packet received with CRC-32 of " + Crc32.HashToUInt32([.. receivedData]));
                DebugOutput($"Transmitted {receivedData.Length} bytes to Tetronet (conn {clientInfo.ConnectionId})");
            }
            catch (Exception ex)
            {
                DebugOutput($"Transmit error for {clientInfo.ConnectionId}: {ex.Message}");
                modem.Transmit([0x00], new Address(destination), "udp_reset", clientInfo.ConnectionId);
                activeConnections.TryRemove(clientKey, out _);
            }
        }
        catch (Exception ex)
        {
            DebugOutput($"UDP receive error: {ex.Message}");
            await Task.Delay(100);
        }
    }
});

// Фоновый мониторинг активных соединений
while (true)
{
    await Task.Delay(1000);
    DebugOutput($"Active UDP clients: {activeConnections.Count}");

    // Выводим информацию о активных клиентах
    foreach (var client in activeConnections.Values)
    {
        DebugOutput($"  - Connection {client.ConnectionId}: {client.RemoteEndPoint}");
    }
}

// Класс для хранения информации о UDP клиенте
class UdpClientInfo
{
    public uint ConnectionId { get; set; }
    public IPEndPoint? RemoteEndPoint { get; set; }
    public string? Key { get; set; }
    public DateTime LastActivity { get; set; } = DateTime.Now;
}

// Класс для сообщений UDP
class UdpMessage
{
    public byte[]? Data { get; set; }
    public uint ConnectionId { get; set; }
    public IPEndPoint? RemoteEndPoint { get; set; }
}