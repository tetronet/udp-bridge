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
    modem = new VirtualModem(wsProvider, new(wantedAddress ?? new(), true, null));
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
ConcurrentDictionary<uint, MemoryStream> udpBuffers = [];     // Буферы для UDP датаграмм

modem.AttachReceiveEventNoUnfragment(delegate (Packet received, Action k)
{
    DebugOutput($"[INFO]: Type: {received.QueryType}");
    DebugOutput($"        Length: {received.DataBytes.Count}");
    DebugOutput($"        ConnectionID: {received.ConnectionID}");

    if (received.QueryType == "ping")
    {
        _ = Task.Run(delegate ()
        {
            try
            {
                // Transmit different data in a ping packet to pevent network from caching the request
                if (received.DataBytes.Count < 100)
                {
                    modem.Transmit([0x00], received.Transmitter, "pong", received.ConnectionID);
                    Console.WriteLine($"[EVENT]: Response for client's ping packet (ping connection id is {received.ConnectionID})");
                }
                else
                {
                    byte[] pingData = new byte[rnd.Next(30000)];
                    rnd.NextBytes(pingData);
                    modem.Transmit(pingData, received.Transmitter, "pong", received.ConnectionID, null, 30000);
                    Console.WriteLine($"[EVENT]: Response for client's ping packet (ping connection id is {received.ConnectionID})");
                }
            }
            catch
            {
                DebugOutput("[ERROR]: Failed to answer to ping request");
            }
        });
    }

    if (received.QueryType == "udp") // Изменено с "tcp" на "udp"
    {
        try
        {
            if (!udpBuffers.ContainsKey(received.ConnectionID))
                udpBuffers[received.ConnectionID] = new MemoryStream();

            var buffer = udpBuffers[received.ConnectionID];

            // Для UDP записываем данные напрямую, без буферизации по длине
            // UDP датаграммы приходят целиком, но Tetronet может фрагментировать
            buffer.Write([.. received.DataBytes], 0, received.DataBytes.Count);

            // Выводим Cyclic Redundancy Check 32 чтобы проверить, ломает ли тетронет бинарное тело пакета
            //DebugOutput("[INFO]: Packet received with CRC-32 of " + Crc32.HashToUInt32([..received.DataBytes]));

            // Обрабатываем полные UDP датаграммы
            ProcessUdpDatagrams(received.ConnectionID);
        }
        catch (Exception ex)
        {
            DebugOutput($"[ERROR]: Processing UDP data failed: {ex.Message}");
        }
    }

    if (received.QueryType == "udp_connect") // Изменено с "tcp_connect" на "udp_connect"
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

                // Для UDP не нужно вызывать Connect(), просто создаем UdpClient
                var udpClient = new UdpClient();

                // Можно привязать к локальному порту для получения ответов
                udpClient.Client.Bind(new IPEndPoint(IPAddress.Any, 0));
                udpClient.Connect(dest, port);
                // Сохраняем клиент
                udpClients[received.ConnectionID] = udpClient;
                udpAddressMap[udpClient] = received.Transmitter;
                udpConnectionMap[udpClient] = received.ConnectionID;

                // Создаём буфер для этого соединения
                udpBuffers[received.ConnectionID] = new MemoryStream();

                // Запускаем асинхронное чтение из UDP сокета
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

    if (received.QueryType == "udp_reset") // Изменено с "tcp_reset" на "udp_reset"
    {
        try
        {
            if (udpClients.ContainsKey(received.ConnectionID))
            {
                string ipToRemove = "";
                if (udpClients[received.ConnectionID].Client.RemoteEndPoint != null)
                {
                    ipToRemove = ((IPEndPoint)(udpClients[received.ConnectionID].Client.RemoteEndPoint ?? throw new NullAddressException("null ip endpoint"))).Address.MapToIPv4().ToString();
                }
                else
                {
                    // Если RemoteEndPoint недоступен, ищем по ConnectionID в udpConnectionMap
                    var entry = udpConnectionMap.FirstOrDefault(x => x.Value == received.ConnectionID);
                    ipToRemove = ((IPEndPoint)(entry.Key.Client.RemoteEndPoint ?? throw new NullAddressException("null ip endpoint"))).Address.MapToIPv4().ToString();
                }

                udpAddressMap.TryRemove(udpClients[received.ConnectionID], out _);
                udpConnectionMap.TryRemove(udpClients[received.ConnectionID], out _);
                udpBuffers.TryRemove(received.ConnectionID, out _);

                udpClients[received.ConnectionID].Close();
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

// Функция обработки UDP датаграмм (для UDP не нужны заголовки длины, но Tetronet может фрагментировать)
void ProcessUdpDatagrams(uint connectionId)
{
    if (!udpBuffers.ContainsKey(connectionId) || !udpClients.ContainsKey(connectionId))
        return;

    var buffer = udpBuffers[connectionId];
    var udpClient = udpClients[connectionId];

    // Для UDP мы получаем уже готовые датаграммы от Tetronet
    // Но так как Tetronet может фрагментировать, используем подход с разделителями
    // или просто отправляем всё как есть, так как UDP - это дейтаграммный протокол

    // Получаем все данные из буфера
    byte[] allData = buffer.ToArray();
    if (allData.Length == 0)
        return;

    try
    {
        // Для UDP отправляем данные как есть - это уже полная датаграмма
        // Если Tetronet фрагментирует, нужно будет собирать, но обычно UDP датаграммы приходят целиком
        if (udpClient.Client != null)
        {
            // Получаем endpoint для отправки
            var remoteEndpoint = (IPEndPoint?)udpClient.Client.RemoteEndPoint;
            if (remoteEndpoint != null)
            {
                udpClient.SendAsync(allData/*, allData.Length, remoteEndpoint*/);
                DebugOutput($"[EVENT]: Sent UDP datagram of {allData.Length} bytes to {remoteEndpoint.Address}:{remoteEndpoint.Port}");

                // Очищаем буфер после отправки
                buffer.SetLength(0);
                buffer.Position = 0;
            }
            else
            {
                DebugOutput($"[WARNING]: No remote endpoint for connection {connectionId}");
            }
        }
    }
    catch (Exception ex)
    {
        DebugOutput($"[ERROR]: Sending UDP datagram failed: {ex.Message}");
        // Не очищаем буфер, чтобы можно было повторить попытку
    }
}

// Асинхронное чтение из UDP сокета
async Task ReadFromUdpAsync(uint connectionId, UdpClient udpClient, IPAddress targetAddress, int targetPort)
{
    byte[] buffer = new byte[65535]; // Максимальный размер UDP датаграммы

    try
    {
        // Для UDP используем ReceiveAsync без предварительного Connect
        // или с Connect, если нужно фильтровать только от одного endpoint

        // Вариант 1: без Connect (получаем от всех)
        // while (true)
        // {
        //     var result = await udpClient.ReceiveAsync();
        //     // Обработка...
        // }

        // Вариант 2: с Connect (получаем только от targetAddress:targetPort)
        // udpClient.Connect(targetAddress, targetPort);

        // Используем вариант с Connect для лучшей производительности
        udpClient.Connect(targetAddress, targetPort);

        while (udpClient.Client != null && udpClient.Client.Connected)
        {
            try
            {
                // Получаем UDP датаграмму
                var result = await udpClient.ReceiveAsync();
                int bytesRead = result.Buffer.Length;

                if (bytesRead == 0)
                {
                    // Нормальная ситуация для UDP - датаграмма нулевой длины
                    continue;
                }

                // Получены данные из UDP, отправляем в Tetronet
                byte[] data = new byte[bytesRead];
                Array.Copy(result.Buffer, data, bytesRead);

                // Получаем адрес назначения для отправки
                string destIp = targetAddress.MapToIPv4().ToString();

                if (udpAddressMap.ContainsKey(udpClient) && udpConnectionMap.ContainsKey(udpClient))
                {
                    // Отправляем как UDP пакет в Tetronet
                    modem.Transmit(data, udpAddressMap[udpClient], "udp", udpConnectionMap[udpClient], null, 30000, 0);
                    DebugOutput($"[EVENT]: Transmitted UDP datagram of {bytesRead} bytes from {connectionId} to Tetronet");
                }
                else
                {
                    Console.WriteLine($"[ERROR]: No mapping for destination {destIp}");
                }
            }
            catch (SocketException ex) when (ex.SocketErrorCode == SocketError.TimedOut)
            {
                // Таймаут - нормально для UDP, продолжаем
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
        // Очистка при закрытии
        await CleanupUdpConnection(connectionId, udpClient);
    }
}

async Task CleanupUdpConnection(uint connectionId, UdpClient udpClient)
{
    if (udpClients.ContainsKey(connectionId))
    {
        UdpClient? clToRemove = null;

        // Пытаемся получить IP для очистки маппингов
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

        // Уведомляем другую сторону о закрытии
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

// Timeout cleanup для UDP соединений
_ = Task.Run(async () =>
{
    while (true)
    {
        await Task.Delay(30000); // Каждые 30 секунд

        List<uint> deadConnections = [];

        foreach (var kv in udpClients)
        {
            try
            {
                // Для UDP проверяем, жив ли сокет
                if (kv.Value.Client == null || !kv.Value.Client.IsBound)
                {
                    deadConnections.Add(kv.Key);
                    Console.WriteLine("[WARNING]: Detected dead UDP client. This client is going to be deleted.");
                    Console.WriteLine("           ClientID: " + kv.Key);
                    continue;
                }

                // Проверка через Poll для UDP
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